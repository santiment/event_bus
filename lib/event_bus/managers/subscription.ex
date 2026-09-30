defmodule EventBus.Manager.Subscription do
  @moduledoc false

  # Serializes subscription writes; the notify hot path reads opts from ETS.
  # Each subscribe bumps a global generation; limited subscriptions carry it
  # in their topic-map opts so admission and completions from a stale
  # subscription are ignored. Limited admission requires remaining > in_flight.
  use GenServer

  alias EventBus.Service.Subscription, as: SubscriptionService

  @typep subscriber :: EventBus.subscriber()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_topic_patterns ::
           EventBus.subscriber_with_topic_patterns()
  @typep topic :: EventBus.topic()

  @backend SubscriptionService
  @opts_table :eb_subscription_opts
  # Survives manager restarts.
  @limits_table :eb_subscription_limits
  @default_opts %{priority: 0, guard: nil, limit_generation: nil}

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def init(_opts) do
    # Mirrors map_size(limits) so hot paths can skip the GenServer lock-free.
    ref = :counters.new(1, [:atomics])
    :persistent_term.put({__MODULE__, :limited_gate}, ref)

    limits =
      for {{_module, _config} = subscriber, limit} <-
            :ets.tab2list(@limits_table),
          into: %{},
          do: {subscriber, limit}

    generation =
      case :ets.lookup(@limits_table, :generation) do
        [{:generation, generation}] -> generation
        [] -> 0
      end

    {:ok, sync_limited_gate(%{limits: limits, generation: generation})}
  end

  @doc """
  Does the subscriber subscribe to topic_patterns?
  """
  @spec subscribed?(subscriber_with_topic_patterns()) :: boolean()
  defdelegate subscribed?(subscriber),
    to: @backend,
    as: :subscribed?

  @doc """
  Subscribe the subscriber to topic_patterns
  """
  @spec subscribe(subscriber_with_topic_patterns()) :: :ok
  def subscribe({subscriber, topic_patterns}) do
    validate_patterns!(topic_patterns)

    GenServer.call(
      __MODULE__,
      {:subscribe, {normalize(subscriber), topic_patterns}}
    )
  end

  @doc """
  Subscribe the subscriber to topic_patterns with options (guard, priority)
  """
  @spec subscribe(subscriber_with_topic_patterns(), keyword()) :: :ok
  def subscribe({subscriber, topic_patterns}, opts) do
    validate_patterns!(topic_patterns)
    normalized_opts = validate_opts!(opts)

    GenServer.call(
      __MODULE__,
      {:subscribe_with_opts, {normalize(subscriber), topic_patterns},
       normalized_opts}
    )
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after one terminal event
  """
  @spec subscribe_once(subscriber_with_topic_patterns()) :: :ok
  def subscribe_once(subscriber_with_topic_patterns) do
    subscribe_n(subscriber_with_topic_patterns, 1)
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after N terminal events
  """
  @spec subscribe_n(subscriber_with_topic_patterns(), pos_integer()) :: :ok
  def subscribe_n({subscriber, topic_patterns}, count) do
    # Validated in the caller: a manager crash would drop all limit state.
    validate_patterns!(topic_patterns)

    if not (is_integer(count) and count > 0) do
      raise ArgumentError, "count must be a positive integer"
    end

    GenServer.call(
      __MODULE__,
      {:subscribe_n, {normalize(subscriber), topic_patterns}, count}
    )
  end

  @doc """
  Unsubscribe the subscriber
  """
  @spec unsubscribe(subscriber()) :: :ok
  def unsubscribe(subscriber) do
    GenServer.call(__MODULE__, {:unsubscribe, normalize(subscriber)})
  end

  @doc """
  Set subscribers to the topic
  """
  @spec register_topic(topic()) :: :ok
  def register_topic(topic) do
    GenServer.call(__MODULE__, {:register_topic, topic})
  end

  @doc """
  Unset subscribers from the topic
  """
  @spec unregister_topic(topic()) :: :ok
  def unregister_topic(topic) do
    GenServer.call(__MODULE__, {:unregister_topic, topic})
  end

  @doc """
  Read subscriber opts (priority, guard) directly from ETS.
  No GenServer.call — this is on the notification hot path.
  """
  @spec fetch_opts(subscriber()) :: %{
          guard: function() | nil,
          priority: integer()
        }
  def fetch_opts(subscriber) do
    case :ets.lookup(@opts_table, subscriber) do
      [{^subscriber, opts}] -> Map.take(opts, [:priority, :guard])
      _ -> %{priority: 0, guard: nil}
    end
  end

  @doc """
  Run admission for the limited subscribers of a single dispatch cycle.

  Takes `{subscriber, limit_generation}` candidates as read from the topic
  map. A candidate is admitted only if its generation still matches the
  subscriber's live limit and `remaining > in_flight`; its in_flight counter
  is then incremented. Candidates whose subscription was exhausted,
  unsubscribed or replaced since the topic map was read are dropped.

  Returns `{admitted_subscribers, generation_snapshot}` where the snapshot
  maps each admitted subscriber to its generation. The snapshot is stored
  alongside the event so that terminal callbacks (which may arrive much
  later) decrement the correct subscription generation.
  """
  @spec prepare_subscribers_for_dispatch([{subscriber(), pos_integer()}]) ::
          {subscribers(), %{optional(subscriber()) => pos_integer()}}
  def prepare_subscribers_for_dispatch(candidates) do
    GenServer.call(__MODULE__, {:prepare_subscribers_for_dispatch, candidates})
  end

  @doc """
  Decrement the remaining counter for a limited subscriber after a terminal
  event (completed or skipped). The generation argument must match the
  subscriber's current generation — stale decrements from a prior
  subscription are ignored. Also decrements in_flight. When both reach 0,
  the subscriber is auto-unsubscribed.
  """
  @spec decrement_limit(subscriber(), non_neg_integer()) :: :ok
  def decrement_limit(subscriber, generation) do
    GenServer.call(__MODULE__, {:decrement_limit, subscriber, generation})
  end

  @doc """
  Batch version of `decrement_limit/2`. Processes all `{subscriber, generation}`
  pairs in a single GenServer call. Unlimited subscribers (no entry in the
  limits map) are skipped with a cheap `Map.get` — no per-subscriber overhead.
  """
  @spec decrement_limits([{subscriber(), non_neg_integer()}]) :: :ok
  def decrement_limits([]), do: :ok

  def decrement_limits(subscriber_generations) do
    GenServer.call(__MODULE__, {:decrement_limits, subscriber_generations})
  end

  @doc """
  Release a limited subscriber's in-flight reservation WITHOUT spending its
  delivery budget. Used when an admitted event is skipped before it reaches
  the subscriber's `process/1` (guard rejection or upstream cancellation), so
  `subscribe_once`/`subscribe_n` are not consumed by an undelivered event.
  The generation argument must match the subscriber's current generation;
  stale releases from a prior subscription are ignored.
  """
  @spec release_in_flight(subscriber(), non_neg_integer()) :: :ok
  def release_in_flight(subscriber, generation) do
    GenServer.call(__MODULE__, {:release_in_flight, subscriber, generation})
  end

  @doc """
  Return `true` if any subscriber currently has an active limit
  (`subscribe_once`/`subscribe_n`). Lock-free atomic read — safe to call on
  the notification and completion hot paths to skip GenServer round-trips.
  """
  @spec any_limited?() :: boolean()
  def any_limited? do
    case :persistent_term.get({__MODULE__, :limited_gate}, nil) do
      nil -> false
      ref -> :counters.get(ref, 1) > 0
    end
  end

  @doc """
  Return the set of subscribers that currently have active limits
  (`subscribe_once`/`subscribe_n`). Used by the sweeper to skip per-subscriber
  work for unlimited subscribers.
  """
  @spec limited_subscribers() :: MapSet.t(subscriber())
  def limited_subscribers do
    GenServer.call(__MODULE__, :limited_subscribers)
  end

  @doc """
  Fetch subscribers
  """
  @spec subscribers() :: subscribers()
  defdelegate subscribers,
    to: @backend,
    as: :subscribers

  @doc """
  Fetch subscribers of the topic
  """
  @spec subscribers(topic()) :: subscribers()
  defdelegate subscribers(topic),
    to: @backend,
    as: :subscribers

  @doc """
  Fetch subscribers of the topic with their opts, pre-sorted by priority
  (highest first). Single lock-free ETS read — the notification hot path.
  """
  @spec subscribers_with_opts(topic()) :: [{subscriber(), map()}]
  defdelegate subscribers_with_opts(topic),
    to: @backend,
    as: :subscribers_with_opts

  @doc false
  def handle_call({:subscribe, {subscriber, topic_patterns}}, _from, state) do
    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, @default_opts)
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, sync_limited_gate(state)}
  end

  @doc false
  def handle_call(
        {:subscribe_n, {subscriber, topic_patterns}, count},
        _from,
        state
      ) do
    state =
      state
      |> reset_subscription_state(subscriber)
      |> put_limit(subscriber, count)
      # Gate first, so completions of the first delivery see any_limited?/0.
      |> sync_limited_gate()

    write_opts_to_ets(subscriber, %{
      @default_opts
      | limit_generation: state.generation
    })

    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, state}
  end

  @doc false
  def handle_call(
        {:subscribe_with_opts, {subscriber, topic_patterns}, validated_opts},
        _from,
        state
      ) do
    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, validated_opts)
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, sync_limited_gate(state)}
  end

  @doc false
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    @backend.unsubscribe(subscriber)
    :ets.delete(@opts_table, subscriber)

    {:reply, :ok,
     sync_limited_gate(clear_subscription_state(state, subscriber))}
  end

  @doc false
  def handle_call(
        {:prepare_subscribers_for_dispatch, candidates},
        _from,
        state
      ) do
    {admitted, snapshot, state} = do_prepare_subscribers(candidates, state)
    {:reply, {admitted, snapshot}, state}
  end

  @doc false
  def handle_call({:register_topic, topic}, _from, state) do
    @backend.register_topic(topic)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:unregister_topic, topic}, _from, state) do
    @backend.unregister_topic(topic)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:decrement_limit, subscriber, generation}, _from, state) do
    state = maybe_decrement_limit(state, subscriber, generation)
    {:reply, :ok, sync_limited_gate(state)}
  end

  @doc false
  def handle_call({:decrement_limits, subscriber_generations}, _from, state) do
    state =
      Enum.reduce(subscriber_generations, state, fn {subscriber, generation},
                                                    acc ->
        maybe_decrement_limit(acc, subscriber, generation)
      end)

    {:reply, :ok, sync_limited_gate(state)}
  end

  @doc false
  def handle_call({:release_in_flight, subscriber, generation}, _from, state) do
    state = maybe_release_in_flight(state, subscriber, generation)
    {:reply, :ok, sync_limited_gate(state)}
  end

  @doc false
  def handle_call(:limited_subscribers, _from, state) do
    limited = state.limits |> Map.keys() |> MapSet.new()
    {:reply, limited, state}
  end

  defp validate_patterns!(patterns) do
    if not (is_list(patterns) and
              Enum.all?(patterns, &(is_binary(&1) or is_atom(&1)))) do
      raise ArgumentError,
            "topic patterns must be a list of strings or atoms, got: #{inspect(patterns)}"
    end
  end

  defp validate_opts!(opts) when is_list(opts) do
    priority = Keyword.get(opts, :priority, 0)
    guard = Keyword.get(opts, :guard)

    if not is_integer(priority) do
      raise ArgumentError, ":priority must be an integer"
    end

    if !(is_nil(guard) or is_function(guard, 1)) do
      raise ArgumentError, ":guard must be a 1-arity function"
    end

    %{@default_opts | priority: priority, guard: guard}
  end

  defp write_opts_to_ets(subscriber, opts) do
    :ets.insert(@opts_table, {subscriber, opts})
  end

  defp sync_limited_gate(state) do
    ref = :persistent_term.get({__MODULE__, :limited_gate})
    :counters.put(ref, 1, map_size(state.limits))
    state
  end

  defp reset_subscription_state(state, subscriber) do
    generation = state.generation + 1
    :ets.insert(@limits_table, {:generation, generation})
    clear_subscription_state(%{state | generation: generation}, subscriber)
  end

  defp clear_subscription_state(state, subscriber) do
    :ets.delete(@limits_table, subscriber)
    %{state | limits: Map.delete(state.limits, subscriber)}
  end

  defp put_limit(state, subscriber, count) do
    limit = %{generation: state.generation, remaining: count, in_flight: 0}
    store_limit(state, subscriber, limit)
  end

  defp store_limit(state, subscriber, limit) do
    :ets.insert(@limits_table, {subscriber, limit})
    %{state | limits: Map.put(state.limits, subscriber, limit)}
  end

  defp maybe_decrement_limit(state, subscriber, generation) do
    case Map.get(state.limits, subscriber) do
      %{generation: ^generation, remaining: remaining, in_flight: in_flight} =
          limit
      when in_flight > 0 ->
        updated_limit = %{
          limit
          | remaining: remaining - 1,
            in_flight: in_flight - 1
        }

        maybe_finalize_limit(state, subscriber, updated_limit)

      _ ->
        state
    end
  end

  defp maybe_release_in_flight(state, subscriber, generation) do
    case Map.get(state.limits, subscriber) do
      %{generation: ^generation, in_flight: in_flight} = limit
      when in_flight > 0 ->
        updated_limit = %{limit | in_flight: in_flight - 1}
        maybe_finalize_limit(state, subscriber, updated_limit)

      _ ->
        state
    end
  end

  defp do_prepare_subscribers(candidates, state) do
    {admitted, snapshot, state} =
      Enum.reduce(candidates, {[], %{}, state}, fn {subscriber, generation},
                                                   {admitted, snapshot, state} =
                                                     acc ->
        case Map.get(state.limits, subscriber) do
          %{generation: ^generation, remaining: remaining, in_flight: in_flight} =
              limit
          when remaining > in_flight ->
            state =
              store_limit(state, subscriber, %{limit | in_flight: in_flight + 1})

            {[subscriber | admitted], Map.put(snapshot, subscriber, generation),
             state}

          _ ->
            acc
        end
      end)

    {Enum.reverse(admitted), snapshot, state}
  end

  defp maybe_finalize_limit(state, subscriber, %{remaining: 0, in_flight: 0}) do
    @backend.unsubscribe(subscriber)
    :ets.delete(@opts_table, subscriber)
    clear_subscription_state(state, subscriber)
  end

  defp maybe_finalize_limit(state, subscriber, updated_limit) do
    store_limit(state, subscriber, updated_limit)
  end

  defp normalize(subscriber) when is_atom(subscriber), do: {subscriber, nil}
  defp normalize({_module, _config} = subscriber), do: subscriber
end
