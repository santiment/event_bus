defmodule EventBus.Manager.Subscription do
  @moduledoc false

  ###########################################################################
  # Subscription manager
  #
  # The GenServer serializes writes to subscription control-plane data
  # (opts, limits, generations). Reads of opts go directly through ETS
  # for zero-overhead access on the notification hot path.
  #
  # ## Generations
  #
  # Each subscriber has a monotonically increasing generation counter,
  # bumped on every subscribe/resubscribe. When an event is dispatched,
  # the current generation is captured in a snapshot alongside the event.
  # When the subscriber eventually reaches a terminal state (completed or
  # skipped), the observation layer passes the saved generation to
  # decrement_limit/2. If it no longer matches the subscriber's current
  # generation — because the subscriber re-subscribed in the meantime —
  # the decrement is ignored. This prevents a stale completion from
  # spending a fresh subscription's budget.
  #
  # ## Limits and in-flight tracking
  #
  # subscribe_once/subscribe_n set a `remaining` counter. Because events
  # are dispatched concurrently (via Task.Supervisor), multiple events
  # can be in flight before any terminal callback arrives to decrement
  # the counter. Without tracking in-flight events, a subscribe_once
  # subscriber could receive two events before the first one completes.
  #
  # To prevent overdelivery, prepare_subscribers_for_dispatch/1 checks
  # `remaining > in_flight` (not just `remaining > 0`) and increments
  # in_flight for each admitted event. decrement_limit/2 then decrements
  # both remaining and in_flight. The subscriber is only unsubscribed
  # when remaining=0 AND in_flight=0, meaning all deliveries are done.
  ###########################################################################

  use GenServer

  alias EventBus.Service.Subscription, as: SubscriptionService

  @typep subscriber :: EventBus.subscriber()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_topic_patterns ::
           EventBus.subscriber_with_topic_patterns()
  @typep topic :: EventBus.topic()

  @backend SubscriptionService
  @opts_table :eb_subscription_opts

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def init(_opts) do
    # generations: %{subscriber => integer} — see "Generations" above
    # limits: %{subscriber => %{generation, remaining, in_flight}} — see "Limits" above
    {:ok, %{limits: %{}, generations: %{}}}
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
    GenServer.call(__MODULE__, {:subscribe, {normalize(subscriber), topic_patterns}})
  end

  @doc """
  Subscribe the subscriber to topic_patterns with options (guard, priority)
  """
  @spec subscribe(subscriber_with_topic_patterns(), keyword()) :: :ok
  def subscribe({subscriber, topic_patterns}, opts) do
    GenServer.call(__MODULE__, {:subscribe_with_opts, {normalize(subscriber), topic_patterns}, opts})
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after one terminal event
  """
  @spec subscribe_once(subscriber_with_topic_patterns()) :: :ok
  def subscribe_once({subscriber, topic_patterns}) do
    GenServer.call(__MODULE__, {:subscribe_n, {normalize(subscriber), topic_patterns}, 1})
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after N terminal events
  """
  @spec subscribe_n(subscriber_with_topic_patterns(), pos_integer()) :: :ok
  def subscribe_n({subscriber, topic_patterns}, count) do
    GenServer.call(__MODULE__, {:subscribe_n, {normalize(subscriber), topic_patterns}, count})
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
  @spec fetch_opts(subscriber()) :: %{guard: function() | nil, priority: integer()}
  def fetch_opts(subscriber) do
    case :ets.lookup(@opts_table, subscriber) do
      [{^subscriber, opts}] -> Map.take(opts, [:priority, :guard])
      _ -> %{priority: 0, guard: nil}
    end
  end

  @doc """
  Prepare the subscriber list for a single dispatch cycle.

  For each subscriber:
  - Unlimited subscribers are always included.
  - Limited subscribers (subscribe_once/subscribe_n) are included only if
    `remaining > in_flight`, and their in_flight counter is incremented.

  Returns `{admitted_subscribers, generation_snapshot}` where the snapshot
  maps each admitted subscriber to the generation that was current at
  dispatch time. The snapshot is stored alongside the event so that
  terminal callbacks (which may arrive much later) decrement the correct
  subscription generation.
  """
  @spec prepare_subscribers_for_dispatch(subscribers()) ::
          {subscribers(), %{optional(subscriber()) => non_neg_integer()}}
  def prepare_subscribers_for_dispatch(subscribers) do
    GenServer.call(__MODULE__, {:prepare_subscribers_for_dispatch, subscribers})
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

  ###########################################################################
  # DELEGATIONS
  ###########################################################################

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

  ###########################################################################
  # PRIVATE API
  ###########################################################################

  @doc false
  def handle_call({:subscribe, {subscriber, topic_patterns}}, _from, state) do
    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, %{priority: 0, guard: nil}, state)
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:subscribe_n, {subscriber, topic_patterns}, count}, _from, state) do
    state =
      state
      |> reset_subscription_state(subscriber)
      |> put_limit(subscriber, count)

    write_opts_to_ets(subscriber, %{priority: 0, guard: nil}, state)
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:subscribe_with_opts, {subscriber, topic_patterns}, opts}, _from, state) do
    normalized_opts = normalize_opts!(opts)
    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, normalized_opts, state)
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    @backend.unsubscribe(subscriber)
    :ets.delete(@opts_table, subscriber)
    {:reply, :ok, clear_subscription_state(state, subscriber)}
  end

  @doc false
  def handle_call({:prepare_subscribers_for_dispatch, subscribers}, _from, state) do
    {admitted, snapshot, state} = do_prepare_subscribers(subscribers, state)
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
    {:reply, :ok, maybe_decrement_limit(state, subscriber, generation)}
  end

  @doc false
  def handle_call({:decrement_limits, subscriber_generations}, _from, state) do
    state =
      Enum.reduce(subscriber_generations, state, fn {subscriber, generation}, acc ->
        maybe_decrement_limit(acc, subscriber, generation)
      end)

    {:reply, :ok, state}
  end

  defp normalize_opts!(opts) when is_list(opts) do
    priority = Keyword.get(opts, :priority, 0)
    guard = Keyword.get(opts, :guard)

    if !is_integer(priority) do
      raise ArgumentError, ":priority must be an integer"
    end

    if !(is_nil(guard) or is_function(guard, 1)) do
      raise ArgumentError, ":guard must be a 1-arity function"
    end

    %{priority: priority, guard: guard}
  end

  # Write opts + generation to ETS for lock-free reads on the hot path.
  defp write_opts_to_ets(subscriber, opts, state) do
    generation = Map.get(state.generations, subscriber, 0)
    :ets.insert(@opts_table, {subscriber, Map.put(opts, :generation, generation)})
  end

  defp reset_subscription_state(state, subscriber) do
    state
    |> bump_generation(subscriber)
    |> clear_subscription_state(subscriber)
  end

  defp clear_subscription_state(state, subscriber) do
    %{state | limits: Map.delete(state.limits, subscriber)}
  end

  defp bump_generation(state, subscriber) do
    generation = Map.get(state.generations, subscriber, 0) + 1
    %{state | generations: Map.put(state.generations, subscriber, generation)}
  end

  defp put_limit(state, subscriber, count) when is_integer(count) and count > 0 do
    generation = Map.fetch!(state.generations, subscriber)
    limit = %{generation: generation, remaining: count, in_flight: 0}
    %{state | limits: Map.put(state.limits, subscriber, limit)}
  end

  defp maybe_decrement_limit(state, subscriber, generation) do
    case Map.get(state.limits, subscriber) do
      %{generation: ^generation, remaining: remaining, in_flight: in_flight} = limit
      when in_flight > 0 ->
        updated_limit = %{limit | remaining: remaining - 1, in_flight: in_flight - 1}
        maybe_finalize_limit(state, subscriber, updated_limit)

      _ ->
        state
    end
  end

  defp do_prepare_subscribers(subscribers, state) do
    {admitted, snapshot, limits} =
      Enum.reduce(subscribers, {[], %{}, state.limits}, fn subscriber, {admitted, snapshot, limits} ->
        case Map.get(limits, subscriber) do
          nil ->
            generation = Map.get(state.generations, subscriber, 0)
            {[subscriber | admitted], Map.put(snapshot, subscriber, generation), limits}

          %{generation: generation, remaining: remaining, in_flight: in_flight} = limit
          when remaining > in_flight ->
            updated_limit = %{limit | in_flight: in_flight + 1}

            {[
               subscriber | admitted
             ], Map.put(snapshot, subscriber, generation), Map.put(limits, subscriber, updated_limit)}

          _ ->
            {admitted, snapshot, limits}
        end
      end)

    {Enum.reverse(admitted), snapshot, %{state | limits: limits}}
  end

  defp maybe_finalize_limit(state, subscriber, %{remaining: 0, in_flight: 0}) do
    @backend.unsubscribe(subscriber)
    :ets.delete(@opts_table, subscriber)
    clear_subscription_state(state, subscriber)
  end

  defp maybe_finalize_limit(state, subscriber, updated_limit) do
    %{state | limits: Map.put(state.limits, subscriber, updated_limit)}
  end

  # Normalize bare module subscribers to {module, nil} so all downstream
  # code can assume a uniform {module, config} shape.
  defp normalize(subscriber) when is_atom(subscriber), do: {subscriber, nil}
  defp normalize({_module, _config} = subscriber), do: subscriber
end
