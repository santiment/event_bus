defmodule EventBus.Manager.Subscription do
  @moduledoc false

  ###########################################################################
  # Subscription manager
  #
  # The GenServer serializes writes to subscription control-plane data
  # (opts, limits, generations). Reads of opts and generations go directly
  # through ETS for zero-overhead access on the notification hot path.
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
    # limits: remaining subscribe_once/subscribe_n counters (read-modify-write,
    #   stays in GenServer state for serialized access)
    # generations: monotonically increasing version per subscriber (authoritative
    #   copy lives here; a snapshot is written to ETS for lock-free reads)
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
  Snapshot the current generation for each subscriber directly from ETS.
  No GenServer.call — this is on the notification hot path.
  """
  @spec snapshot_generations(subscribers()) :: %{optional(subscriber()) => non_neg_integer()}
  def snapshot_generations(subscribers) do
    Enum.into(subscribers, %{}, fn subscriber ->
      generation =
        case :ets.lookup(@opts_table, subscriber) do
          [{^subscriber, %{generation: gen}}] -> gen
          _ -> 0
        end

      {subscriber, generation}
    end)
  end

  @doc false
  @spec decrement_limit(subscriber(), non_neg_integer()) :: :ok
  def decrement_limit(subscriber, generation) do
    GenServer.call(__MODULE__, {:decrement_limit, subscriber, generation})
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
    @backend.subscribe({subscriber, topic_patterns})
    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, %{priority: 0, guard: nil}, state)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:subscribe_n, {subscriber, topic_patterns}, count}, _from, state) do
    @backend.subscribe({subscriber, topic_patterns})

    state =
      state
      |> reset_subscription_state(subscriber)
      |> put_limit(subscriber, count)

    write_opts_to_ets(subscriber, %{priority: 0, guard: nil}, state)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:subscribe_with_opts, {subscriber, topic_patterns}, opts}, _from, state) do
    normalized_opts = normalize_opts!(opts)
    @backend.subscribe({subscriber, topic_patterns})

    state = reset_subscription_state(state, subscriber)
    write_opts_to_ets(subscriber, normalized_opts, state)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    @backend.unsubscribe(subscriber)
    :ets.delete(@opts_table, subscriber)
    {:reply, :ok, clear_subscription_state(state, subscriber)}
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
    limit = %{generation: generation, remaining: count}
    %{state | limits: Map.put(state.limits, subscriber, limit)}
  end

  defp maybe_decrement_limit(state, subscriber, generation) do
    case Map.get(state.limits, subscriber) do
      %{generation: ^generation, remaining: remaining} when remaining <= 1 ->
        @backend.unsubscribe(subscriber)
        :ets.delete(@opts_table, subscriber)
        clear_subscription_state(state, subscriber)

      %{generation: ^generation, remaining: remaining} ->
        %{state | limits: Map.put(state.limits, subscriber, %{generation: generation, remaining: remaining - 1})}

      _ ->
        state
    end
  end

  # Normalize bare module subscribers to {module, nil} so all downstream
  # code can assume a uniform {module, config} shape.
  defp normalize(subscriber) when is_atom(subscriber), do: {subscriber, nil}
  defp normalize({_module, _config} = subscriber), do: subscriber
end
