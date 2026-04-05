defmodule EventBus.Manager.Subscription do
  @moduledoc false

  ###########################################################################
  # Subscription manager
  ###########################################################################

  use GenServer

  alias EventBus.Service.Subscription, as: SubscriptionService

  @typep subscriber :: EventBus.subscriber()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_topic_patterns ::
           EventBus.subscriber_with_topic_patterns()
  @typep topic :: EventBus.topic()

  @backend SubscriptionService

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def init(_opts) do
    # Control-plane data lives in the GenServer state instead of ETS:
    #   * opts: validated guard/priority settings
    #   * limits: remaining subscribe_once/subscribe_n counters
    #   * generations: monotonically increasing version per subscriber
    #
    # The generation lets us bind a terminal event back to the subscription
    # version that was active when the event was dispatched. Without it, an
    # older async completion can spend the limit of a freshly re-subscribed
    # subscriber.
    {:ok, %{opts: %{}, limits: %{}, generations: %{}}}
  end

  @doc """
  Does the subscriber subscribe to topic_patterns?
  """
  @spec subscribed?(subscriber_with_topic_patterns()) :: boolean()
  def subscribed?({_subscriber, _topic_patterns} = subscriber) do
    GenServer.call(__MODULE__, {:subscribed?, subscriber})
  end

  @doc """
  Subscribe the subscriber to topic_patterns
  """
  @spec subscribe(subscriber_with_topic_patterns()) :: :ok
  def subscribe({subscriber, topic_patterns}) do
    GenServer.call(__MODULE__, {:subscribe, {subscriber, topic_patterns}})
  end

  @doc """
  Subscribe the subscriber to topic_patterns with options (guard, priority)
  """
  @spec subscribe(subscriber_with_topic_patterns(), keyword()) :: :ok
  def subscribe({subscriber, topic_patterns}, opts) do
    GenServer.call(__MODULE__, {:subscribe_with_opts, {subscriber, topic_patterns}, opts})
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after one terminal event
  """
  @spec subscribe_once(subscriber_with_topic_patterns()) :: :ok
  def subscribe_once({subscriber, topic_patterns}) do
    GenServer.call(__MODULE__, {:subscribe_n, {subscriber, topic_patterns}, 1})
  end

  @doc """
  Subscribe the subscriber, auto-unsubscribe after N terminal events
  """
  @spec subscribe_n(subscriber_with_topic_patterns(), pos_integer()) :: :ok
  def subscribe_n({subscriber, topic_patterns}, count) do
    GenServer.call(__MODULE__, {:subscribe_n, {subscriber, topic_patterns}, count})
  end

  @doc """
  Unsubscribe the subscriber
  """
  @spec unsubscribe(subscriber()) :: :ok
  def unsubscribe(subscriber) do
    GenServer.call(__MODULE__, {:unsubscribe, subscriber})
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

  @doc false
  @spec fetch_opts(subscriber()) :: %{guard: function() | nil, priority: integer()}
  def fetch_opts(subscriber) do
    GenServer.call(__MODULE__, {:fetch_opts, subscriber})
  end

  @doc false
  @spec snapshot_generations(subscribers()) :: %{optional(subscriber()) => non_neg_integer()}
  def snapshot_generations(subscribers) do
    GenServer.call(__MODULE__, {:snapshot_generations, subscribers})
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
  @spec handle_call(
          {:subscribed?, subscriber_with_topic_patterns()},
          any(),
          term()
        ) ::
          {:reply, boolean(), term()}
  def handle_call({:subscribed?, subscriber}, _from, state) do
    {:reply, @backend.subscribed?(subscriber), state}
  end

  @doc false
  @spec handle_call(
          {:subscribe, subscriber_with_topic_patterns()},
          any(),
          term()
        ) ::
          {:reply, :ok, term()}
  def handle_call({:subscribe, {subscriber, topic_patterns}}, _from, state) do
    @backend.subscribe({subscriber, topic_patterns})
    {:reply, :ok, reset_subscription_state(state, subscriber)}
  end

  @doc false
  def handle_call({:subscribe_n, {subscriber, topic_patterns}, count}, _from, state) do
    # Reuse the same ETS-backed subscriber/topic registration path as a normal
    # subscription, then track the per-subscription limit only in manager state.
    @backend.subscribe({subscriber, topic_patterns})

    state =
      state
      |> reset_subscription_state(subscriber)
      |> put_limit(subscriber, count)

    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:subscribe_with_opts, {subscriber, topic_patterns}, opts}, _from, state) do
    # Validate options before mutating state so invalid priorities/guards fail
    # fast at subscribe time instead of crashing the notifier later.
    normalized_opts = normalize_opts!(opts)
    @backend.subscribe({subscriber, topic_patterns})

    state =
      state
      |> reset_subscription_state(subscriber)
      |> put_opts(subscriber, normalized_opts)

    {:reply, :ok, state}
  end

  @doc false
  @spec handle_call({:unsubscribe, subscriber()}, any(), term()) ::
          {:reply, :ok, term()}
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    @backend.unsubscribe(subscriber)
    {:reply, :ok, clear_subscription_state(state, subscriber)}
  end

  @doc false
  @spec handle_call({:register_topic, topic()}, any(), term()) ::
          {:reply, :ok, term()}
  def handle_call({:register_topic, topic}, _from, state) do
    @backend.register_topic(topic)
    {:reply, :ok, state}
  end

  @doc false
  @spec handle_call({:unregister_topic, topic()}, any(), term()) ::
          {:reply, :ok, term()}
  def handle_call({:unregister_topic, topic}, _from, state) do
    @backend.unregister_topic(topic)
    {:reply, :ok, state}
  end

  @doc false
  def handle_call({:fetch_opts, subscriber}, _from, state) do
    {:reply, Map.get(state.opts, subscriber, %{priority: 0, guard: nil}), state}
  end

  @doc false
  def handle_call({:snapshot_generations, subscribers}, _from, state) do
    # Capture the generation that was current at dispatch time for each
    # subscriber. Observation stores this snapshot alongside the watcher entry
    # and uses it later when terminal states arrive asynchronously.
    snapshot =
      Enum.into(subscribers, %{}, fn subscriber ->
        {subscriber, Map.get(state.generations, subscriber, 0)}
      end)

    {:reply, snapshot, state}
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

  defp reset_subscription_state(state, subscriber) do
    # Any re-subscribe creates a new subscription version. We intentionally
    # clear prior opts/limits so plain subscribe/1 replaces subscribe_n/2 or
    # subscribe/2 rather than merging hidden state across subscriptions.
    state
    |> bump_generation(subscriber)
    |> clear_subscription_state(subscriber)
  end

  defp clear_subscription_state(state, subscriber) do
    %{state | opts: Map.delete(state.opts, subscriber), limits: Map.delete(state.limits, subscriber)}
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

  defp put_opts(state, subscriber, opts) do
    %{state | opts: Map.put(state.opts, subscriber, opts)}
  end

  defp maybe_decrement_limit(state, subscriber, generation) do
    case Map.get(state.limits, subscriber) do
      %{generation: ^generation, remaining: remaining} when remaining <= 1 ->
        # Only consume the limit if the terminal event belongs to the currently
        # active subscription generation.
        @backend.unsubscribe(subscriber)
        clear_subscription_state(state, subscriber)

      %{generation: ^generation, remaining: remaining} ->
        %{state | limits: Map.put(state.limits, subscriber, %{generation: generation, remaining: remaining - 1})}

      _ ->
        state
    end
  end
end
