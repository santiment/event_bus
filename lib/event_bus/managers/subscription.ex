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
    {:ok, nil}
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
    {:reply, :ok, state}
  end

  @doc false
  @spec handle_call({:unsubscribe, subscriber()}, any(), term()) ::
          {:reply, :ok, term()}
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    @backend.unsubscribe(subscriber)
    {:reply, :ok, state}
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
end
