defmodule EventBus do
  @moduledoc """
  Traceable, extendable and minimalist event bus implementation for Elixir with
  built-in event store and event observation manager based on ETS.
  """

  alias EventBus.Manager.Subscription

  alias EventBus.Model.Event
  alias EventBus.Service.Debug
  alias EventBus.Service.Notification, as: NotificationService
  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Service.Topic, as: TopicService

  @typedoc "EventBus.Model.Event struct"
  @type event :: Event.t()

  @typedoc "Event id"
  @type event_id :: String.t() | integer()

  @typedoc "Tuple of topic name and event id"
  @type event_shadow :: {topic(), event_id()}

  @typedoc "Event subscriber"
  @type subscriber :: subscriber_without_config() | subscriber_with_config()

  @typedoc "Subscriber configuration"
  @type subscriber_config :: any()

  @typedoc "List of event subscribers"
  @type subscribers :: list(subscriber())

  @typedoc "Event subscriber with config"
  @type subscriber_with_config :: {module(), subscriber_config()}

  @typedoc "Tuple of subscriber and event reference"
  @type subscriber_with_event_ref ::
          subscriber_with_event_shadow() | subscriber_with_topic_and_event_id()

  @typedoc "Tuple of subscriber and event shadow"
  @type subscriber_with_event_shadow :: {subscriber(), event_shadow()}

  @typedoc "Tuple of subscriber, topic and event id"
  @type subscriber_with_topic_and_event_id ::
          {subscriber(), topic(), event_id()}

  @typedoc "Tuple of subscriber and list of topic patterns"
  @type subscriber_with_topic_patterns :: {subscriber(), topic_patterns()}

  @typedoc "Event subscriber without config"
  @type subscriber_without_config :: module()

  @typedoc "Topic name"
  @type topic :: atom()

  @typedoc "List of topic names"
  @type topics :: list(topic())

  @typedoc "Regex pattern to match topic name"
  @type topic_pattern :: String.t()

  @typedoc "List of topic patterns"
  @type topic_patterns :: list(topic_pattern())

  @doc """
  Send an event to all subscribers asynchronously.

  The event is dispatched in a separate task, allowing concurrent processing
  of multiple events. Returns immediately.

  ## Examples

      event = %Event{id: 1, topic: :webhook_received,
        data: %{"message" => "Hi all!"}}
      EventBus.notify(event)
      :ok

  """
  @spec notify(event()) :: :ok
  def notify(%Event{} = event) do
    Task.Supervisor.start_child(EventBus.TaskSupervisor, fn ->
      NotificationService.notify(event)
    end)

    :ok
  end

  @doc """
  Send an event to all subscribers synchronously in the calling process.

  Blocks until all subscribers have been dispatched.

  ## Examples

      event = %Event{id: 1, topic: :webhook_received,
        data: %{"message" => "Hi all!"}}
      EventBus.notify_sync(event)
      :ok

  """
  @spec notify_sync(event()) :: :ok
  def notify_sync(%Event{} = event) do
    NotificationService.notify(event)
  end

  @doc """
  Check if a topic registered.

  ## Examples

      EventBus.topic_exist?(:demo_topic)
      true

  """
  @spec topic_exist?(topic()) :: boolean()
  defdelegate topic_exist?(topic),
    to: TopicService,
    as: :exist?

  @doc """
  List all the registered topics.

  ## Examples

      EventBus.topics()
      [:metrics_summed]

  """
  @spec topics() :: topics()
  defdelegate topics,
    to: TopicService,
    as: :all

  @doc """
  Register a topic.

  ## Examples

      EventBus.register_topic(:demo_topic)
      :ok

  """
  @spec register_topic(topic()) :: :ok
  defdelegate register_topic(topic),
    to: TopicService,
    as: :register

  @doc """
  Unregister a topic.

  ## Examples

      EventBus.unregister_topic(:demo_topic)
      :ok

  """
  @spec unregister_topic(topic()) :: :ok
  defdelegate unregister_topic(topic),
    to: TopicService,
    as: :unregister

  @doc """
  Subscribe a subscriber to the event bus.

  ## Examples

      EventBus.subscribe({MyEventSubscriber, [".*"]})
      :ok

      # For configurable subscribers you can pass tuple of subscriber and config
      my_config = %{}
      EventBus.subscribe({{OtherSubscriber, my_config}, [".*"]})
      :ok

  """
  @spec subscribe(subscriber_with_topic_patterns()) :: :ok
  defdelegate subscribe(subscriber_with_topic_patterns),
    to: Subscription,
    as: :subscribe

  @doc """
  Subscribe a subscriber with options.

  Supported options:

    * `:priority` - integer, higher runs first (default `0`)
    * `:guard` - 1-arity function receiving `%Event{}`, return truthy to dispatch

  ## Examples

      EventBus.subscribe({MySubscriber, ["order_.*"]},
        guard: fn event -> event.data.amount > 1000 end,
        priority: 100
      )
      :ok

  """
  @spec subscribe(subscriber_with_topic_patterns(), keyword()) :: :ok
  defdelegate subscribe(subscriber_with_topic_patterns, opts),
    to: Subscription,
    as: :subscribe

  @doc """
  Subscribe a subscriber that auto-unsubscribes after one terminal event.

  ## Examples

      EventBus.subscribe_once({MyEventSubscriber, [".*"]})
      :ok

  """
  @spec subscribe_once(subscriber_with_topic_patterns()) :: :ok
  defdelegate subscribe_once(subscriber_with_topic_patterns),
    to: Subscription,
    as: :subscribe_once

  @doc """
  Subscribe a subscriber that auto-unsubscribes after N terminal events.

  ## Examples

      EventBus.subscribe_n({MyEventSubscriber, [".*"]}, 5)
      :ok

  """
  @spec subscribe_n(subscriber_with_topic_patterns(), pos_integer()) :: :ok
  defdelegate subscribe_n(subscriber_with_topic_patterns, count),
    to: Subscription,
    as: :subscribe_n

  @doc """
  Unsubscribe a subscriber from the event bus.

  ## Examples

      EventBus.unsubscribe(MyEventSubscriber)
      :ok

      # For configurable subscribers you must pass tuple of subscriber and config
      my_config = %{}
      EventBus.unsubscribe({OtherSubscriber, my_config})
      :ok

  """
  @spec unsubscribe(subscriber()) :: :ok
  defdelegate unsubscribe(subscriber),
    to: Subscription,
    as: :unsubscribe

  @doc """
  Check if the given subscriber subscribed to the event bus for the given topic
  patterns.

  ## Examples

      EventBus.subscribe({MyEventSubscriber, [".*"]})
      :ok

      EventBus.subscribed?({MyEventSubscriber, [".*"]})
      true

      EventBus.subscribed?({MyEventSubscriber, ["some_initialized"]})
      false

      EventBus.subscribed?({AnothEventSubscriber, [".*"]})
      false

  """
  @spec subscribed?(subscriber_with_topic_patterns()) :: boolean()
  defdelegate subscribed?(subscriber_with_topic_patterns),
    to: Subscription,
    as: :subscribed?

  @doc """
  List the subscribers.

  ## Examples

      EventBus.subscribers()
      [MyEventSubscriber]

      # One usual and one configured subscriber with its config
      EventBus.subscribers()
      [MyEventSubscriber, {OtherSubscriber, %{}}]

  """
  @spec subscribers() :: subscribers()
  defdelegate subscribers,
    to: Subscription,
    as: :subscribers

  @doc """
  List the subscribers for the given topic.

  ## Examples

      EventBus.subscribers(:metrics_received)
      [MyEventSubscriber]

      # One usual and one configured subscriber with its config
      EventBus.subscribers(:metrics_received)
      [MyEventSubscriber, {OtherSubscriber, %{}}]

  """
  @spec subscribers(topic()) :: subscribers()
  defdelegate subscribers(topic),
    to: Subscription,
    as: :subscribers

  @doc """
  Fetch an event.

  ## Examples

      EventBus.fetch_event({:hello_received, "123"})
      %EventBus.Model.Event{}

  """
  @spec fetch_event(event_shadow()) :: event() | nil
  defdelegate fetch_event(event_shadow),
    to: StoreService,
    as: :fetch

  @doc """
  Fetch an event's data.

  ## Examples

      EventBus.fetch_event_data({:hello_received, "123"})

  """
  @spec fetch_event_data(event_shadow()) :: any()
  defdelegate fetch_event_data(event_shadow),
    to: StoreService,
    as: :fetch_data

  @doc """
  Mark the event as completed for the subscriber.

  ## Examples

      topic        = :hello_received
      event_id     = "124"
      event_shadow = {topic, event_id}

      # For regular subscribers
      EventBus.mark_as_completed({MyEventSubscriber, event_shadow})

      # For configurable subscribers you must pass tuple of subscriber and config
      my_config = %{}
      subscriber  = {OtherSubscriber, my_config}

      EventBus.mark_as_completed({subscriber, event_shadow})
      :ok

  """
  @spec mark_as_completed(subscriber_with_event_ref()) :: :ok
  def mark_as_completed({subscriber, {topic, id}}),
    do: ObservationService.mark_as_completed({subscriber, {topic, id}})

  def mark_as_completed({subscriber, topic, id}),
    do: ObservationService.mark_as_completed({subscriber, {topic, id}})

  @doc """
  Mark the event as skipped for the subscriber.

  ## Examples

      EventBus.mark_as_skipped({MyEventSubscriber, {:unmatched_occurred, "124"}})

      # For configurable subscribers you must pass tuple of subscriber and config
      my_config = %{}
      subscriber  = {OtherSubscriber, my_config}
      EventBus.mark_as_skipped({subscriber, {:unmatched_occurred, "124"}})
      :ok

  """
  @spec mark_as_skipped(subscriber_with_event_ref()) :: :ok
  def mark_as_skipped({subscriber, {topic, id}}),
    do: ObservationService.mark_as_skipped({subscriber, {topic, id}})

  def mark_as_skipped({subscriber, topic, id}),
    do: ObservationService.mark_as_skipped({subscriber, {topic, id}})

  @doc """
  Toggle debug mode on or off.

  When enabled, EventBus logs the full lifecycle of every event
  using `Logger.debug/1`.

  ## Examples

      EventBus.toggle_debug(true)
      :ok

      EventBus.toggle_debug(false)
      :ok

  """
  @spec toggle_debug(boolean()) :: :ok
  defdelegate toggle_debug(enabled),
    to: Debug,
    as: :toggle
end
