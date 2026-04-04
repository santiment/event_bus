defmodule EventBus.Service.Notification do
  @moduledoc false

  require Logger

  alias EventBus.Manager.Observation, as: ObservationManager
  alias EventBus.Manager.Store, as: StoreManager
  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Model.Event
  alias EventBus.Telemetry

  @typep event :: EventBus.event()
  @typep event_shadow :: EventBus.event_shadow()
  @typep subscriber :: EventBus.subscriber()
  @typep subscribers :: EventBus.subscribers()
  @typep topic :: EventBus.topic()

  @doc false
  @spec notify(event()) :: :ok
  def notify(%Event{id: id, topic: topic} = event) do
    subscribers = SubscriptionManager.subscribers(topic)

    if subscribers == [] do
      warn_missing_topic_subscription(topic)
    else
      :ok = StoreManager.create(event)
      :ok = ObservationManager.create({subscribers, {topic, id}})

      start_time = System.monotonic_time()

      Telemetry.execute(
        [:event_bus, :notify, :start],
        %{system_time: System.system_time()},
        %{topic: topic, event_id: id}
      )

      notify_subscribers(subscribers, {topic, id})

      duration = System.monotonic_time() - start_time

      Telemetry.execute(
        [:event_bus, :notify, :stop],
        %{duration: duration},
        %{topic: topic, event_id: id, subscriber_count: length(subscribers)}
      )
    end

    :ok
  end

  @spec notify_subscribers(subscribers(), event_shadow()) :: :ok
  defp notify_subscribers(subscribers, event_shadow) do
    Enum.each(subscribers, fn subscriber ->
      notify_subscriber(subscriber, event_shadow)
    end)

    :ok
  end

  @spec notify_subscriber(subscriber(), event_shadow()) :: :ok
  defp notify_subscriber({subscriber, config}, {topic, id}) do
    subscriber.process({config, topic, id})
  rescue
    error ->
      stacktrace = __STACKTRACE__
      log_error(subscriber, error, stacktrace)
      emit_exception_telemetry(subscriber, topic, id, error, stacktrace)
      ObservationManager.mark_as_skipped({{subscriber, config}, {topic, id}})
  end

  defp notify_subscriber(subscriber, {topic, id}) do
    subscriber.process({topic, id})
  rescue
    error ->
      stacktrace = __STACKTRACE__
      log_error(subscriber, error, stacktrace)
      emit_exception_telemetry(subscriber, topic, id, error, stacktrace)
      ObservationManager.mark_as_skipped({subscriber, {topic, id}})
  end

  defp emit_exception_telemetry(subscriber, topic, id, error, stacktrace) do
    Telemetry.execute(
      [:event_bus, :notify, :exception],
      %{duration: 0},
      %{
        topic: topic,
        event_id: id,
        subscriber: subscriber,
        kind: :error,
        reason: error,
        stacktrace: stacktrace
      }
    )
  end

  @spec warn_missing_topic_subscription(topic()) :: :ok
  defp warn_missing_topic_subscription(topic) do
    if EventBus.topic_exist?(topic) do
      Logger.warning("Topic :#{topic} doesn't have subscribers")
    else
      Logger.warning(
        "Topic :#{topic} is not registered and has no subscribers"
      )
    end
  end

  @spec log_error(module(), any(), Exception.stacktrace()) :: :ok
  defp log_error(subscriber, error, stacktrace) do
    formatted = Exception.format(:error, error, stacktrace)
    Logger.error("#{subscriber}.process/1 raised an error!\n#{formatted}")
  end
end
