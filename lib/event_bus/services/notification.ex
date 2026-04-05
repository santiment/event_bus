defmodule EventBus.Service.Notification do
  @moduledoc false

  require Logger

  alias EventBus.CancelEvent
  alias EventBus.Manager.Observation, as: ObservationManager
  alias EventBus.Manager.Store, as: StoreManager
  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Model.Event
  alias EventBus.Service.Debug
  alias EventBus.Service.Subscription, as: SubscriptionService
  alias EventBus.Telemetry

  @typep event :: EventBus.event()
  @typep subscribers :: EventBus.subscribers()
  @typep topic :: EventBus.topic()

  @doc false
  @spec notify(event()) :: :ok
  def notify(%Event{id: id, topic: topic} = event) do
    subscribers = SubscriptionManager.subscribers(topic)

    if subscribers == [] do
      warn_missing_topic_subscription(topic)
    else
      Debug.log("notify topic=#{inspect(topic)} id=#{inspect(id)}")

      :ok = StoreManager.create(event)
      :ok = ObservationManager.create({subscribers, {topic, id}})

      start_time = System.monotonic_time()

      Telemetry.execute(
        [:event_bus, :notify, :start],
        %{system_time: System.system_time()},
        %{topic: topic, event_id: id}
      )

      notify_subscribers(subscribers, event, start_time)

      duration = System.monotonic_time() - start_time

      Telemetry.execute(
        [:event_bus, :notify, :stop],
        %{duration: duration},
        %{topic: topic, event_id: id, subscriber_count: length(subscribers)}
      )
    end

    :ok
  end

  @spec notify_subscribers(subscribers(), event(), integer()) :: :ok
  defp notify_subscribers(subscribers, %Event{id: id, topic: topic} = event, start_time) do
    sorted = sort_by_priority(subscribers)
    dispatch_in_order(sorted, event, {topic, id}, start_time)
    :ok
  end

  defp dispatch_in_order([], _event, _event_shadow, _start_time), do: :ok

  defp dispatch_in_order([subscriber | rest], event, {topic, id}, start_time) do
    case dispatch_subscriber(subscriber, event, {topic, id}, start_time) do
      :cancelled ->
        skip_remaining(rest, {topic, id})

      _ ->
        dispatch_in_order(rest, event, {topic, id}, start_time)
    end
  end

  defp skip_remaining(subscribers, {topic, id}) do
    Enum.each(subscribers, fn subscriber ->
      sub_key = subscriber_key(subscriber)
      Debug.log("skipped_by_cancel topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}")
      ObservationManager.mark_as_skipped({sub_key, {topic, id}})
    end)
  end

  defp sort_by_priority(subscribers) do
    Enum.sort_by(subscribers, fn sub -> -fetch_priority(sub) end, &<=/2)
  end

  defp fetch_priority({_subscriber, _config} = sub) do
    SubscriptionService.fetch_opts(sub).priority
  end

  defp fetch_priority(subscriber) do
    SubscriptionService.fetch_opts(subscriber).priority
  end

  defp dispatch_subscriber(subscriber, event, {topic, id}, start_time) do
    sub_key = subscriber_key(subscriber)
    opts = SubscriptionService.fetch_opts(sub_key)

    case evaluate_guard(opts.guard, event, sub_key, topic, id) do
      :pass ->
        do_dispatch(subscriber, {topic, id}, start_time)

      :skip ->
        ObservationManager.mark_as_skipped({sub_key, {topic, id}})
        :ok
    end
  end

  defp evaluate_guard(nil, _event, _sub_key, _topic, _id), do: :pass

  defp evaluate_guard(guard, event, sub_key, topic, id) when is_function(guard, 1) do
    if guard.(event) do
      :pass
    else
      Debug.log("guard_skipped topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}")
      :skip
    end
  rescue
    error ->
      stacktrace = __STACKTRACE__
      Logger.error("Guard for #{inspect(sub_key)} raised an error!\n#{Exception.format(:error, error, stacktrace)}")
      :skip
  end

  defp do_dispatch({subscriber, config} = sub_key, {topic, id}, start_time) do
    Debug.log("dispatch topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}")
    Debug.record_dispatch(sub_key, topic, id)

    case subscriber.process({config, topic, id}) do
      {:cancel, reason} ->
        ObservationManager.mark_as_completed({sub_key, {topic, id}})
        Debug.log("cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)} reason=#{inspect(reason)}")
        :cancelled

      _ ->
        :ok
    end
  rescue
    error ->
      case error do
        %CancelEvent{reason: reason} ->
          Debug.log("cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect({subscriber, config})} reason=#{inspect(reason)}")
          ObservationManager.mark_as_skipped({{subscriber, config}, {topic, id}})
          :cancelled

        _ ->
          stacktrace = __STACKTRACE__
          duration = System.monotonic_time() - start_time
          log_error(subscriber, error, stacktrace)
          emit_exception_telemetry(subscriber, topic, id, duration, error, stacktrace)
          ObservationManager.mark_as_skipped({{subscriber, config}, {topic, id}})
          :ok
      end
  end

  defp do_dispatch(subscriber, {topic, id}, start_time) do
    Debug.log("dispatch topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(subscriber)}")
    Debug.record_dispatch(subscriber, topic, id)

    case subscriber.process({topic, id}) do
      {:cancel, reason} ->
        ObservationManager.mark_as_completed({subscriber, {topic, id}})
        Debug.log("cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(subscriber)} reason=#{inspect(reason)}")
        :cancelled

      _ ->
        :ok
    end
  rescue
    error ->
      case error do
        %CancelEvent{reason: reason} ->
          Debug.log("cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(subscriber)} reason=#{inspect(reason)}")
          ObservationManager.mark_as_skipped({subscriber, {topic, id}})
          :cancelled

        _ ->
          stacktrace = __STACKTRACE__
          duration = System.monotonic_time() - start_time
          log_error(subscriber, error, stacktrace)
          emit_exception_telemetry(subscriber, topic, id, duration, error, stacktrace)
          ObservationManager.mark_as_skipped({subscriber, {topic, id}})
          :ok
      end
  end

  defp subscriber_key({subscriber, config}), do: {subscriber, config}
  defp subscriber_key(subscriber), do: subscriber

  defp emit_exception_telemetry(subscriber, topic, id, duration, error, stacktrace) do
    Telemetry.execute(
      [:event_bus, :notify, :exception],
      %{duration: duration},
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
      Logger.warning("Topic :#{topic} is not registered and has no subscribers")
    end
  end

  @spec log_error(module(), any(), Exception.stacktrace()) :: :ok
  defp log_error(subscriber, error, stacktrace) do
    formatted = Exception.format(:error, error, stacktrace)
    Logger.error("#{subscriber}.process/1 raised an error!\n#{formatted}")
  end
end
