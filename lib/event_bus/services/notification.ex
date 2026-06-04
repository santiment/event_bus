defmodule EventBus.Service.Notification do
  @moduledoc false

  require Logger

  alias EventBus.CancelEvent
  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Model.Event
  alias EventBus.Service.Debug
  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService
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
      {admitted_subscribers, snapshot} =
        SubscriptionManager.prepare_subscribers_for_dispatch(subscribers)

      if admitted_subscribers == [] do
        Debug.log(
          "notify_dropped topic=#{inspect(topic)} id=#{inspect(id)} reason=no_admitted_subscribers"
        )
      else
        Debug.log("notify topic=#{inspect(topic)} id=#{inspect(id)}")

        :ok = StoreService.create(event)

        :ok =
          ObservationService.save({topic, id}, {admitted_subscribers, [], []})

        :ok = ObservationService.save_snapshot({topic, id}, snapshot)

        start_time = System.monotonic_time()

        Telemetry.execute(
          [:event_bus, :notify, :start],
          %{system_time: System.system_time()},
          %{topic: topic, event_id: id}
        )

        notify_subscribers(admitted_subscribers, event, start_time)

        duration = System.monotonic_time() - start_time

        Telemetry.execute(
          [:event_bus, :notify, :stop],
          %{duration: duration},
          %{
            topic: topic,
            event_id: id,
            subscriber_count: length(admitted_subscribers)
          }
        )
      end
    end

    :ok
  end

  @spec notify_subscribers(subscribers(), event(), integer()) :: :ok
  defp notify_subscribers(
         subscribers,
         %Event{id: id, topic: topic} = event,
         start_time
       ) do
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
    Enum.each(subscribers, fn sub_key ->
      Debug.log(
        "skipped_by_cancel topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
      )

      ObservationService.mark_as_skipped({sub_key, {topic, id}})
    end)
  end

  defp sort_by_priority(subscribers) do
    Enum.sort_by(
      subscribers,
      fn sub -> -SubscriptionManager.fetch_opts(sub).priority end,
      &<=/2
    )
  end

  defp dispatch_subscriber(sub_key, event, {topic, id}, start_time) do
    opts = SubscriptionManager.fetch_opts(sub_key)

    case evaluate_guard(opts.guard, event, sub_key, topic, id) do
      :pass ->
        do_dispatch(sub_key, {topic, id}, start_time)

      :skip ->
        ObservationService.mark_as_skipped({sub_key, {topic, id}})
        :ok
    end
  end

  defp evaluate_guard(nil, _event, _sub_key, _topic, _id), do: :pass

  defp evaluate_guard(guard, event, sub_key, topic, id)
       when is_function(guard, 1) do
    if guard.(event) do
      :pass
    else
      Debug.log(
        "guard_skipped topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
      )

      :skip
    end
  rescue
    error ->
      stacktrace = __STACKTRACE__

      Logger.error(
        "Guard for #{inspect(sub_key)} raised an error!\n#{Exception.format(:error, error, stacktrace)}"
      )

      :skip
  end

  # All subscribers are now normalized to {module, config} tuples.
  # Config-less subscribers have config=nil; we call process({topic, id}) for those
  # and process({config, topic, id}) for configured subscribers.
  defp do_dispatch({subscriber, config} = sub_key, {topic, id}, start_time) do
    Debug.log(
      "dispatch topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
    )

    Debug.record_dispatch(sub_key, topic, id)

    call_args = if is_nil(config), do: {topic, id}, else: {config, topic, id}

    case subscriber.process(call_args) do
      {:cancel, reason} ->
        ObservationService.mark_as_completed({sub_key, {topic, id}})

        Debug.log(
          "cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)} reason=#{inspect(reason)}"
        )

        :cancelled

      _ ->
        :ok
    end
  rescue
    error ->
      case error do
        %CancelEvent{reason: reason} ->
          Debug.log(
            "cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)} reason=#{inspect(reason)}"
          )

          ObservationService.mark_as_skipped({sub_key, {topic, id}})
          :cancelled

        _ ->
          stacktrace = __STACKTRACE__
          duration = System.monotonic_time() - start_time
          log_error(subscriber, error, stacktrace)

          emit_exception_telemetry(
            subscriber,
            topic,
            id,
            duration,
            error,
            stacktrace
          )

          ObservationService.mark_as_skipped({sub_key, {topic, id}})
          :ok
      end
  end

  defp emit_exception_telemetry(
         subscriber,
         topic,
         id,
         duration,
         error,
         stacktrace
       ) do
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
