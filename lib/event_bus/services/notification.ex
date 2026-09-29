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
  @typep topic :: EventBus.topic()

  @doc false
  @spec notify(event()) :: :ok
  def notify(%Event{id: id, topic: topic} = event) do
    subscriber_pairs = SubscriptionManager.subscribers_with_opts(topic)

    if subscriber_pairs == [] do
      warn_missing_topic_subscription(topic)
    else
      {admitted_pairs, snapshot} = admit_subscribers(subscriber_pairs)

      if admitted_pairs == [] do
        Debug.log(fn ->
          "notify_dropped topic=#{inspect(topic)} id=#{inspect(id)} reason=no_admitted_subscribers"
        end)
      else
        Debug.log(fn -> "notify topic=#{inspect(topic)} id=#{inspect(id)}" end)

        :ok = StoreService.create(event)

        # Save before observation rows: a terminal transition needs the snapshot.
        if map_size(snapshot) > 0,
          do: ObservationService.save_snapshot({topic, id}, snapshot)

        admitted_subscribers =
          Enum.map(admitted_pairs, fn {sub, _opts} -> sub end)

        :ok =
          ObservationService.save({topic, id}, {admitted_subscribers, [], []})

        start_time = System.monotonic_time()

        Telemetry.execute(
          [:event_bus, :notify, :start],
          %{system_time: System.system_time()},
          %{topic: topic, event_id: id}
        )

        dispatch_in_order(admitted_pairs, event, {topic, id}, start_time)

        duration = System.monotonic_time() - start_time

        Telemetry.execute(
          [:event_bus, :notify, :stop],
          %{duration: duration},
          %{
            topic: topic,
            event_id: id,
            subscriber_count: length(admitted_pairs)
          }
        )
      end
    end

    :ok
  end

  @spec admit_subscribers([{EventBus.subscriber(), map()}]) ::
          {[{EventBus.subscriber(), map()}], map()}
  defp admit_subscribers(subscriber_pairs) do
    candidates =
      for {sub, %{limit_generation: generation}} <- subscriber_pairs,
          not is_nil(generation),
          do: {sub, generation}

    if candidates == [] do
      {subscriber_pairs, %{}}
    else
      {admitted, snapshot} =
        SubscriptionManager.prepare_subscribers_for_dispatch(candidates)

      admitted_set = MapSet.new(admitted)

      admitted_pairs =
        Enum.filter(subscriber_pairs, fn {sub, opts} ->
          is_nil(Map.get(opts, :limit_generation)) or
            MapSet.member?(admitted_set, sub)
        end)

      {admitted_pairs, snapshot}
    end
  end

  defp dispatch_in_order([], _event, _event_shadow, _start_time), do: :ok

  defp dispatch_in_order(
         [{sub_key, opts} | rest],
         event,
         {topic, id},
         start_time
       ) do
    case dispatch_subscriber(sub_key, opts, event, {topic, id}, start_time) do
      :cancelled ->
        skip_remaining(rest, {topic, id})

      _ ->
        dispatch_in_order(rest, event, {topic, id}, start_time)
    end
  end

  defp skip_remaining(subscribers, {topic, id}) do
    Enum.each(subscribers, fn {sub_key, _opts} ->
      Debug.log(fn ->
        "skipped_by_cancel topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
      end)

      ObservationService.discard_undelivered({sub_key, {topic, id}})
    end)
  end

  defp dispatch_subscriber(sub_key, opts, event, {topic, id}, start_time) do
    case evaluate_guard(opts.guard, event, sub_key, topic, id) do
      :pass ->
        do_dispatch(sub_key, {topic, id}, start_time)

      :skip ->
        ObservationService.discard_undelivered({sub_key, {topic, id}})
        :ok
    end
  end

  defp evaluate_guard(nil, _event, _sub_key, _topic, _id), do: :pass

  defp evaluate_guard(guard, event, sub_key, topic, id)
       when is_function(guard, 1) do
    if guard.(event) do
      :pass
    else
      Debug.log(fn ->
        "guard_skipped topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
      end)

      :skip
    end
  rescue
    error ->
      stacktrace = __STACKTRACE__

      Logger.error(
        "Guard for #{inspect(sub_key)} raised an error!\n#{Exception.format(:error, error, stacktrace)}"
      )

      :skip
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__

      Logger.error(
        "Guard for #{inspect(sub_key)} #{kind}ed!\n#{Exception.format(kind, reason, stacktrace)}"
      )

      :skip
  end

  defp do_dispatch({subscriber, config} = sub_key, {topic, id}, start_time) do
    Debug.log(fn ->
      "dispatch topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)}"
    end)

    Debug.record_dispatch(sub_key, topic, id)

    call_args = if is_nil(config), do: {topic, id}, else: {config, topic, id}

    case subscriber.process(call_args) do
      {:cancel, reason} ->
        ObservationService.mark_as_completed({sub_key, {topic, id}})

        Debug.log(fn ->
          "cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)} reason=#{inspect(reason)}"
        end)

        :cancelled

      _ ->
        :ok
    end
  rescue
    error ->
      case error do
        %CancelEvent{reason: reason} ->
          Debug.log(fn ->
            "cancelled topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(sub_key)} reason=#{inspect(reason)}"
          end)

          ObservationService.mark_as_skipped({sub_key, {topic, id}})
          :cancelled

        _ ->
          handle_dispatch_crash(
            sub_key,
            {topic, id},
            start_time,
            :error,
            error,
            __STACKTRACE__
          )
      end
  catch
    kind, reason ->
      handle_dispatch_crash(
        sub_key,
        {topic, id},
        start_time,
        kind,
        reason,
        __STACKTRACE__
      )
  end

  defp handle_dispatch_crash(
         {subscriber, _config} = sub_key,
         {topic, id},
         start_time,
         kind,
         reason,
         stacktrace
       ) do
    duration = System.monotonic_time() - start_time
    log_error(subscriber, kind, reason, stacktrace)

    emit_exception_telemetry(
      subscriber,
      topic,
      id,
      duration,
      kind,
      reason,
      stacktrace
    )

    ObservationService.mark_as_skipped({sub_key, {topic, id}})
    :ok
  end

  defp emit_exception_telemetry(
         subscriber,
         topic,
         id,
         duration,
         kind,
         reason,
         stacktrace
       ) do
    Telemetry.execute(
      [:event_bus, :notify, :exception],
      %{duration: duration},
      %{
        topic: topic,
        event_id: id,
        subscriber: subscriber,
        kind: kind,
        reason: reason,
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

  @spec log_error(
          module(),
          :error | :exit | :throw,
          any(),
          Exception.stacktrace()
        ) :: :ok
  defp log_error(subscriber, kind, reason, stacktrace) do
    formatted = Exception.format(kind, reason, stacktrace)
    Logger.error("#{subscriber}.process/1 raised an error!\n#{formatted}")
  end
end
