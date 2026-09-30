defmodule EventBus.ConcurrencyInvariantsTest do
  @moduledoc """
  Randomized mix of notify/subscribe/unsubscribe/unregister/sweep from many
  processes. Once quiescent, nothing may be left behind.
  """
  use ExUnit.Case, async: false

  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Model.Event
  alias EventBus.Service.Sweeper

  @topics [:invariant_a, :invariant_b]
  @patterns ["invariant_.*"]

  defmodule Sync do
    def process({cfg, topic, id}) do
      case rem(:erlang.phash2({topic, id}), 5) do
        0 -> EventBus.mark_as_skipped({{__MODULE__, cfg}, {topic, id}})
        1 -> raise "boom"
        2 -> {:cancel, :stop}
        _ -> EventBus.mark_as_completed({{__MODULE__, cfg}, {topic, id}})
      end
    end
  end

  defmodule Async do
    def process({cfg, topic, id}) do
      spawn(fn ->
        Process.sleep(:rand.uniform(3))
        EventBus.mark_as_completed({{__MODULE__, cfg}, {topic, id}})
      end)

      :ok
    end
  end

  setup do
    for {subscriber, _} <- EventBus.subscribers(),
        do: EventBus.unsubscribe(subscriber)

    for topic <- @topics, do: EventBus.register_topic(topic)

    on_exit(fn ->
      for topic <- @topics, do: EventBus.unregister_topic(topic)
    end)

    :ok
  end

  @tag :capture_log
  test "no leaked rows or in-flight reservations after concurrent churn" do
    subscribers =
      for i <- 1..6, do: {if(rem(i, 2) == 0, do: Async, else: Sync), i}

    stop = :atomics.new(1, [])

    workers =
      for w <- 1..10 do
        Task.async(fn -> churn(w, 0, subscribers, stop) end)
      end

    Process.sleep(2_000)
    :atomics.put(stop, 1, 1)
    Enum.each(workers, &Task.await(&1, 30_000))
    Process.sleep(300)

    stranded =
      for {{mod, _} = sub, %{in_flight: in_flight}} <-
            :sys.get_state(SubscriptionManager).limits,
          mod in [Sync, Async] and in_flight > 0,
          do: {sub, in_flight}

    assert [] == stranded

    for topic <- @topics do
      assert [] == :ets.match_object(:eb_event_watchers, {{topic, :_}, :_, :_})
      assert [] == :ets.match_object(:eb_event_store, {{topic, :_}, :_, :_})

      assert [] ==
               :ets.match_object(
                 :eb_event_watcher_status,
                 {{topic, :_, :_}, :_}
               )

      assert [] ==
               :ets.match_object(
                 :eb_event_subscription_generations,
                 {{topic, :_}, :_}
               )
    end
  end

  defp churn(w, n, subscribers, stop) do
    if :atomics.get(stop, 1) == 0 do
      subscriber = Enum.random(subscribers)
      topic = Enum.random(@topics)

      case :rand.uniform(100) do
        x when x <= 60 ->
          EventBus.notify_sync(%Event{id: {w, n}, topic: topic, data: nil})

        x when x <= 70 ->
          EventBus.subscribe_n({subscriber, @patterns}, :rand.uniform(3))

        x when x <= 78 ->
          EventBus.subscribe({subscriber, @patterns},
            priority: :rand.uniform(5)
          )

        x when x <= 84 ->
          EventBus.unsubscribe(subscriber)

        x when x <= 88 ->
          EventBus.subscribe({subscriber, @patterns},
            guard: fn _ -> :rand.uniform(2) == 1 end
          )

        x when x <= 94 ->
          EventBus.unregister_topic(topic)
          EventBus.register_topic(topic)

        _ ->
          Sweeper.sweep(System.convert_time_unit(5, :millisecond, :native))
      end

      churn(w, n + 1, subscribers, stop)
    end
  end
end
