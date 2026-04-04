defmodule EventBus.Service.NotificationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Notification

  alias EventBus.Support.Helper.{
    AnotherBadOne,
    AnotherCalculator,
    BadOne,
    Calculator,
    InputLogger,
    MemoryLeakerOne
  }

  doctest Notification

  @topic :metrics_received
  @event %Event{
    id: "E1",
    transaction_id: "T1",
    topic: @topic,
    data: [1, 2],
    source: "NotificationTest"
  }

  setup do
    for topic <- EventBus.topics() do
      EventBus.unregister_topic(topic)
    end

    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    :ok
  end

  test "notify" do
    EventBus.register_topic(:metrics_received)
    EventBus.register_topic(:metrics_summed)

    EventBus.subscribe(
      {{InputLogger, %{}}, ["metrics_received$", "metrics_summed$"]}
    )

    EventBus.subscribe({{BadOne, %{}}, [".*"]})
    EventBus.subscribe({AnotherBadOne, [".*"]})
    EventBus.subscribe({{Calculator, %{}}, ["metrics_received$"]})
    EventBus.subscribe({{MemoryLeakerOne, %{}}, [".*"]})

    # This subscriber deos not have a config!!!
    EventBus.subscribe({AnotherCalculator, ["metrics_received$"]})

    logs =
      capture_log(fn ->
        Notification.notify(@event)
        Process.sleep(200)
      end)

    assert String.contains?(logs, "BadOne.process/1 raised an error!")

    assert String.contains?(logs, "AnotherBadOne.process/1 raised an error!")

    assert String.contains?(logs, "I don't want to handle your event")

    assert String.contains?(logs, "Event log for %EventBus.Model.Event{")
    assert String.contains?(logs, "id: \"E1\"")
    assert String.contains?(logs, "data: [1, 2]")
    assert String.contains?(logs, "topic: :metrics_received")

    assert String.contains?(logs, "id: \"E123\"")
    assert String.contains?(logs, "data: {3, [1, 2]}")
    assert String.contains?(logs, "source: \"Logger\"")

    assert String.contains?(logs, "source: \"AnotherCalculator\"")
  end

  test "notify without subscribers" do
    EventBus.register_topic(:metrics_received)

    logs =
      capture_log(fn ->
        Notification.notify(@event)
        Process.sleep(100)
      end)

    assert String.contains?(
             logs,
             "Topic :metrics_received doesn't have subscribers"
           )
  end
end
