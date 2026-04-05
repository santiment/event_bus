defmodule EventBusTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Notification

  alias EventBus.Support.Helper.{
    BadOne,
    Calculator,
    InputLogger,
    MemoryLeakerOne
  }

  @event %Event{
    id: "M1",
    transaction_id: "T1",
    data: [1, 7],
    topic: :metrics_received,
    source: "EventBusTest"
  }

  setup do
    EventBus.register_topic(:metrics_received)
    EventBus.register_topic(:metrics_summed)

    for {subscriber, _topics} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    :ok
  end

  test "notify" do
    EventBus.subscribe({{InputLogger, %{}}, [".*"]})
    EventBus.subscribe({{BadOne, %{}}, [".*"]})
    EventBus.subscribe({{Calculator, %{}}, ["metrics_received"]})
    EventBus.subscribe({{MemoryLeakerOne, %{}}, [".*"]})

    Logger.put_module_level(InputLogger, :info)

    logs =
      capture_log(fn ->
        Notification.notify(@event)

        # Wait for follow-up async work (nested notify / GenServer.cast) to finish.
        Process.sleep(300)
      end)

    Logger.delete_module_level(InputLogger)

    assert String.contains?(logs, "BadOne.process/1 raised an error!")

    assert String.contains?(logs, "Event log for %EventBus.Model.Event{")
    assert String.contains?(logs, "id: \"M1\"")
    assert String.contains?(logs, "data: [1, 7]")
    assert String.contains?(logs, "topic: :metrics_received")

    assert String.contains?(logs, "id: \"E123\"")
    assert String.contains?(logs, "data: {8, [1, 7]}")
    assert String.contains?(logs, "topic: :metrics_summed")
  end
end
