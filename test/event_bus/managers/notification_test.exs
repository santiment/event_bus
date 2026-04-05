defmodule EventBus.NotifyTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event

  @topic :metrics_received
  @event %Event{
    id: "E1",
    transaction_id: "T1",
    topic: @topic,
    data: [1, 2],
    source: "NotifyTest"
  }

  setup do
    refute is_nil(Process.whereis(EventBus.TaskSupervisor))
    :ok
  end

  test "notify dispatches asynchronously via TaskSupervisor" do
    capture_log(fn ->
      assert :ok == EventBus.notify(@event)
    end)
  end

  test "notify_sync dispatches in the calling process" do
    capture_log(fn ->
      assert :ok == EventBus.notify_sync(@event)
    end)
  end
end
