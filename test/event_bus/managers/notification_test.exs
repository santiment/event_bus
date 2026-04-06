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

  defmodule BlockingSubscriber do
    def process({topic, id}) do
      test_pid = Application.fetch_env!(:event_bus, :notify_test_pid)
      send(test_pid, {:entered, self(), topic, id})

      receive do
        :release -> :ok
      end

      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  setup do
    refute is_nil(Process.whereis(EventBus.TaskSupervisor))
    EventBus.register_topic(@topic)
    Application.put_env(:event_bus, :notify_test_pid, self())

    on_exit(fn ->
      Application.delete_env(:event_bus, :notify_test_pid)
      EventBus.unsubscribe(BlockingSubscriber)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  test "notify dispatches asynchronously via TaskSupervisor" do
    capture_log(fn ->
      assert :ok == EventBus.notify(@event)
    end)
  end

  test "notify returns before a blocking subscriber completes" do
    EventBus.subscribe({BlockingSubscriber, ["metrics_received"]})

    capture_log(fn ->
      assert :ok == EventBus.notify(@event)
      assert_receive {:entered, worker, @topic, "E1"}
      assert %Event{} = EventBus.fetch_event({@topic, "E1"})

      send(worker, :release)
      Process.sleep(100)
    end)

    assert EventBus.fetch_event({@topic, "E1"}) == nil
  end

  test "notify_sync dispatches in the calling process" do
    capture_log(fn ->
      assert :ok == EventBus.notify_sync(@event)
    end)
  end

  test "notify_sync blocks until the subscriber completes" do
    EventBus.subscribe({BlockingSubscriber, ["metrics_received"]})

    task =
      Task.async(fn ->
        capture_log(fn ->
          EventBus.notify_sync(@event)
        end)
      end)

    assert_receive {:entered, worker, @topic, "E1"}
    assert Task.yield(task, 50) == nil

    send(worker, :release)

    assert {:ok, _logs} = Task.yield(task, 1_000) || Task.shutdown(task)
    assert EventBus.fetch_event({@topic, "E1"}) == nil
  end
end
