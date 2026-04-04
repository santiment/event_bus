defmodule EventBus.Service.PriorityTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.CancelEvent
  alias EventBus.Model.Event

  @topic :priority_test_topic

  setup do
    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    on_exit(fn ->
      for {subscriber, _} <- EventBus.subscribers() do
        EventBus.unsubscribe(subscriber)
      end

      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  defmodule HighPriority do
    def process({topic, id}) do
      send(:priority_test, {:processed, :high, System.monotonic_time()})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule MedPriority do
    def process({topic, id}) do
      send(:priority_test, {:processed, :med, System.monotonic_time()})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule LowPriority do
    def process({topic, id}) do
      send(:priority_test, {:processed, :low, System.monotonic_time()})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule CancellingSubscriber do
    def process({topic, id}) do
      send(:priority_test, {:processed, :canceller})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
      {:cancel, "validation failed"}
    end
  end

  defmodule CancelRaisingSubscriber do
    def process({_topic, _id}) do
      send(:priority_test, {:processed, :raise_canceller})
      raise CancelEvent, reason: "auth failed"
    end
  end

  defmodule AfterCancelSubscriber do
    def process({topic, id}) do
      send(:priority_test, {:processed, :after_cancel})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defp notify_and_wait(id) do
    event = %Event{id: id, topic: @topic, data: %{}}
    EventBus.notify(event)
    Process.sleep(200)
  end

  test "subscribers are dispatched in priority order" do
    Process.register(self(), :priority_test)

    # Subscribe in reverse priority order
    EventBus.subscribe({LowPriority, ["priority_test_topic"]}, priority: -10)
    EventBus.subscribe({HighPriority, ["priority_test_topic"]}, priority: 100)
    EventBus.subscribe({MedPriority, ["priority_test_topic"]}, priority: 0)

    notify_and_wait("prio-1")

    assert_received {:processed, :high, t1}
    assert_received {:processed, :med, t2}
    assert_received {:processed, :low, t3}

    assert t1 <= t2
    assert t2 <= t3
  end

  test "default priority is 0" do
    Process.register(self(), :priority_test)

    EventBus.subscribe({LowPriority, ["priority_test_topic"]}, priority: -10)
    # No priority specified - defaults to 0
    EventBus.subscribe({MedPriority, ["priority_test_topic"]})
    EventBus.subscribe({HighPriority, ["priority_test_topic"]}, priority: 10)

    notify_and_wait("prio-default-1")

    assert_received {:processed, :high, t1}
    assert_received {:processed, :med, t2}
    assert_received {:processed, :low, t3}

    assert t1 <= t2
    assert t2 <= t3
  end

  test "cancellation via return value stops propagation" do
    Process.register(self(), :priority_test)

    EventBus.subscribe({CancellingSubscriber, ["priority_test_topic"]}, priority: 100)
    EventBus.subscribe({AfterCancelSubscriber, ["priority_test_topic"]}, priority: 0)

    notify_and_wait("cancel-return-1")

    assert_received {:processed, :canceller}
    refute_received {:processed, :after_cancel}

    # Event should be cleaned up (canceller completed, after_cancel skipped)
    Process.sleep(100)
    assert EventBus.fetch_event({@topic, "cancel-return-1"}) == nil
  end

  test "cancellation via CancelEvent exception stops propagation" do
    Process.register(self(), :priority_test)

    EventBus.subscribe({CancelRaisingSubscriber, ["priority_test_topic"]}, priority: 100)
    EventBus.subscribe({AfterCancelSubscriber, ["priority_test_topic"]}, priority: 0)

    capture_log(fn ->
      notify_and_wait("cancel-raise-1")
    end)

    assert_received {:processed, :raise_canceller}
    refute_received {:processed, :after_cancel}

    # Event should be cleaned up (raiser skipped by cancellation, after_cancel skipped)
    Process.sleep(100)
    assert EventBus.fetch_event({@topic, "cancel-raise-1"}) == nil
  end

  test "non-cancelling error does not stop propagation" do
    Process.register(self(), :priority_test)

    defmodule RegularErrorSubscriber do
      def process({_topic, _id}) do
        send(:priority_test, {:processed, :erroring})
        raise "regular error"
      end
    end

    EventBus.subscribe({RegularErrorSubscriber, ["priority_test_topic"]}, priority: 100)
    EventBus.subscribe({AfterCancelSubscriber, ["priority_test_topic"]}, priority: 0)

    capture_log(fn ->
      notify_and_wait("no-cancel-error-1")
    end)

    assert_received {:processed, :erroring}
    assert_received {:processed, :after_cancel}
  end

  test "priority combined with guard" do
    Process.register(self(), :priority_test)

    # High priority but guarded out
    EventBus.subscribe(
      {HighPriority, ["priority_test_topic"]},
      priority: 100,
      guard: fn _event -> false end
    )

    EventBus.subscribe({LowPriority, ["priority_test_topic"]}, priority: -10)

    notify_and_wait("prio-guard-1")

    refute_received {:processed, :high, _}
    assert_received {:processed, :low, _}
  end
end
