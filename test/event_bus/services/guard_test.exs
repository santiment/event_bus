defmodule EventBus.Service.GuardTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event

  @topic :guard_test_topic

  setup do
    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    on_exit(fn ->
      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  defmodule PassingSubscriber do
    def process({topic, id}) do
      send(:guard_test, {:processed, __MODULE__, topic, id})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule AnotherSubscriber do
    def process({topic, id}) do
      send(:guard_test, {:processed, __MODULE__, topic, id})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defp notify_and_wait(id, data \\ %{}) do
    event = %Event{id: id, topic: @topic, data: data}
    EventBus.notify(event)
    Process.sleep(200)
  end

  test "guard that returns true allows dispatch" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn _event -> true end
    )

    notify_and_wait("guard-pass-1")
    assert_received {:processed, PassingSubscriber, @topic, "guard-pass-1"}
  end

  test "guard that returns false skips dispatch" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn _event -> false end
    )

    notify_and_wait("guard-skip-1")
    refute_received {:processed, PassingSubscriber, @topic, "guard-skip-1"}

    # Event should be cleaned up (subscriber was skipped)
    capture_log(fn ->
      assert EventBus.fetch_event({@topic, "guard-skip-1"}) == nil
    end)
  end

  test "guard receives the full event struct" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn event ->
        send(:guard_test, {:guard_called, event.data})
        true
      end
    )

    notify_and_wait("guard-struct-1", %{amount: 500})
    assert_received {:guard_called, %{amount: 500}}
    assert_received {:processed, PassingSubscriber, @topic, "guard-struct-1"}
  end

  test "guard filters by event data content" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn event -> event.data[:amount] > 100 end
    )

    notify_and_wait("guard-filter-low", %{amount: 50})
    refute_received {:processed, PassingSubscriber, @topic, "guard-filter-low"}

    notify_and_wait("guard-filter-high", %{amount: 200})
    assert_received {:processed, PassingSubscriber, @topic, "guard-filter-high"}
  end

  test "guard that raises marks subscriber as skipped" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn _event -> raise "guard error" end
    )

    capture_log(fn ->
      notify_and_wait("guard-raise-1")

      refute_received {:processed, PassingSubscriber, @topic, "guard-raise-1"}

      # Event should be cleaned up
      assert EventBus.fetch_event({@topic, "guard-raise-1"}) == nil
    end)
  end

  test "guard that raises does not stop later subscribers" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      priority: 100,
      guard: fn _event -> raise "guard error" end
    )

    EventBus.subscribe({AnotherSubscriber, ["guard_test_topic"]}, priority: 0)

    capture_log(fn ->
      notify_and_wait("guard-raise-continue-1")
    end)

    refute_received {:processed, PassingSubscriber, @topic, "guard-raise-continue-1"}
    assert_received {:processed, AnotherSubscriber, @topic, "guard-raise-continue-1"}
  end

  test "subscriber without guard is unaffected" do
    Process.register(self(), :guard_test)

    EventBus.subscribe({PassingSubscriber, ["guard_test_topic"]})

    notify_and_wait("guard-none-1")
    assert_received {:processed, PassingSubscriber, @topic, "guard-none-1"}
  end

  test "re-subscribing with plain subscribe/1 clears guard" do
    Process.register(self(), :guard_test)

    EventBus.subscribe(
      {PassingSubscriber, ["guard_test_topic"]},
      guard: fn _event -> false end
    )

    # Re-subscribe without guard
    EventBus.subscribe({PassingSubscriber, ["guard_test_topic"]})

    notify_and_wait("guard-clear-1")
    assert_received {:processed, PassingSubscriber, @topic, "guard-clear-1"}
  end
end
