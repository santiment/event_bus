defmodule EventBus.Service.SubscribeOnceTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event

  @topic :subscribe_once_topic

  setup do
    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    on_exit(fn ->
      Application.delete_env(:event_bus, :subscribe_once_test_pid)
      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  defmodule OnceSubscriber do
    def process({topic, id}) do
      send(:subscribe_once_test, {:processed, __MODULE__, topic, id})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule CountingSubscriber do
    def process({topic, id}) do
      send(:subscribe_once_test, {:processed, __MODULE__, topic, id})
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule ConfigOnceSubscriber do
    def process({config, topic, id}) do
      send(:subscribe_once_test, {:processed, {__MODULE__, config}, topic, id})
      EventBus.mark_as_completed({{__MODULE__, config}, {topic, id}})
    end
  end

  defmodule DelayedCompletionSubscriber do
    def process({topic, id}) do
      test_pid = Application.fetch_env!(:event_bus, :subscribe_once_test_pid)

      waiter =
        spawn(fn ->
          send(test_pid, {:completion_waiter, id, self()})

          receive do
            :complete ->
              EventBus.mark_as_completed({__MODULE__, {topic, id}})
          end
        end)

      send(test_pid, {:spawned_waiter, id, waiter})
      :ok
    end
  end

  defp notify_and_wait(id) do
    event = %Event{id: id, topic: @topic, data: %{}}
    EventBus.notify(event)
    Process.sleep(100)
  end

  test "subscribe_once auto-unsubscribes after one event" do
    Process.register(self(), :subscribe_once_test)

    EventBus.subscribe_once({OnceSubscriber, ["subscribe_once_topic"]})
    assert [{{OnceSubscriber, nil}, _}] = EventBus.subscribers()

    notify_and_wait("once-1")
    assert_received {:processed, OnceSubscriber, @topic, "once-1"}

    # Subscriber should be auto-unsubscribed now
    Process.sleep(100)
    assert [] == EventBus.subscribers()

    # Second event should not be received
    capture_log(fn ->
      notify_and_wait("once-2")
    end)

    refute_received {:processed, OnceSubscriber, @topic, "once-2"}
  end

  test "subscribe_n with count 3 unsubscribes after 3 events" do
    Process.register(self(), :subscribe_once_test)

    EventBus.subscribe_n({CountingSubscriber, ["subscribe_once_topic"]}, 3)

    notify_and_wait("n-1")
    assert_received {:processed, CountingSubscriber, @topic, "n-1"}

    notify_and_wait("n-2")
    assert_received {:processed, CountingSubscriber, @topic, "n-2"}

    notify_and_wait("n-3")
    assert_received {:processed, CountingSubscriber, @topic, "n-3"}

    # Should be unsubscribed after 3 events
    Process.sleep(100)
    assert [] == EventBus.subscribers()

    capture_log(fn ->
      notify_and_wait("n-4")
    end)

    refute_received {:processed, CountingSubscriber, @topic, "n-4"}
  end

  test "manual unsubscribe clears limit entry" do
    EventBus.subscribe_n({OnceSubscriber, ["subscribe_once_topic"]}, 5)
    EventBus.unsubscribe(OnceSubscriber)
    assert [] == EventBus.subscribers()
  end

  test "re-subscribing with plain subscribe clears limit" do
    Process.register(self(), :subscribe_once_test)

    EventBus.subscribe_once({OnceSubscriber, ["subscribe_once_topic"]})

    # Re-subscribe with plain subscribe - should clear the limit
    EventBus.subscribe({OnceSubscriber, ["subscribe_once_topic"]})

    notify_and_wait("re-1")
    assert_received {:processed, OnceSubscriber, @topic, "re-1"}

    # Should still be subscribed (limit was cleared)
    Process.sleep(100)
    assert [{{OnceSubscriber, nil}, _}] = EventBus.subscribers()

    notify_and_wait("re-2")
    assert_received {:processed, OnceSubscriber, @topic, "re-2"}
  end

  test "re-subscribing with subscribe_n replaces limit" do
    Process.register(self(), :subscribe_once_test)

    EventBus.subscribe_n({CountingSubscriber, ["subscribe_once_topic"]}, 1)

    # Replace with a higher limit
    EventBus.subscribe_n({CountingSubscriber, ["subscribe_once_topic"]}, 3)

    notify_and_wait("replace-1")
    assert_received {:processed, CountingSubscriber, @topic, "replace-1"}

    # Should still be subscribed (limit is now 3)
    Process.sleep(100)
    assert [{{CountingSubscriber, nil}, _}] = EventBus.subscribers()
  end

  test "subscribe_once works with configured subscribers" do
    Process.register(self(), :subscribe_once_test)
    config = %{key: "val"}

    EventBus.subscribe_once(
      {{ConfigOnceSubscriber, config}, ["subscribe_once_topic"]}
    )

    notify_and_wait("config-1")

    assert_received {:processed, {ConfigOnceSubscriber, ^config}, @topic,
                     "config-1"}

    Process.sleep(100)
    assert [] == EventBus.subscribers()
  end

  test "crashing subscriber still consumes one count" do
    defmodule CrashOnceSubscriber do
      def process({topic, id}) do
        send(:subscribe_once_test, {:crash_attempt, topic, id})
        raise "crash"
      end
    end

    Process.register(self(), :subscribe_once_test)

    EventBus.subscribe_once({CrashOnceSubscriber, ["subscribe_once_topic"]})

    capture_log(fn ->
      notify_and_wait("crash-1")
    end)

    assert_received {:crash_attempt, @topic, "crash-1"}

    # Should be unsubscribed because the crash caused a skip which decremented
    Process.sleep(100)
    assert [] == EventBus.subscribers()
  end

  test "completion from an earlier event does not consume a fresh subscription limit" do
    # This reproduces the stale-completion race: the first subscription receives
    # an event, then we re-subscribe before that old event reaches its terminal
    # state. The old completion must not spend the new subscription's limit.
    Application.put_env(:event_bus, :subscribe_once_test_pid, self())

    EventBus.subscribe_once(
      {DelayedCompletionSubscriber, ["subscribe_once_topic"]}
    )

    old_event = %Event{id: "old-event", topic: @topic, data: %{}}
    EventBus.Service.Notification.notify(old_event)

    waiter =
      receive do
        {:completion_waiter, "old-event", pid} ->
          assert_receive {:spawned_waiter, "old-event", ^pid}
          pid

        {:spawned_waiter, "old-event", pid} ->
          assert_receive {:completion_waiter, "old-event", ^pid}
          pid
      end

    EventBus.subscribe_once(
      {DelayedCompletionSubscriber, ["subscribe_once_topic"]}
    )

    send(waiter, :complete)
    Process.sleep(100)

    assert [{{DelayedCompletionSubscriber, nil}, _patterns}] =
             EventBus.subscribers()
  end

  test "subscribe_once does not overdeliver while a prior event is still in flight" do
    Application.put_env(:event_bus, :subscribe_once_test_pid, self())

    EventBus.subscribe_once(
      {DelayedCompletionSubscriber, ["subscribe_once_topic"]}
    )

    first_event = %Event{id: "in-flight-1", topic: @topic, data: %{}}
    second_event = %Event{id: "in-flight-2", topic: @topic, data: %{}}

    EventBus.notify(first_event)

    waiter =
      receive do
        {:completion_waiter, "in-flight-1", pid} ->
          assert_receive {:spawned_waiter, "in-flight-1", ^pid}
          pid

        {:spawned_waiter, "in-flight-1", pid} ->
          assert_receive {:completion_waiter, "in-flight-1", ^pid}
          pid
      end

    EventBus.notify(second_event)

    refute_receive {:completion_waiter, "in-flight-2", _pid}, 150
    refute_receive {:spawned_waiter, "in-flight-2", _pid}, 150

    send(waiter, :complete)
    Process.sleep(100)

    assert [] == EventBus.subscribers()
  end
end
