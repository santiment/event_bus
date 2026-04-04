defmodule EventBus.Service.DebugTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Debug

  @topic :debug_test_topic

  setup do
    # Ensure debug is off by default
    Debug.toggle(false)

    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    on_exit(fn ->
      Debug.toggle(false)
      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  defmodule CompletingSubscriber do
    def process({topic, id}) do
      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defmodule ConfigSubscriber do
    def process({config, topic, id}) do
      EventBus.mark_as_completed({{__MODULE__, config}, {topic, id}})
    end
  end

  test "enabled?/0 returns false by default" do
    refute Debug.enabled?()
  end

  test "toggle/1 enables and disables debug mode" do
    Debug.toggle(true)
    assert Debug.enabled?()

    Debug.toggle(false)
    refute Debug.enabled?()
  end

  test "EventBus.toggle_debug/1 delegates correctly" do
    EventBus.toggle_debug(true)
    assert Debug.enabled?()

    EventBus.toggle_debug(false)
    refute Debug.enabled?()
  end

  test "no debug logs when debug is disabled" do
    EventBus.subscribe({CompletingSubscriber, ["debug_test_topic"]})

    event = %Event{
      id: "debug-off-1",
      topic: @topic,
      data: %{test: true}
    }

    logs =
      capture_log([level: :debug], fn ->
        EventBus.notify(event)
        Process.sleep(200)
      end)

    refute logs =~ "[EventBus]"
  end

  test "logs full event lifecycle when debug is enabled" do
    Debug.toggle(true)
    EventBus.subscribe({CompletingSubscriber, ["debug_test_topic"]})

    event = %Event{
      id: "debug-on-1",
      topic: @topic,
      data: %{test: true}
    }

    logs =
      capture_log([level: :debug], fn ->
        EventBus.notify(event)
        Process.sleep(200)
      end)

    # Notify log
    assert logs =~ "[EventBus] notify topic=:debug_test_topic id=\"debug-on-1\""

    # Dispatch log
    assert logs =~ "[EventBus] dispatch topic=:debug_test_topic id=\"debug-on-1\""
    assert logs =~ "CompletingSubscriber"

    # Completed log with duration
    assert logs =~ "[EventBus] completed topic=:debug_test_topic id=\"debug-on-1\""
    assert logs =~ "duration="

    # Cleaned log
    assert logs =~ "[EventBus] cleaned topic=:debug_test_topic id=\"debug-on-1\""
  end

  test "logs subscribe and unsubscribe" do
    Debug.toggle(true)

    logs =
      capture_log([level: :debug], fn ->
        EventBus.subscribe({CompletingSubscriber, ["debug_test_topic"]})
      end)

    assert logs =~ "[EventBus] subscribe subscriber="
    assert logs =~ "CompletingSubscriber"
    assert logs =~ "patterns=[\"debug_test_topic\"]"

    logs =
      capture_log([level: :debug], fn ->
        EventBus.unsubscribe(CompletingSubscriber)
      end)

    assert logs =~ "[EventBus] unsubscribe subscriber="
    assert logs =~ "CompletingSubscriber"
  end

  test "logs topic registration" do
    Debug.toggle(true)
    topic = :debug_register_test

    logs =
      capture_log([level: :debug], fn ->
        EventBus.register_topic(topic)
      end)

    assert logs =~ "[EventBus] register_topic topic=:debug_register_test"

    logs =
      capture_log([level: :debug], fn ->
        EventBus.unregister_topic(topic)
      end)

    assert logs =~ "[EventBus] unregister_topic topic=:debug_register_test"
  end

  test "logs skipped for crashing subscriber" do
    Debug.toggle(true)

    defmodule CrashingSubscriber do
      def process(_), do: raise("crash")
    end

    EventBus.subscribe({CrashingSubscriber, ["debug_test_topic"]})

    event = %Event{
      id: "debug-crash-1",
      topic: @topic,
      data: %{test: true}
    }

    logs =
      capture_log([level: :debug], fn ->
        EventBus.notify(event)
        Process.sleep(200)
      end)

    assert logs =~ "[EventBus] skipped topic=:debug_test_topic id=\"debug-crash-1\""
    assert logs =~ "CrashingSubscriber"
  end

  test "dispatch metadata is cleaned up after event completion" do
    Debug.toggle(true)
    EventBus.subscribe({CompletingSubscriber, ["debug_test_topic"]})

    event = %Event{id: "debug-cleanup-1", topic: @topic, data: %{}}
    EventBus.notify(event)
    Process.sleep(200)

    # After completion and cleanup, dispatch metadata should be gone
    assert :not_found ==
             Debug.fetch_and_clear_dispatch_time(
               CompletingSubscriber,
               @topic,
               "debug-cleanup-1"
             )
  end

  test "works with configured subscribers" do
    Debug.toggle(true)
    EventBus.subscribe({{ConfigSubscriber, %{key: "val"}}, ["debug_test_topic"]})

    event = %Event{
      id: "debug-config-1",
      topic: @topic,
      data: %{test: true}
    }

    logs =
      capture_log([level: :debug], fn ->
        EventBus.notify(event)
        Process.sleep(200)
      end)

    assert logs =~ "[EventBus] dispatch"
    assert logs =~ "ConfigSubscriber"
    assert logs =~ "[EventBus] completed"
  end
end
