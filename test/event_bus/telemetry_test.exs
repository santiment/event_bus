defmodule EventBus.TelemetryTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Notification

  @topic :telemetry_test_topic

  setup do
    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    test_pid = self()

    on_exit(fn ->
      :telemetry.detach("telemetry-test-start")
      :telemetry.detach("telemetry-test-stop")
      :telemetry.detach("telemetry-test-exception")

      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    {:ok, test_pid: test_pid}
  end

  defmodule GoodSubscriber do
    def process({topic, id}) do
      EventBus.mark_as_completed({__MODULE__, topic, id})
    end
  end

  defmodule FailingSubscriber do
    def process(_event_shadow) do
      raise "telemetry test failure"
    end
  end

  describe "telemetry events on successful notify" do
    test "emits :start and :stop events", %{test_pid: test_pid} do
      capture_log(fn ->
        :telemetry.attach(
          "telemetry-test-start",
          [:event_bus, :notify, :start],
          fn event_name, measurements, metadata, _config ->
            send(test_pid, {:telemetry, event_name, measurements, metadata})
          end,
          nil
        )

        :telemetry.attach(
          "telemetry-test-stop",
          [:event_bus, :notify, :stop],
          fn event_name, measurements, metadata, _config ->
            send(test_pid, {:telemetry, event_name, measurements, metadata})
          end,
          nil
        )
      end)

      EventBus.subscribe({GoodSubscriber, ["telemetry_test_topic"]})

      event = %Event{
        id: "telemetry-start-stop",
        topic: @topic,
        data: %{test: true}
      }

      Notification.notify(event)

      assert_receive {:telemetry, [:event_bus, :notify, :start],
                      start_measurements, start_metadata}

      assert is_integer(start_measurements.system_time)
      assert start_metadata.topic == @topic
      assert start_metadata.event_id == "telemetry-start-stop"

      assert_receive {:telemetry, [:event_bus, :notify, :stop],
                      stop_measurements, stop_metadata}

      assert is_integer(stop_measurements.duration)
      assert stop_measurements.duration >= 0
      assert stop_metadata.topic == @topic
      assert stop_metadata.event_id == "telemetry-start-stop"
      assert stop_metadata.subscriber_count == 1
    end
  end

  describe "telemetry events on subscriber exception" do
    test "emits :exception event with duration and error details", %{
      test_pid: test_pid
    } do
      capture_log(fn ->
        :telemetry.attach(
          "telemetry-test-exception",
          [:event_bus, :notify, :exception],
          fn event_name, measurements, metadata, _config ->
            send(test_pid, {:telemetry, event_name, measurements, metadata})
          end,
          nil
        )
      end)

      EventBus.subscribe({FailingSubscriber, ["telemetry_test_topic"]})

      event = %Event{
        id: "telemetry-exception",
        topic: @topic,
        data: %{test: true}
      }

      capture_log(fn ->
        Notification.notify(event)
      end)

      assert_receive {:telemetry, [:event_bus, :notify, :exception],
                      measurements, metadata}

      assert is_integer(measurements.duration)
      assert measurements.duration >= 0
      assert metadata.topic == @topic
      assert metadata.event_id == "telemetry-exception"
      assert metadata.subscriber == FailingSubscriber
      assert metadata.kind == :error
      assert %RuntimeError{message: "telemetry test failure"} = metadata.reason
      assert is_list(metadata.stacktrace)
    end
  end

  describe "no telemetry events when no subscribers" do
    test "does not emit :start/:stop when topic has no subscribers", %{
      test_pid: test_pid
    } do
      capture_log(fn ->
        :telemetry.attach(
          "telemetry-test-start",
          [:event_bus, :notify, :start],
          fn event_name, measurements, metadata, _config ->
            send(test_pid, {:telemetry, event_name, measurements, metadata})
          end,
          nil
        )
      end)

      event = %Event{
        id: "telemetry-no-sub",
        topic: @topic,
        data: %{test: true}
      }

      capture_log(fn ->
        Notification.notify(event)
      end)

      refute_receive {:telemetry, [:event_bus, :notify, :start], _, _}, 100
    end
  end
end
