defmodule EventBus.RegressionTest do
  @moduledoc """
  Regression tests for known issues and behaviors that upcoming
  modernization work must not break.
  """
  use ExUnit.Case, async: false
  use EventBus.EventSource

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Notification

  @topic :regression_test_topic

  setup do
    # Clean up any leftover subscribers from other tests
    for {subscriber, _} <- EventBus.subscribers() do
      EventBus.unsubscribe(subscriber)
    end

    EventBus.register_topic(@topic)

    on_exit(fn ->
      # Allow async notification operations to drain before unregistering
      Process.sleep(100)
      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  # -- Issue #176: EventSource default id_generator --

  describe "EventSource default id_generator (issue #176)" do
    test "build generates an id without explicit id_generator config" do
      # Temporarily remove the id_generator config to test the default
      original = Application.get_env(:event_bus, :id_generator)
      Application.delete_env(:event_bus, :id_generator)

      try do
        event =
          EventBus.EventSource.build %{topic: @topic} do
            :some_data
          end

        assert is_binary(event.id)
        assert String.length(event.id) > 0
      after
        if original do
          Application.put_env(:event_bus, :id_generator, original)
        end
      end
    end
  end

  # -- Issue #118: Missing subscriber warning messages --

  describe "missing subscriber warning messages (issue #118)" do
    test "warns for registered topic with no subscribers" do
      event = %Event{
        id: "regression-118-a",
        topic: @topic,
        data: %{test: true}
      }

      logs =
        capture_log(fn ->
          Notification.notify(event)
          Process.sleep(50)
        end)

      assert logs =~ "#{@topic}"
      assert logs =~ "doesn't have subscribers"
    end

    test "warns differently for unregistered topic" do
      event = %Event{
        id: "regression-118-b",
        topic: :completely_unknown_topic,
        data: %{test: true}
      }

      logs =
        capture_log(fn ->
          Notification.notify(event)
          Process.sleep(50)
        end)

      assert logs =~ "completely_unknown_topic"
      assert logs =~ "is not registered and has no subscribers"
    end
  end

  # -- Issue #169: Error handling in notification delivery --

  describe "subscriber error handling (issue #169)" do
    defmodule FailingSubscriber do
      def process(_event_shadow) do
        raise "intentional test failure"
      end
    end

    test "logs errors when subscriber raises" do
      EventBus.subscribe({FailingSubscriber, ["regression_test_topic"]})

      event = %Event{
        id: "regression-169",
        topic: @topic,
        data: %{test: true}
      }

      logs =
        capture_log(fn ->
          Notification.notify(event)
          Process.sleep(200)
        end)

      assert logs =~ "FailingSubscriber.process/1 raised an error!"
      assert logs =~ "intentional test failure"
    end

    test "marks subscriber as skipped when it raises" do
      EventBus.subscribe({FailingSubscriber, ["regression_test_topic"]})

      event = %Event{
        id: "regression-169-skip",
        topic: @topic,
        data: %{test: true}
      }

      capture_log(fn ->
        Notification.notify(event)
        Process.sleep(200)

        # Event should be cleaned up (all subscribers processed = skipped)
        assert EventBus.fetch_event({@topic, "regression-169-skip"}) == nil
      end)
    end
  end

  # -- Topic.register_from_config/0 --

  describe "register_from_config/0" do
    test "registers configured topics and consolidated ETS tables exist" do
      EventBus.Manager.Topic.register_from_config()
      configured_topics = Application.get_env(:event_bus, :topics, [])

      assert length(configured_topics) > 0, "Config should have topics"

      # Consolidated tables should exist
      assert :ets.info(EventBus.Service.Store.table_name()) != :undefined,
             "Consolidated store ETS table should exist"

      assert :ets.info(EventBus.Service.Observation.table_name()) != :undefined,
             "Consolidated observation ETS table should exist"

      # All configured topics should be registered
      for topic <- configured_topics do
        assert EventBus.topic_exist?(topic),
               "Topic #{topic} should be registered"
      end
    end
  end
end
