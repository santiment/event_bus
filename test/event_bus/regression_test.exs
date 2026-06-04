defmodule EventBus.RegressionTest do
  @moduledoc """
  Regression tests for known issues and behaviors that upcoming
  modernization work must not break.
  """
  use ExUnit.Case, async: false
  use EventBus.EventSource

  @topic :regression_test_topic

  setup do
    EventBus.register_topic(@topic)

    on_exit(fn ->
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

  # -- Topic.register_from_config/0 --

  describe "register_from_config/0" do
    test "registers configured topics" do
      EventBus.Service.Topic.register_from_config()
      configured_topics = Application.get_env(:event_bus, :topics, [])

      assert length(configured_topics) > 0, "Config should have topics"

      # All configured topics should be registered
      for topic <- configured_topics do
        assert EventBus.topic_exist?(topic),
               "Topic #{topic} should be registered"
      end
    end
  end
end
