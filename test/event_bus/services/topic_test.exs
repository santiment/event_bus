defmodule EventBus.Service.TopicTest do
  use ExUnit.Case, async: false

  alias EventBus.Service.{Observation, Store, Topic}

  doctest Topic

  @config_topics Application.compile_env(:event_bus, :topics, [])

  setup do
    # Re-register config topics in case a prior test removed them
    Enum.each(@config_topics, &Topic.register/1)

    on_exit(fn ->
      topics = Topic.all() -- @config_topics
      Enum.each(topics, fn topic -> Topic.unregister(topic) end)
    end)

    :ok
  end

  test "exist?" do
    topic = :metrics_received_1
    Topic.register(topic)

    assert Topic.exist?(topic)
  end

  test "register_topic" do
    topic = :t1
    Topic.register(topic)

    assert Enum.any?(Topic.all(), fn t -> t == topic end)

    # Consolidated tables should exist
    assert :ets.info(Store.table_name()) != :undefined
    assert :ets.info(Observation.table_name()) != :undefined
  end

  test "register_topic does not re-register same topic" do
    topic = :t2
    Topic.register(topic)
    topic_count = length(Topic.all())
    Topic.register(topic)

    assert topic_count == length(Topic.all())
  end

  test "unregister_topic" do
    topic = :t3
    Topic.register(topic)
    Topic.unregister(topic)

    refute Enum.any?(Topic.all(), fn t -> t == topic end)
  end

  test "all" do
    topic = :t3
    Topic.register(topic)
    all = Topic.all()
    assert :t3 in all
    assert :metrics_received in all
  end

  test "exist? with an existent topic" do
    assert Topic.exist?(:metrics_received)
  end

  test "exist? with a non-existent topic" do
    refute Topic.exist?(:unknown_called)
  end
end
