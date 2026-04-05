defmodule EventBus.Service.TopicEtsTest do
  use ExUnit.Case, async: false
  alias EventBus.Service.Topic

  setup do
    on_exit(fn ->
      topics = [:t1, :t2]
      Enum.each(topics, fn topic -> Topic.unregister(topic) end)
    end)

    :ok
  end

  test "exist? after register" do
    topic = :metrics_received_1
    Topic.register(topic)

    assert Topic.exist?(topic)
  end

  test "register is idempotent" do
    Topic.register(:t1)
    count = length(Topic.all())
    Topic.register(:t1)

    assert count == length(Topic.all())
  end

  test "unregister removes topic" do
    topic = :t2
    Topic.register(topic)
    Topic.unregister(topic)

    refute Topic.exist?(topic)
  end
end
