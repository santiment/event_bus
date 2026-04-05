defmodule EventBus.Manager.StoreTest do
  use ExUnit.Case, async: false

  alias EventBus.Manager.Store
  alias EventBus.Model.Event

  doctest Store

  @topic :metrics_stored

  setup do
    refute is_nil(Process.whereis(Store))
    :ok
  end

  test "register_topic" do
    assert :ok == Store.register_topic(@topic)
  end

  test "unregister_topic" do
    assert :ok == Store.unregister_topic(@topic)
  end

  test "create" do
    event = %Event{id: "E1", transaction_id: "T1", data: %{}, topic: @topic}
    assert :ok == Store.create(event)
    Store.delete({@topic, "E1"})
  end

  test "delete" do
    event = %Event{id: "E1", transaction_id: "T1", data: [1, 2], topic: @topic}
    Store.create(event)

    assert :ok == Store.delete({event.topic, event.id})
  end
end
