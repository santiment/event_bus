defmodule EventBus.Service.StoreTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Store

  doctest Store

  setup do
    :ok
  end

  test "consolidated table exists" do
    assert :ets.info(Store.table_name()) != :undefined
  end

  test "register_topic is a no-op" do
    assert :ok == Store.register_topic(:store_test_topic)
  end

  test "unregister_topic deletes entries for the topic" do
    topic = :store_unregister_test

    event = %Event{
      id: "E1",
      transaction_id: "T1",
      data: "test",
      topic: topic
    }

    Store.create(event)
    assert event == Store.fetch({topic, "E1"})

    Store.unregister_topic(topic)

    capture_log(fn ->
      assert is_nil(Store.fetch({topic, "E1"}))
    end)
  end

  test "create" do
    topic = :metrics_received_2

    event = %Event{
      id: "E1",
      transaction_id: "T1",
      data: ["Mustafa", "Turan"],
      topic: topic
    }

    assert :ok == Store.create(event)
    Store.delete({topic, "E1"})
  end

  test "fetch" do
    topic = :metrics_received_3

    first_event = %Event{
      id: "E1",
      transaction_id: "T1",
      data: ["Mustafa", "Turan"],
      topic: topic
    }

    second_event = %Event{
      id: "E2",
      transaction_id: "T1",
      data: %{name: "Mustafa", surname: "Turan"},
      topic: topic
    }

    :ok = Store.create(first_event)
    :ok = Store.create(second_event)

    assert first_event == Store.fetch({topic, first_event.id})
    assert second_event == Store.fetch({topic, second_event.id})

    Store.delete({topic, "E1"})
    Store.delete({topic, "E2"})
  end

  test "fetch_data" do
    topic = :metrics_received_4

    event = %Event{
      id: "E1",
      transaction_id: "T1",
      data: ["Mustafa", "Turan"],
      topic: topic
    }

    :ok = Store.create(event)

    assert event.data == Store.fetch_data({topic, event.id})
    Store.delete({topic, "E1"})
  end

  test "create stores bus-owned metadata with inserted_at" do
    topic = :metadata_test

    event = %Event{
      id: "META1",
      transaction_id: "T1",
      data: "test",
      topic: topic
    }

    before = System.monotonic_time()
    Store.create(event)
    after_time = System.monotonic_time()

    metadata = Store.fetch_metadata({topic, "META1"})
    assert metadata != nil
    assert is_integer(metadata.inserted_at)
    assert metadata.inserted_at >= before
    assert metadata.inserted_at <= after_time

    Store.delete({topic, "META1"})
  end

  test "fetch_metadata returns nil for non-existent event" do
    capture_log(fn ->
      assert is_nil(Store.fetch_metadata({:no_topic, "no_id"}))
    end)
  end

  test "delete and fetch" do
    topic = :metrics_received_5

    event = %Event{
      id: "E1",
      transaction_id: "T1",
      data: ["Mustafa", "Turan"],
      topic: topic
    }

    :ok = Store.create(event)
    Store.delete({topic, event.id})

    capture_log(fn ->
      assert is_nil(Store.fetch({topic, event.id}))
    end)
  end
end
