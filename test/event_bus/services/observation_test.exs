defmodule EventBus.Service.ObservationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Service.{Observation, Store, Topic}

  alias EventBus.Support.Helper.{
    BadOne,
    Calculator,
    InputLogger,
    MemoryLeakerOne
  }

  doctest Observation

  setup do
    on_exit(fn ->
      topics = Topic.all() -- [:metrics_received, :metrics_summed]
      Enum.each(topics, fn topic -> Topic.unregister(topic) end)
    end)

    :ok
  end

  test "consolidated table exists" do
    assert :ets.info(Observation.table_name()) != :undefined
  end

  test "register_topic is a no-op" do
    assert :ok == Observation.register_topic(:obs_test_topic)
  end

  test "unregister_topic deletes entries for the topic" do
    topic = :obs_unregister_test
    id = "E1"
    subscribers = [{InputLogger, %{}}]

    Observation.save({topic, id}, {subscribers, [], []})
    assert {subscribers, [], []} == Observation.fetch({topic, id})

    Observation.unregister_topic(topic)

    capture_log(fn ->
      assert nil == Observation.fetch({topic, id})
    end)
  end

  test "unregister_topic clears watcher status and generation snapshot entries" do
    topic = :obs_unregister_status_test
    id = "E1"
    subscriber = {InputLogger, %{}}

    Observation.save({topic, id}, {[subscriber], [], []})
    Observation.save_snapshot({topic, id}, %{subscriber => 3})

    assert [{{topic, id, subscriber}, :pending}] ==
             :ets.lookup(:eb_event_watcher_status, {topic, id, subscriber})

    assert [{{topic, id}, snapshot}] =
             :ets.lookup(:eb_event_subscription_generations, {topic, id})

    assert snapshot[subscriber] == 3

    Observation.unregister_topic(topic)

    assert [] == :ets.lookup(:eb_event_watcher_status, {topic, id, subscriber})
    assert [] == :ets.lookup(:eb_event_subscription_generations, {topic, id})

    capture_log(fn ->
      Observation.mark_as_completed({subscriber, {topic, id}})
      assert nil == Observation.fetch({topic, id})
    end)
  end

  test "create and fetch" do
    topic = :some_event_occurred1
    id = "E1"

    subscribers = [
      {InputLogger, %{}},
      {Calculator, %{}},
      {MemoryLeakerOne, %{}},
      {BadOne, %{}}
    ]

    Observation.save({topic, id}, {subscribers, [], []})

    assert {subscribers, [], []} == Observation.fetch({topic, id})
  end

  test "fetch a non-existent id" do
    topic = :some_event_occurred1
    id = "NA"

    capture_log(fn ->
      assert nil == Observation.fetch({topic, id})
    end)
  end

  test "complete" do
    topic = :some_event_occurred2
    id = "E1"

    subscribers = [
      {InputLogger, %{}},
      {Calculator, %{}},
      {MemoryLeakerOne, %{}},
      {BadOne, %{}}
    ]

    Observation.save({topic, id}, {subscribers, [], []})
    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [{InputLogger, %{}}], []} ==
             Observation.fetch({topic, id})
  end

  test "skip" do
    id = "E1"
    topic = :some_event_occurred3

    subscribers = [
      {InputLogger, %{}},
      {Calculator, %{}},
      {MemoryLeakerOne, %{}},
      {BadOne, %{}}
    ]

    Observation.save({topic, id}, {subscribers, [], []})
    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [], [{InputLogger, %{}}]} ==
             Observation.fetch({topic, id})
  end

  test "mark_as_completed is idempotent" do
    topic = :idempotent_complete_test
    id = "E1"
    subscribers = [{InputLogger, %{}}, {Calculator, %{}}]

    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [{InputLogger, %{}}], []} ==
             Observation.fetch({topic, id})
  end

  test "mark_as_skipped is idempotent" do
    topic = :idempotent_skip_test
    id = "E1"
    subscribers = [{InputLogger, %{}}, {Calculator, %{}}]

    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [], [{InputLogger, %{}}]} ==
             Observation.fetch({topic, id})
  end

  test "first terminal state wins - completed then skipped" do
    topic = :terminal_wins_test1
    id = "E1"
    subscribers = [{InputLogger, %{}}, {Calculator, %{}}]

    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [{InputLogger, %{}}], []} ==
             Observation.fetch({topic, id})
  end

  test "first terminal state wins - skipped then completed" do
    topic = :terminal_wins_test2
    id = "E1"
    subscribers = [{InputLogger, %{}}, {Calculator, %{}}]

    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [], [{InputLogger, %{}}]} ==
             Observation.fetch({topic, id})
  end

  test "emits telemetry event on observation complete" do
    topic = :telemetry_complete_test
    id = "E1"
    subscriber = {InputLogger, %{}}
    subscribers = [subscriber]
    test_pid = self()

    handler_id = "obs-complete-test"

    capture_log(fn ->
      :telemetry.attach(
        handler_id,
        [:event_bus, :observation, :complete],
        fn event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event_name, measurements, metadata})
        end,
        nil
      )
    end)

    Store.create(%EventBus.Model.Event{id: id, topic: topic, data: nil})
    Observation.save({topic, id}, {subscribers, [], []})
    Observation.mark_as_completed({subscriber, {topic, id}})

    assert_receive {:telemetry, [:event_bus, :observation, :complete], measurements, metadata}
    assert measurements.subscriber_count == 1
    assert metadata.topic == topic
    assert metadata.event_id == id
    assert metadata.completers == [subscriber]
    assert metadata.skippers == []

    :telemetry.detach(handler_id)
  end

  test "double complete does not prevent cleanup" do
    topic = :double_complete_cleanup_test
    id = "E1"
    subscriber = {InputLogger, %{}}
    subscribers = [subscriber]

    # Create a store entry so cleanup's StoreManager.delete doesn't error
    Store.create(%EventBus.Model.Event{id: id, topic: topic, data: nil})
    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_completed({subscriber, {topic, id}})

    # Event should be cleaned up since the sole subscriber completed
    capture_log(fn ->
      assert nil == Observation.fetch({topic, id})
    end)

    # Second call should be a no-op
    capture_log(fn ->
      Observation.mark_as_completed({subscriber, {topic, id}})
    end)
  end
end
