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

    assert_receive {:telemetry, [:event_bus, :observation, :complete],
                    measurements, metadata}

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

  test "fetch logs at info level when observation is missing" do
    topic = :obs_fetch_log_test
    id = "missing"

    # Temporarily lower Logger level so the lazy info message gets evaluated
    prev_level = Logger.level()
    Logger.configure(level: :info)

    log =
      capture_log([level: :info], fn ->
        assert nil == Observation.fetch({topic, id})
      end)

    Logger.configure(level: prev_level)

    assert log =~ "[EVENTBUS][OBSERVATION]"
    assert log =~ "#{topic}.#{id}.ets_fetch_error"
  end

  test "check_completion handles concurrent watcher deletion gracefully" do
    # Simulate the race condition where the watcher entry is deleted
    # between CAS on status_table and update_counter on watchers table.
    topic = :concurrent_delete_test
    id = "RACE1"
    subscriber = {InputLogger, %{}}

    # Set up status entry (so CAS succeeds) but no watcher entry
    :ets.insert(:eb_event_watcher_status, {{topic, id, subscriber}, :pending})

    # mark_as_completed: CAS succeeds (pending -> completed),
    # then check_completion tries update_counter on missing key -> rescue ArgumentError
    assert :ok == Observation.mark_as_completed({subscriber, {topic, id}})

    # Clean up
    :ets.delete(:eb_event_watcher_status, {topic, id, subscriber})
  end

  test "on_complete handles already-cleaned watcher entry" do
    # Simulate the race where on_complete fires but the watcher entry
    # was already deleted by a concurrent force_expire.
    topic = :on_complete_race_test
    id = "RACE2"
    sub_a = {InputLogger, %{}}
    sub_b = {Calculator, %{}}

    Store.create(%EventBus.Model.Event{id: id, topic: topic, data: nil})
    Observation.save({topic, id}, {[sub_a, sub_b], [], []})
    Observation.save_snapshot({topic, id}, %{sub_a => 0, sub_b => 0})

    # Complete sub_a normally (counter: 2 -> 1)
    Observation.mark_as_completed({sub_a, {topic, id}})
    # Watcher still exists
    assert [{_, _, 1}] = :ets.lookup(Observation.table_name(), {topic, id})

    # Now simulate: delete the watcher before sub_b completes, as if
    # force_expire ran concurrently
    :ets.delete(Observation.table_name(), {topic, id})
    :ets.delete(:eb_event_subscription_generations, {topic, id})

    # sub_b's CAS will succeed (status_table entry still exists)
    # but check_completion will hit the ArgumentError rescue
    assert :ok == Observation.mark_as_completed({sub_b, {topic, id}})

    # Clean up remaining status and store entries
    :ets.delete(:eb_event_watcher_status, {topic, id, sub_a})
    :ets.delete(:eb_event_watcher_status, {topic, id, sub_b})
    Store.delete({topic, id})
  end
end
