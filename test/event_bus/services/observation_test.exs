defmodule EventBus.Service.ObservationTest do
  use ExUnit.Case, async: false

  alias EventBus.Service.{Observation, Topic}

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

  test "exist?" do
    topic = :metrics_received_1
    Observation.register_topic(topic)

    assert Observation.exist?(topic)
  end

  test "register_topic" do
    topic = :metrics_destroyed
    Observation.register_topic(topic)
    all_tables = :ets.all()
    table_name = String.to_atom("eb_ew_#{topic}")

    assert Enum.any?(all_tables, fn t -> t == table_name end)
  end

  test "unregister_topic" do
    topic = :metrics_destroyed
    Observation.register_topic(topic)
    Observation.unregister_topic(topic)
    all_tables = :ets.all()
    table_name = String.to_atom("eb_ew_#{topic}")

    refute Enum.any?(all_tables, fn t -> t == table_name end)
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

    Observation.register_topic(topic)
    Observation.save({topic, id}, {subscribers, [], []})

    assert {subscribers, [], []} == Observation.fetch({topic, id})
  end

  test "fetch a non-existent id" do
    topic = :some_event_occurred1
    id = "NA"

    Observation.register_topic(topic)

    assert nil == Observation.fetch({topic, id})
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

    Observation.register_topic(topic)
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

    Observation.register_topic(topic)
    Observation.save({topic, id}, {subscribers, [], []})
    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [], [{InputLogger, %{}}]} ==
             Observation.fetch({topic, id})
  end

  test "mark_as_completed is idempotent" do
    topic = :idempotent_complete_test
    id = "E1"
    subscribers = [{InputLogger, %{}}, {Calculator, %{}}]

    Observation.register_topic(topic)
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

    Observation.register_topic(topic)
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

    Observation.register_topic(topic)
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

    Observation.register_topic(topic)
    Observation.save({topic, id}, {subscribers, [], []})

    Observation.mark_as_skipped({{InputLogger, %{}}, {topic, id}})
    Observation.mark_as_completed({{InputLogger, %{}}, {topic, id}})

    assert {subscribers, [], [{InputLogger, %{}}]} ==
             Observation.fetch({topic, id})
  end

  test "double complete does not prevent cleanup" do
    topic = :double_complete_cleanup_test
    id = "E1"
    subscriber = {InputLogger, %{}}
    subscribers = [subscriber]

    # Register both store and observation tables since cleanup deletes from store
    EventBus.Service.Store.register_topic(topic)
    Observation.register_topic(topic)
    Observation.save({topic, id}, {subscribers, [], []})

    # Without idempotency fix, the second call would add a duplicate
    # making length(completers) > length(subscribers) and preventing cleanup
    Observation.mark_as_completed({subscriber, {topic, id}})

    # Event should be cleaned up since the sole subscriber completed
    assert nil == Observation.fetch({topic, id})

    # Second call should be a no-op
    Observation.mark_as_completed({subscriber, {topic, id}})
  end
end
