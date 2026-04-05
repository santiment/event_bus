defmodule EventBus.Manager.ObservationTest do
  use ExUnit.Case, async: false
  alias EventBus.Manager.Observation

  alias EventBus.Support.Helper.{
    BadOne,
    Calculator,
    InputLogger,
    MemoryLeakerOne
  }

  doctest Observation

  setup do
    :ok
  end

  test "register_topic" do
    assert :ok == Observation.register_topic(:metrics_destroyed)
  end

  test "unregister_topic" do
    assert :ok == Observation.unregister_topic(:metrics_destroyed)
  end

  test "create" do
    topic = :some_event_occurred1
    id = "E1"

    subscribers = [
      {InputLogger, %{}},
      {Calculator, %{}},
      {MemoryLeakerOne, %{}},
      {BadOne, %{}}
    ]

    assert :ok == Observation.create({subscribers, {topic, id}})
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

    Observation.create({subscribers, {topic, id}})

    subscriber = {InputLogger, %{}}
    another_subscriber = {Calculator, %{}}

    # With an event_shadow tuple
    assert :ok === Observation.mark_as_completed({subscriber, {topic, id}})

    # With an open tuple
    assert :ok ===
             Observation.mark_as_completed({another_subscriber, topic, id})
  end

  test "skip" do
    topic = :some_event_occurred3
    id = "E1"

    subscribers = [
      {InputLogger, %{}},
      {Calculator, %{}},
      {MemoryLeakerOne, %{}},
      {BadOne, %{}}
    ]

    Observation.create({subscribers, {topic, id}})

    subscriber = {InputLogger, %{}}
    another_subscriber = {Calculator, %{}}

    # With an event_shadow tuple
    assert :ok == Observation.mark_as_skipped({subscriber, {topic, id}})

    # With an open tuple
    assert :ok == Observation.mark_as_skipped({another_subscriber, topic, id})
  end
end
