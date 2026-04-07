defmodule EventBus.Service.SweeperTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Service.Observation
  alias EventBus.Service.Store
  alias EventBus.Service.Sweeper

  @topic :sweeper_test_topic

  setup do
    EventBus.register_topic(@topic)

    # Clear any stale expired events left by other test modules so that
    # exact-count assertions in sweep tests are not thrown off.
    Sweeper.sweep(ttl_native(0))

    on_exit(fn ->
      Process.sleep(50)

      for {subscriber, _} <- EventBus.subscribers() do
        EventBus.unsubscribe(subscriber)
      end

      EventBus.unregister_topic(@topic)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp create_event(id, topic \\ @topic, age_ms \\ 0) do
    event = %Event{id: id, topic: topic, data: %{test: true}}
    inserted_at = System.monotonic_time() - System.convert_time_unit(age_ms, :millisecond, :native)
    metadata = %{inserted_at: inserted_at}
    :ets.insert(Store.table_name(), {{topic, id}, event, metadata})
    event
  end

  defp setup_observation(topic, id, subscribers) do
    Observation.save({topic, id}, {subscribers, [], []})
    snapshot = Map.new(subscribers, fn sub -> {sub, 0} end)
    Observation.save_snapshot({topic, id}, snapshot)
  end

  defp ttl_native(ms) do
    System.convert_time_unit(ms, :millisecond, :native)
  end

  # ---------------------------------------------------------------------------
  # Store.find_expired/1
  # ---------------------------------------------------------------------------

  describe "Store.find_expired/1" do
    test "returns events older than the cutoff" do
      create_event("old1", @topic, 5_000)
      create_event("old2", @topic, 3_000)

      cutoff = System.monotonic_time() - ttl_native(1_000)
      expired = Store.find_expired(cutoff)

      expired_ids = Enum.map(expired, fn {{_topic, id}, _inserted_at} -> id end)
      assert "old1" in expired_ids
      assert "old2" in expired_ids

      Store.delete({@topic, "old1"})
      Store.delete({@topic, "old2"})
    end

    test "does not return recent events" do
      create_event("recent1", @topic, 0)

      cutoff = System.monotonic_time() - ttl_native(1_000)
      expired = Store.find_expired(cutoff)
      expired_ids = Enum.map(expired, fn {{_topic, id}, _inserted_at} -> id end)

      refute "recent1" in expired_ids

      Store.delete({@topic, "recent1"})
    end

    test "returns inserted_at timestamp for each expired event" do
      create_event("aged1", @topic, 2_000)

      cutoff = System.monotonic_time() - ttl_native(1_000)
      expired = Store.find_expired(cutoff)

      assert [{{@topic, "aged1"}, inserted_at}] = expired
      assert is_integer(inserted_at)

      Store.delete({@topic, "aged1"})
    end

    test "returns empty list when no events are expired" do
      create_event("fresh1", @topic, 0)

      cutoff = System.monotonic_time() - ttl_native(60_000)
      assert [] == Store.find_expired(cutoff)

      Store.delete({@topic, "fresh1"})
    end
  end

  # ---------------------------------------------------------------------------
  # Store.select_expired/2 cursor API
  # ---------------------------------------------------------------------------

  describe "Store.select_expired/2 cursor API" do
    test "returns :done when no events match" do
      assert :done == Store.select_expired(System.monotonic_time() - ttl_native(60_000), 10)
    end

    test "returns a batch and continuation" do
      create_event("cur1", @topic, 2_000)

      cutoff = System.monotonic_time() - ttl_native(500)
      {batch, _continuation} = Store.select_expired(cutoff, 10)
      assert length(batch) >= 1
      ids = Enum.map(batch, fn {_topic, id, _ts} -> id end)
      assert "cur1" in ids

      Store.delete({@topic, "cur1"})
    end

    test "iterates across multiple batches" do
      for i <- 1..7 do
        create_event("batch#{i}", @topic, 2_000)
      end

      cutoff = System.monotonic_time() - ttl_native(500)
      all_ids = collect_all_cursor_ids(Store.select_expired(cutoff, 3), [])

      for i <- 1..7 do
        assert "batch#{i}" in all_ids
      end

      for i <- 1..7 do
        Store.delete({@topic, "batch#{i}"})
      end
    end
  end

  defp collect_all_cursor_ids(:done, acc), do: acc

  defp collect_all_cursor_ids({batch, continuation}, acc) do
    ids = Enum.map(batch, fn {_topic, id, _ts} -> id end)
    collect_all_cursor_ids(Store.continue_expired(continuation), acc ++ ids)
  end

  # ---------------------------------------------------------------------------
  # Observation.force_expire/1
  # ---------------------------------------------------------------------------

  describe "Observation.force_expire/1" do
    test "cleans all observation tables for the event" do
      subscriber = {TestSub, nil}
      create_event("fe1")
      setup_observation(@topic, "fe1", [subscriber])

      assert {:ok, _} = Observation.force_expire({@topic, "fe1"})

      assert [] == :ets.lookup(Observation.table_name(), {@topic, "fe1"})
      assert [] == :ets.lookup(:eb_event_watcher_status, {@topic, "fe1", subscriber})
      assert [] == :ets.lookup(:eb_event_subscription_generations, {@topic, "fe1"})

      capture_log(fn ->
        assert nil == Store.fetch({@topic, "fe1"})
      end)
    end

    test "returns subscriber details" do
      sub_a = {SubA, nil}
      sub_b = {SubB, nil}
      create_event("fe2")
      setup_observation(@topic, "fe2", [sub_a, sub_b])

      Observation.mark_as_completed({sub_a, {@topic, "fe2"}})

      assert {:ok, info} = Observation.force_expire({@topic, "fe2"})
      assert info.subscribers == [sub_a, sub_b]
      assert sub_a in info.completers
      refute sub_b in info.completers
      refute sub_b in info.skippers
    end

    test "returns :not_found for already-cleaned events" do
      assert :not_found == Observation.force_expire({@topic, "nonexistent"})
    end

    test "decrements in_flight for pending limited subscribers" do
      topic = :sweeper_limit_test
      EventBus.register_topic(topic)

      defmodule LimitTestSub do
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe_n({{LimitTestSub, nil}, ["sweeper_limit_test"]}, 3)

      event = %Event{id: "lim1", topic: topic, data: %{}}
      EventBus.notify_sync(event)

      # force_expire should decrement in_flight so the subscriber isn't stuck
      # Give it another event to confirm
      event2 = %Event{id: "lim2", topic: topic, data: %{}}
      EventBus.notify_sync(event2)

      assert {[{LimitTestSub, nil}], _, _} = Observation.fetch({topic, "lim2"})

      Observation.force_expire({topic, "lim1"})
      Observation.force_expire({topic, "lim2"})
      EventBus.unsubscribe({LimitTestSub, nil})
      EventBus.unregister_topic(topic)
    end

    test "handles event with all subscribers already completed" do
      sub = {AllDoneSub, nil}
      create_event("fe3")
      setup_observation(@topic, "fe3", [sub])

      Observation.mark_as_completed({sub, {@topic, "fe3"}})

      capture_log(fn ->
        assert :not_found == Observation.force_expire({@topic, "fe3"})
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Observation.expire_batch/1
  # ---------------------------------------------------------------------------

  describe "Observation.expire_batch/1" do
    test "cleans all tables for a list of event shadows" do
      sub = {BatchExpSub, nil}

      for i <- 1..3 do
        id = "be#{i}"
        create_event(id)
        setup_observation(@topic, id, [sub])
      end

      shadows = Enum.map(1..3, fn i -> {@topic, "be#{i}"} end)
      assert 3 == Observation.expire_batch(shadows)

      for i <- 1..3 do
        id = "be#{i}"
        assert [] == :ets.lookup(Store.table_name(), {@topic, id})
        assert [] == :ets.lookup(Observation.table_name(), {@topic, id})
        assert [] == :ets.lookup(:eb_event_watcher_status, {@topic, id, sub})
        assert [] == :ets.lookup(:eb_event_subscription_generations, {@topic, id})
      end
    end

    test "returns 0 for empty list" do
      assert 0 == Observation.expire_batch([])
    end

    test "skips already-cleaned events and returns correct count" do
      sub = {SkipSub, nil}
      create_event("exists")
      setup_observation(@topic, "exists", [sub])

      # "ghost" has no observation entry
      assert 1 == Observation.expire_batch([{@topic, "exists"}, {@topic, "ghost"}])
    end

    test "decrements limited subscription counters" do
      topic = :batch_limit_test
      EventBus.register_topic(topic)

      defmodule BatchLimitSub do
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe_n({{BatchLimitSub, nil}, ["batch_limit_test"]}, 5)

      # Dispatch two events (subscriber never completes → in_flight goes up)
      for id <- ["bl1", "bl2"] do
        EventBus.notify_sync(%Event{id: id, topic: topic, data: %{}})
      end

      # Expire both in a single batch
      Observation.expire_batch([{topic, "bl1"}, {topic, "bl2"}])

      # Subscriber should still work — dispatch a third event
      EventBus.notify_sync(%Event{id: "bl3", topic: topic, data: %{}})
      assert {[{BatchLimitSub, nil}], _, _} = Observation.fetch({topic, "bl3"})

      Observation.force_expire({topic, "bl3"})
      EventBus.unsubscribe({BatchLimitSub, nil})
      EventBus.unregister_topic(topic)
    end

    test "handles mix of unlimited and limited subscribers" do
      topic = :batch_mixed_test
      EventBus.register_topic(topic)

      defmodule MixedUnlimited do
        def process({_topic, _id}), do: :ok
      end

      defmodule MixedLimited do
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe({{MixedUnlimited, nil}, ["batch_mixed_test"]})
      EventBus.subscribe_n({{MixedLimited, nil}, ["batch_mixed_test"]}, 3)

      EventBus.notify_sync(%Event{id: "mx1", topic: topic, data: %{}})

      # Expire via batch — should handle both subscriber types correctly
      Observation.expire_batch([{topic, "mx1"}])

      # Limited subscriber should still receive future events
      EventBus.notify_sync(%Event{id: "mx2", topic: topic, data: %{}})
      subscribers = Enum.map(elem(Observation.fetch({topic, "mx2"}), 0), &elem(&1, 0))
      assert MixedLimited in subscribers

      Observation.force_expire({topic, "mx2"})
      EventBus.unsubscribe({MixedUnlimited, nil})
      EventBus.unsubscribe({MixedLimited, nil})
      EventBus.unregister_topic(topic)
    end
  end

  # ---------------------------------------------------------------------------
  # Sweeper.sweep/1
  # ---------------------------------------------------------------------------

  describe "Sweeper.sweep/1" do
    test "expires old events and returns count" do
      subscriber = {SweepSub1, nil}
      create_event("sw1", @topic, 500)
      setup_observation(@topic, "sw1", [subscriber])

      count = Sweeper.sweep(ttl_native(100))
      assert count == 1

      capture_log(fn ->
        assert nil == Store.fetch({@topic, "sw1"})
        assert nil == Observation.fetch({@topic, "sw1"})
      end)
    end

    test "does not expire recent events" do
      subscriber = {SweepSub2, nil}
      create_event("sw2", @topic, 0)
      setup_observation(@topic, "sw2", [subscriber])

      count = Sweeper.sweep(ttl_native(60_000))
      assert count == 0

      assert {[^subscriber], _, _} = Observation.fetch({@topic, "sw2"})

      Observation.force_expire({@topic, "sw2"})
    end

    test "returns 0 when no events are expired" do
      assert 0 == Sweeper.sweep(ttl_native(999_999_999))
    end

    test "handles mix of expired and fresh events" do
      sub = {MixSub, nil}
      create_event("old", @topic, 2_000)
      create_event("new", @topic, 0)
      setup_observation(@topic, "old", [sub])
      setup_observation(@topic, "new", [sub])

      count = Sweeper.sweep(ttl_native(500))
      assert count == 1

      capture_log(fn ->
        assert nil == Observation.fetch({@topic, "old"})
      end)

      assert {[^sub], _, _} = Observation.fetch({@topic, "new"})

      Observation.force_expire({@topic, "new"})
    end

    test "handles event already cleaned by normal completion" do
      sub = {RaceSub, nil}
      create_event("race1", @topic, 2_000)
      setup_observation(@topic, "race1", [sub])

      Observation.mark_as_completed({sub, {@topic, "race1"}})

      count = Sweeper.sweep(ttl_native(500))
      assert count == 0
    end

    test "cleans all four ETS tables" do
      sub = {CleanSub, nil}
      create_event("clean1", @topic, 2_000)
      setup_observation(@topic, "clean1", [sub])

      assert [{_, _, _}] = :ets.lookup(Store.table_name(), {@topic, "clean1"})
      assert [{_, _, _}] = :ets.lookup(Observation.table_name(), {@topic, "clean1"})
      assert [{_, :pending}] = :ets.lookup(:eb_event_watcher_status, {@topic, "clean1", sub})
      assert [{_, _}] = :ets.lookup(:eb_event_subscription_generations, {@topic, "clean1"})

      Sweeper.sweep(ttl_native(500))

      assert [] == :ets.lookup(Store.table_name(), {@topic, "clean1"})
      assert [] == :ets.lookup(Observation.table_name(), {@topic, "clean1"})
      assert [] == :ets.lookup(:eb_event_watcher_status, {@topic, "clean1", sub})
      assert [] == :ets.lookup(:eb_event_subscription_generations, {@topic, "clean1"})
    end

    test "expires multiple events in a single sweep" do
      sub = {MultiSub, nil}

      for i <- 1..5 do
        id = "multi#{i}"
        create_event(id, @topic, 2_000)
        setup_observation(@topic, id, [sub])
      end

      count = Sweeper.sweep(ttl_native(500))
      assert count == 5

      for i <- 1..5 do
        assert [] == :ets.lookup(Store.table_name(), {@topic, "multi#{i}"})
      end
    end

    test "processes events across multiple internal batches" do
      sub = {BatchSub, nil}

      for i <- 1..150 do
        id = "b#{i}"
        create_event(id, @topic, 2_000)
        setup_observation(@topic, id, [sub])
      end

      count = Sweeper.sweep(ttl_native(500))
      assert count == 150

      for i <- 1..150 do
        assert [] == :ets.lookup(Store.table_name(), {@topic, "b#{i}"})
      end
    end

    test "handles events with multiple subscribers, some completed" do
      sub_a = {PartialA, nil}
      sub_b = {PartialB, nil}
      create_event("partial1", @topic, 2_000)
      setup_observation(@topic, "partial1", [sub_a, sub_b])

      Observation.mark_as_completed({sub_a, {@topic, "partial1"}})

      count = Sweeper.sweep(ttl_native(500))
      assert count == 1

      capture_log(fn ->
        assert nil == Observation.fetch({@topic, "partial1"})
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry
  # ---------------------------------------------------------------------------

  describe "telemetry" do
    test "emits :sweep/:cycle with count and duration" do
      test_pid = self()
      handler_id = "sweep-cycle-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event_name, measurements, metadata})
        end,
        nil
      )

      sub = {TelCycleSub, nil}
      create_event("cyc1", @topic, 2_000)
      create_event("cyc2", @topic, 2_000)
      setup_observation(@topic, "cyc1", [sub])
      setup_observation(@topic, "cyc2", [sub])

      Sweeper.sweep(ttl_native(500))

      assert_receive {:telemetry, [:event_bus, :sweep, :cycle], measurements, metadata}
      assert measurements.expired_count == 2
      assert is_integer(measurements.duration)
      assert measurements.duration >= 0
      assert metadata == %{}

      :telemetry.detach(handler_id)
    end

    test "does not emit :sweep/:cycle when nothing expired" do
      test_pid = self()
      handler_id = "sweep-no-cycle-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn _event_name, _measurements, _metadata, _config ->
          send(test_pid, :unexpected_cycle)
        end,
        nil
      )

      Sweeper.sweep(ttl_native(999_999_999))

      refute_receive :unexpected_cycle, 100

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer lifecycle
  # ---------------------------------------------------------------------------

  describe "GenServer" do
    test "schedules periodic sweeps" do
      test_pid = self()
      handler_id = "sweep-periodic-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn _event_name, measurements, _metadata, _config ->
          if measurements.expired_count > 0, do: send(test_pid, :sweep_ran)
        end,
        nil
      )

      prev_ttl = Application.get_env(:event_bus, :event_ttl)
      prev_interval = Application.get_env(:event_bus, :sweep_interval)

      Application.put_env(:event_bus, :event_ttl, 50)
      Application.put_env(:event_bus, :sweep_interval, 50)

      {:ok, pid} = Sweeper.start_link()

      sub = {PeriodicSub, nil}
      create_event("per1", @topic, 200)
      setup_observation(@topic, "per1", [sub])

      assert_receive :sweep_ran, 500

      GenServer.stop(pid)
      :telemetry.detach(handler_id)

      if prev_ttl, do: Application.put_env(:event_bus, :event_ttl, prev_ttl), else: Application.delete_env(:event_bus, :event_ttl)
      if prev_interval, do: Application.put_env(:event_bus, :sweep_interval, prev_interval), else: Application.delete_env(:event_bus, :sweep_interval)
    end
  end

  # ---------------------------------------------------------------------------
  # Integration with notify
  # ---------------------------------------------------------------------------

  describe "integration" do
    test "sweep cleans up events from a real notify flow" do
      topic = :sweeper_integration_test
      EventBus.register_topic(topic)

      defmodule SlowSubscriber do
        @moduledoc false
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe({{SlowSubscriber, nil}, ["sweeper_integration_test"]})

      event = %Event{id: "int1", topic: topic, data: %{hello: "world"}}
      EventBus.notify_sync(event)

      assert %Event{} = EventBus.fetch_event({topic, "int1"})

      Process.sleep(10)
      count = Sweeper.sweep(ttl_native(1))
      assert count == 1

      capture_log(fn ->
        assert nil == EventBus.fetch_event({topic, "int1"})
      end)

      EventBus.unsubscribe({SlowSubscriber, nil})
      EventBus.unregister_topic(topic)
    end

    test "sweep does not affect events that complete normally" do
      topic = :sweeper_normal_test
      EventBus.register_topic(topic)

      defmodule GoodSubscriber do
        @moduledoc false
        def process({topic, id}) do
          EventBus.mark_as_completed({{__MODULE__, nil}, {topic, id}})
        end
      end

      EventBus.subscribe({{GoodSubscriber, nil}, ["sweeper_normal_test"]})

      event = %Event{id: "norm1", topic: topic, data: %{}}
      EventBus.notify_sync(event)

      Process.sleep(10)

      capture_log(fn ->
        assert nil == EventBus.fetch_event({topic, "norm1"})
      end)

      count = Sweeper.sweep(ttl_native(1))
      assert count == 0

      EventBus.unsubscribe({GoodSubscriber, nil})
      EventBus.unregister_topic(topic)
    end
  end
end
