defmodule EventBus.Service.SweeperTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias EventBus.Model.Event
  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Service.Observation
  alias EventBus.Service.Store
  alias EventBus.Service.Sweeper

  @topic :sweeper_test_topic

  setup do
    EventBus.register_topic(@topic)

    # Clear any stale expired events left by other test modules.
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
  # Store cursor API
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
      for i <- 1..7, do: create_event("batch#{i}", @topic, 2_000)

      cutoff = System.monotonic_time() - ttl_native(500)
      all_ids = collect_all_cursor_ids(Store.select_expired(cutoff, 3), [])

      for i <- 1..7, do: assert("batch#{i}" in all_ids)
      for i <- 1..7, do: Store.delete({@topic, "batch#{i}"})
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
    test "cleans all observation tables" do
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
      assert sub_a in info.completers
      refute sub_b in info.completers
    end

    test "returns :not_found for already-cleaned events" do
      assert :not_found == Observation.force_expire({@topic, "nonexistent"})
    end
  end

  # ---------------------------------------------------------------------------
  # Observation.expire_batch/2
  # ---------------------------------------------------------------------------

  describe "Observation.expire_batch/2" do
    test "cleans all tables and returns count with topic breakdown" do
      sub = {BatchExpSub, nil}

      for i <- 1..3 do
        create_event("be#{i}")
        setup_observation(@topic, "be#{i}", [sub])
      end

      shadows = Enum.map(1..3, fn i -> {@topic, "be#{i}"} end)
      limited = SubscriptionManager.limited_subscribers()
      assert {3, %{@topic => 3}} = Observation.expire_batch(shadows, limited)

      for i <- 1..3 do
        assert [] == :ets.lookup(Store.table_name(), {@topic, "be#{i}"})
        assert [] == :ets.lookup(Observation.table_name(), {@topic, "be#{i}"})
      end
    end

    test "returns {0, %{}} for empty list" do
      assert {0, %{}} == Observation.expire_batch([], MapSet.new())
    end

    test "skips already-cleaned events" do
      sub = {SkipSub, nil}
      create_event("exists")
      setup_observation(@topic, "exists", [sub])

      limited = SubscriptionManager.limited_subscribers()
      assert {1, %{@topic => 1}} = Observation.expire_batch([{@topic, "exists"}, {@topic, "ghost"}], limited)
    end

    test "fast path with empty limited_set" do
      sub = {FastPathSub, nil}

      for i <- 1..3 do
        create_event("fp#{i}")
        setup_observation(@topic, "fp#{i}", [sub])
      end

      shadows = Enum.map(1..3, fn i -> {@topic, "fp#{i}"} end)
      assert {3, %{@topic => 3}} = Observation.expire_batch(shadows, MapSet.new())

      for i <- 1..3 do
        assert [] == :ets.lookup(Store.table_name(), {@topic, "fp#{i}"})
      end
    end

    test "returns per-topic counts across multiple topics" do
      topic2 = :sweeper_topic2
      EventBus.register_topic(topic2)
      sub = {MultiTopicSub, nil}

      create_event("mt1", @topic, 0)
      create_event("mt2", @topic, 0)
      create_event("mt3", topic2, 0)
      setup_observation(@topic, "mt1", [sub])
      setup_observation(@topic, "mt2", [sub])
      setup_observation(topic2, "mt3", [sub])

      shadows = [{@topic, "mt1"}, {@topic, "mt2"}, {topic2, "mt3"}]
      assert {3, topic_counts} = Observation.expire_batch(shadows, MapSet.new())
      assert topic_counts[@topic] == 2
      assert topic_counts[topic2] == 1

      EventBus.unregister_topic(topic2)
    end

    test "decrements limited subscription counters" do
      topic = :batch_limit_test
      EventBus.register_topic(topic)

      defmodule BatchLimitSub do
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe_n({{BatchLimitSub, nil}, ["batch_limit_test"]}, 5)

      for id <- ["bl1", "bl2"] do
        EventBus.notify_sync(%Event{id: id, topic: topic, data: %{}})
      end

      limited = SubscriptionManager.limited_subscribers()
      assert MapSet.member?(limited, {BatchLimitSub, nil})

      Observation.expire_batch([{topic, "bl1"}, {topic, "bl2"}], limited)

      # Subscriber should still work after expiry
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

      limited = SubscriptionManager.limited_subscribers()
      assert MapSet.member?(limited, {MixedLimited, nil})
      refute MapSet.member?(limited, {MixedUnlimited, nil})

      Observation.expire_batch([{topic, "mx1"}], limited)

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
  # Sweeper.sweep/1 (default :bulk_smart mode)
  # ---------------------------------------------------------------------------

  describe "sweep (bulk_smart)" do
    test "expires old events and returns count" do
      sub = {SweepSub1, nil}
      create_event("sw1", @topic, 500)
      setup_observation(@topic, "sw1", [sub])

      count = Sweeper.sweep(ttl_native(100))
      assert count == 1

      capture_log(fn ->
        assert nil == Store.fetch({@topic, "sw1"})
      end)
    end

    test "does not expire recent events" do
      sub = {SweepSub2, nil}
      create_event("sw2", @topic, 0)
      setup_observation(@topic, "sw2", [sub])

      assert 0 == Sweeper.sweep(ttl_native(60_000))
      assert {[^sub], _, _} = Observation.fetch({@topic, "sw2"})

      Observation.force_expire({@topic, "sw2"})
    end

    test "returns 0 when no events are expired" do
      assert 0 == Sweeper.sweep(ttl_native(999_999_999))
    end

    test "cleans all four ETS tables" do
      sub = {CleanSub, nil}
      create_event("clean1", @topic, 2_000)
      setup_observation(@topic, "clean1", [sub])

      Sweeper.sweep(ttl_native(500))

      assert [] == :ets.lookup(Store.table_name(), {@topic, "clean1"})
      assert [] == :ets.lookup(Observation.table_name(), {@topic, "clean1"})
      assert [] == :ets.lookup(:eb_event_watcher_status, {@topic, "clean1", sub})
      assert [] == :ets.lookup(:eb_event_subscription_generations, {@topic, "clean1"})
    end

    test "processes events across multiple internal batches" do
      sub = {BatchSub, nil}

      for i <- 1..150 do
        create_event("b#{i}", @topic, 2_000)
        setup_observation(@topic, "b#{i}", [sub])
      end

      assert 150 == Sweeper.sweep(ttl_native(500))
    end

    test "emits :cycle telemetry with expired_per_topic" do
      test_pid = self()
      handler_id = "bulk-cycle-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn _name, measurements, metadata, _config ->
          send(test_pid, {:cycle, measurements, metadata})
        end,
        nil
      )

      topic2 = :sweeper_tel_topic2
      EventBus.register_topic(topic2)
      sub = {TelBulkSub, nil}

      create_event("tb1", @topic, 2_000)
      create_event("tb2", @topic, 2_000)
      create_event("tb3", topic2, 2_000)
      setup_observation(@topic, "tb1", [sub])
      setup_observation(@topic, "tb2", [sub])
      setup_observation(topic2, "tb3", [sub])

      Sweeper.sweep(ttl_native(500))

      assert_receive {:cycle, measurements, metadata}
      assert measurements.expired_count == 3
      assert is_integer(measurements.duration)
      assert metadata.expired_per_topic[@topic] == 2
      assert metadata.expired_per_topic[topic2] == 1

      :telemetry.detach(handler_id)
      EventBus.unregister_topic(topic2)
    end

    test "does not emit per-event :expired telemetry" do
      test_pid = self()
      handler_id = "bulk-no-per-event-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :expired],
        fn _name, _m, _md, _c -> send(test_pid, :unexpected) end,
        nil
      )

      sub = {NoPerEventSub, nil}
      create_event("npe1", @topic, 2_000)
      setup_observation(@topic, "npe1", [sub])

      Sweeper.sweep(ttl_native(500))

      refute_receive :unexpected, 100
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Sweeper.sweep/2 with strategy: :detailed
  # ---------------------------------------------------------------------------

  describe "sweep (detailed)" do
    test "expires old events and returns count" do
      sub = {DetailSub1, nil}
      create_event("ds1", @topic, 500)
      setup_observation(@topic, "ds1", [sub])

      assert 1 == Sweeper.sweep(ttl_native(100), strategy: :detailed)

      capture_log(fn ->
        assert nil == Store.fetch({@topic, "ds1"})
      end)
    end

    test "emits per-event :expired telemetry" do
      test_pid = self()
      handler_id = "detail-expired-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :expired],
        fn _name, measurements, metadata, _config ->
          send(test_pid, {:expired, measurements, metadata})
        end,
        nil
      )

      sub = {DetailTelSub, nil}
      create_event("dt1", @topic, 2_000)
      setup_observation(@topic, "dt1", [sub])

      Sweeper.sweep(ttl_native(500), strategy: :detailed)

      assert_receive {:expired, measurements, metadata}
      assert is_integer(measurements.age)
      assert measurements.age > 0
      assert metadata.topic == @topic
      assert metadata.event_id == "dt1"
      assert metadata.pending_subscribers == [sub]

      :telemetry.detach(handler_id)
    end

    test "also emits :cycle telemetry" do
      test_pid = self()
      handler_id = "detail-cycle-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn _name, measurements, _metadata, _config ->
          send(test_pid, {:cycle, measurements})
        end,
        nil
      )

      sub = {DetailCycleSub, nil}
      create_event("dc1", @topic, 2_000)
      setup_observation(@topic, "dc1", [sub])

      Sweeper.sweep(ttl_native(500), strategy: :detailed)

      assert_receive {:cycle, measurements}
      assert measurements.expired_count == 1

      :telemetry.detach(handler_id)
    end

    test "handles limited subscribers correctly" do
      topic = :detail_limit_test
      EventBus.register_topic(topic)

      defmodule DetailLimitSub do
        def process({_topic, _id}), do: :ok
      end

      EventBus.subscribe_n({{DetailLimitSub, nil}, ["detail_limit_test"]}, 3)

      EventBus.notify_sync(%Event{id: "dl1", topic: topic, data: %{}})

      Sweeper.sweep(ttl_native(0), strategy: :detailed)

      # Subscriber should still work
      EventBus.notify_sync(%Event{id: "dl2", topic: topic, data: %{}})
      assert {[{DetailLimitSub, nil}], _, _} = Observation.fetch({topic, "dl2"})

      Observation.force_expire({topic, "dl2"})
      EventBus.unsubscribe({DetailLimitSub, nil})
      EventBus.unregister_topic(topic)
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
        fn _name, measurements, _metadata, _config ->
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
  # Custom strategy
  # ---------------------------------------------------------------------------

  describe "custom strategy" do
    defmodule CountingStrategy do
      @behaviour EventBus.SweepStrategy

      alias EventBus.Manager.Subscription, as: SubscriptionManager
      alias EventBus.Service.Observation, as: ObservationService

      @impl true
      def init do
        limited = SubscriptionManager.limited_subscribers()
        %{limited: limited, ids: []}
      end

      @impl true
      def handle_batch(batch, state) do
        event_shadows = Enum.map(batch, fn {topic, id, _} -> {topic, id} end)
        {count, _topics} = ObservationService.expire_batch(event_shadows, state.limited)
        ids = Enum.map(event_shadows, fn {_topic, id} -> id end)
        {count, %{state | ids: state.ids ++ ids}}
      end

      @impl true
      def telemetry_metadata(state) do
        %{expired_ids: state.ids}
      end
    end

    test "accepts a custom module via strategy: option" do
      test_pid = self()
      handler_id = "custom-strategy-#{System.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:event_bus, :sweep, :cycle],
        fn _name, _measurements, metadata, _config ->
          send(test_pid, {:meta, metadata})
        end,
        nil
      )

      sub = {CustomSub, nil}
      create_event("cs1", @topic, 2_000)
      create_event("cs2", @topic, 2_000)
      setup_observation(@topic, "cs1", [sub])
      setup_observation(@topic, "cs2", [sub])

      assert 2 == Sweeper.sweep(ttl_native(500), strategy: CountingStrategy)

      assert_receive {:meta, metadata}
      assert "cs1" in metadata.expired_ids or "cs2" in metadata.expired_ids
      assert length(metadata.expired_ids) == 2

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Integration
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
      assert 1 == Sweeper.sweep(ttl_native(1))

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

      EventBus.notify_sync(%Event{id: "norm1", topic: topic, data: %{}})

      Process.sleep(10)

      capture_log(fn ->
        assert nil == EventBus.fetch_event({topic, "norm1"})
      end)

      assert 0 == Sweeper.sweep(ttl_native(1))

      EventBus.unsubscribe({GoodSubscriber, nil})
      EventBus.unregister_topic(topic)
    end
  end
end
