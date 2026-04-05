defmodule EventBus.Service.SubscriptionTest do
  use ExUnit.Case, async: false

  alias EventBus.Support.Helper.{
    AnotherCalculator,
    Calculator,
    InputLogger,
    MemoryLeakerOne
  }

  alias EventBus.Service.Subscription

  doctest Subscription

  setup do
    on_exit(fn ->
      Subscription.unregister_topic(:auto_subscribed)
    end)

    Subscription.register_topic(:auto_subscribed)
    Subscription.register_topic(:metrics_received)
    Subscription.register_topic(:metrics_summed)

    for {subscriber, _topics} <- Subscription.subscribers() do
      Subscription.unsubscribe(subscriber)
    end

    :ok
  end

  test "subscribed?" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    assert Subscription.subscribed?({{InputLogger, %{}}, [".*"]})
    refute Subscription.subscribed?({InputLogger, [".*"]})
  end

  test "subscribe" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{Calculator, %{}}, [".*"]})
    Subscription.subscribe({{MemoryLeakerOne, %{}}, [".*"]})
    Subscription.subscribe({AnotherCalculator, [".*"]})

    subs = Subscription.subscribers()
    assert length(subs) == 4

    sub_keys = Enum.map(subs, fn {key, _patterns} -> key end)
    assert {InputLogger, %{}} in sub_keys
    assert {Calculator, %{}} in sub_keys
    assert {MemoryLeakerOne, %{}} in sub_keys
    assert AnotherCalculator in sub_keys
  end

  test "does not subscribe same subscriber" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})

    assert [{{InputLogger, %{}}, [".*"]}] == Subscription.subscribers()
  end

  test "unsubscribe" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{Calculator, %{}}, [".*"]})
    Subscription.subscribe({{MemoryLeakerOne, %{}}, [".*"]})
    Subscription.subscribe({AnotherCalculator, [".*"]})
    Subscription.unsubscribe({Calculator, %{}})
    Subscription.unsubscribe(AnotherCalculator)

    subs = Subscription.subscribers()
    assert length(subs) == 2

    sub_keys = Enum.map(subs, fn {key, _patterns} -> key end)
    assert {InputLogger, %{}} in sub_keys
    assert {MemoryLeakerOne, %{}} in sub_keys
    refute {Calculator, %{}} in sub_keys
    refute AnotherCalculator in sub_keys
  end

  test "register_topic auto subscribe workers" do
    topic = :auto_subscribed

    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{Calculator, %{}}, [".*"]})
    Subscription.subscribe({{MemoryLeakerOne, %{}}, ["other_received$"]})
    Subscription.subscribe({AnotherCalculator, [".*"]})

    Subscription.register_topic(topic)

    subs = Subscription.subscribers(topic)
    assert length(subs) == 3
    assert {InputLogger, %{}} in subs
    assert {Calculator, %{}} in subs
    assert AnotherCalculator in subs
    refute {MemoryLeakerOne, %{}} in subs
  end

  test "unregister_topic delete subscribers" do
    topic = :auto_subscribed

    Subscription.subscribe({{InputLogger, %{}}, [".*"]})
    Subscription.subscribe({{Calculator, %{}}, [".*"]})
    Subscription.subscribe({{MemoryLeakerOne, %{}}, ["other_received$"]})
    Subscription.subscribe({AnotherCalculator, [".*"]})

    Subscription.register_topic(topic)
    Subscription.unregister_topic(topic)

    assert [] == Subscription.subscribers(topic)
  end

  test "subscribers" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})

    assert [{{InputLogger, %{}}, [".*"]}] == Subscription.subscribers()
  end

  test "subscribers with event type" do
    Subscription.subscribe({{InputLogger, %{}}, [".*"]})

    assert [{InputLogger, %{}}] == Subscription.subscribers(:metrics_received)
    assert [{InputLogger, %{}}] == Subscription.subscribers(:metrics_summed)
  end

  test "subscribers with event type and without config" do
    Subscription.subscribe({AnotherCalculator, [".*"]})

    assert [AnotherCalculator] == Subscription.subscribers(:metrics_received)
    assert [AnotherCalculator] == Subscription.subscribers(:metrics_summed)
  end

  test "state is stored in ETS tables" do
    Subscription.subscribe(
      {{InputLogger, %{}}, ["metrics_received", "metrics_summed"]}
    )

    Subscription.subscribe({AnotherCalculator, ["metrics_received$"]})

    # Subscribers table
    subs = Subscription.subscribers()
    assert length(subs) == 2

    # Topic map
    mr_subs = Subscription.subscribers(:metrics_received)
    assert length(mr_subs) == 2
    assert {InputLogger, %{}} in mr_subs
    assert AnotherCalculator in mr_subs

    ms_subs = Subscription.subscribers(:metrics_summed)
    assert [{InputLogger, %{}}] == ms_subs

    assert [] == Subscription.subscribers(:auto_subscribed)
  end

  test "opts are stored in ETS for hot-path reads" do
    EventBus.subscribe(
      {AnotherCalculator, ["metrics_received"]},
      guard: fn _event -> true end,
      priority: 7
    )

    opts = EventBus.Manager.Subscription.fetch_opts(AnotherCalculator)
    assert opts.priority == 7
    assert is_function(opts.guard, 1)
  end
end
