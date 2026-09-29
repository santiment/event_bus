defmodule EventBusTest do
  use ExUnit.Case, async: false

  alias EventBus.Model.Event
  alias EventBus.Service.Observation

  @topic :event_bus_facade_topic

  setup do
    EventBus.register_topic(@topic)
    on_exit(fn -> EventBus.unregister_topic(@topic) end)
    :ok
  end

  defmodule Forwarder do
    def process({topic, id}) do
      send(
        :persistent_term.get({__MODULE__, :pid}),
        {:got, EventBus.fetch_event({topic, id})}
      )

      EventBus.mark_as_completed({__MODULE__, {topic, id}})
    end
  end

  defp observe(id, subscriber) do
    Observation.save({@topic, id}, {[subscriber, {Forwarder, :other}], [], []})
  end

  describe "mark_as_completed/1 and mark_as_skipped/1" do
    test "accept {subscriber, {topic, id}} and {subscriber, topic, id}" do
      configured = {Forwarder, %{k: 1}}

      cases = [
        {"c1", {Forwarder, nil}, &EventBus.mark_as_completed/1,
         {Forwarder, {@topic, "c1"}}, 1},
        {"c2", {Forwarder, nil}, &EventBus.mark_as_completed/1,
         {Forwarder, @topic, "c2"}, 1},
        {"c3", configured, &EventBus.mark_as_completed/1,
         {configured, {@topic, "c3"}}, 1},
        {"c4", configured, &EventBus.mark_as_completed/1,
         {configured, @topic, "c4"}, 1},
        {"s1", {Forwarder, nil}, &EventBus.mark_as_skipped/1,
         {Forwarder, {@topic, "s1"}}, 2},
        {"s2", {Forwarder, nil}, &EventBus.mark_as_skipped/1,
         {Forwarder, @topic, "s2"}, 2},
        {"s3", configured, &EventBus.mark_as_skipped/1,
         {configured, {@topic, "s3"}}, 2},
        {"s4", configured, &EventBus.mark_as_skipped/1,
         {configured, @topic, "s4"}, 2}
      ]

      for {id, subscriber, mark, ref, terminal_index} <- cases do
        observe(id, subscriber)
        assert :ok == mark.(ref)

        assert [subscriber] ==
                 elem(Observation.fetch({@topic, id}), terminal_index),
               "#{inspect(ref)} did not reach the expected terminal state"
      end
    end
  end

  test "notify/1 dispatches asynchronously and the event can be fetched" do
    :persistent_term.put({Forwarder, :pid}, self())
    EventBus.subscribe({Forwarder, ["event_bus_facade_topic"]})

    try do
      assert :ok ==
               EventBus.notify(%Event{
                 id: "async-1",
                 topic: @topic,
                 data: :payload
               })

      assert_receive {:got, %Event{id: "async-1", data: :payload}}, 1_000
    after
      EventBus.unsubscribe(Forwarder)
      :persistent_term.erase({Forwarder, :pid})
    end
  end
end
