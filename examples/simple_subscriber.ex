# A minimal subscriber that processes events synchronously.
#
# Usage:
#   EventBus.register_topic(:order_created)
#   EventBus.subscribe({SimpleSubscriber, ["order_created"]})
#
defmodule SimpleSubscriber do
  @behaviour EventBus.Subscriber

  @impl true
  def process({topic, id}) do
    event = EventBus.fetch_event({topic, id})
    IO.inspect(event.data, label: "#{__MODULE__} received #{topic}")

    EventBus.mark_as_completed({__MODULE__, {topic, id}})
  end
end
