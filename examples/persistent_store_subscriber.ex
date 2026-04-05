# A subscriber that persists every event to a data store.
#
# Subscribe with [".*"] to receive all event topics.
# Replace the save_to_store/1 call with your actual persistence logic.
#
# Usage:
#   PersistentStoreSubscriber.start_link()
#   EventBus.subscribe({PersistentStoreSubscriber, [".*"]})
#
defmodule PersistentStoreSubscriber do
  use GenServer

  @behaviour EventBus.Subscriber

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts), do: {:ok, opts}

  @impl EventBus.Subscriber
  def process({topic, id}) do
    GenServer.cast(__MODULE__, {topic, id})
  end

  @impl GenServer
  def handle_cast({topic, id}, state) do
    event = EventBus.fetch_event({topic, id})
    save_to_store(event)

    EventBus.mark_as_completed({__MODULE__, {topic, id}})
    {:noreply, state}
  end

  defp save_to_store(event) do
    # Replace with your persistence logic:
    #   MyRepo.insert(%EventRecord{
    #     topic: event.topic,
    #     event_id: event.id,
    #     data: event.data,
    #     occurred_at: event.occurred_at
    #   })
    IO.inspect(event, label: "persisting")
  end
end
