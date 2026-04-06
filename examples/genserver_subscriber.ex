# A GenServer-based subscriber that handles events asynchronously.
#
# The process/1 callback casts to itself and returns immediately,
# so the event bus dispatch is not blocked. The actual processing
# and mark_as_completed happen in handle_cast.
#
# Usage:
#   GenServerSubscriber.start_link()
#   EventBus.register_topic(:order_created)
#   EventBus.subscribe({GenServerSubscriber, ["order_.*"]})
#
defmodule GenServerSubscriber do
  use GenServer

  @behaviour EventBus.Subscriber

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts), do: {:ok, opts}

  # EventBus calls this — hand off to GenServer immediately.
  @impl EventBus.Subscriber
  def process({topic, id}) do
    GenServer.cast(__MODULE__, {topic, id})
  end

  # Handle specific topics
  @impl GenServer
  def handle_cast({:order_created, id}, state) do
    event = EventBus.fetch_event({:order_created, id})
    # ... do work with event.data ...

    EventBus.mark_as_completed({__MODULE__, {:order_created, id}})
    {:noreply, state}
  end

  # Skip unrecognized topics
  def handle_cast({topic, id}, state) do
    EventBus.mark_as_skipped({__MODULE__, {topic, id}})
    {:noreply, state}
  end
end
