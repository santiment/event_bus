# A subscriber that receives per-instance configuration.
#
# When subscribing with a {Module, config} tuple, the process/1 callback
# receives {config, topic, id} instead of {topic, id}.
# This allows multiple instances of the same module with different configs.
#
# Usage:
#   EventBus.register_topic(:order_created)
#   EventBus.subscribe({{ConfiguredSubscriber, %{region: "us"}}, ["order_.*"]})
#   EventBus.subscribe({{ConfiguredSubscriber, %{region: "eu"}}, ["order_.*"]})
#
defmodule ConfiguredSubscriber do
  @behaviour EventBus.Subscriber

  @impl true
  def process({config, topic, id}) do
    event = EventBus.fetch_event({topic, id})
    IO.puts("#{config.region}: processing #{topic} #{id}")

    # Use the {Module, config} tuple as subscriber identity
    subscriber = {__MODULE__, config}
    EventBus.mark_as_completed({subscriber, {topic, id}})
  end
end
