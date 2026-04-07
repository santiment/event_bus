defmodule EventBus.Telemetry do
  @moduledoc """
  Telemetry integration for EventBus.

  When the `:telemetry` library is available, EventBus emits the following events:

    * `[:event_bus, :notify, :start]` — before dispatching to subscribers
      * Measurements: `%{system_time: integer()}`
      * Metadata: `%{topic: atom(), event_id: term()}`

    * `[:event_bus, :notify, :stop]` — after all subscribers have been notified
      * Measurements: `%{duration: integer()}`
      * Metadata: `%{topic: atom(), event_id: term(), subscriber_count: integer()}`

    * `[:event_bus, :notify, :exception]` — when a subscriber raises
      * Measurements: `%{duration: integer()}`
      * Metadata: `%{topic: atom(), event_id: term(), subscriber: term(),
                     kind: :error, reason: term(), stacktrace: list()}`

    * `[:event_bus, :observation, :complete]` — when all subscribers finish and event is cleaned up
      * Measurements: `%{subscriber_count: integer()}`
      * Metadata: `%{topic: atom(), event_id: term(), completers: list(), skippers: list()}`

    * `[:event_bus, :sweep, :cycle]` — after a sweep cycle that expired at least one event
      * Measurements: `%{expired_count: integer(), duration: integer()}`
      * Metadata: `%{}`

  If `:telemetry` is not available, all calls are no-ops.
  """

  @telemetry_loaded Code.ensure_loaded?(:telemetry)

  if @telemetry_loaded do
    @doc false
    def execute(event_name, measurements, metadata) do
      :telemetry.execute(event_name, measurements, metadata)
    end
  else
    @doc false
    def execute(_event_name, _measurements, _metadata), do: :ok
  end
end
