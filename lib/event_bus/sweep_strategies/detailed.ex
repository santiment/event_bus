defmodule EventBus.SweepStrategy.Detailed do
  @moduledoc """
  Per-event sweep strategy with individual telemetry.

  Each expired event is processed via `Observation.force_expire/1` and gets its
  own `[:event_bus, :sweep, :expired]` telemetry event with age and pending
  subscriber details. Useful when you need per-event visibility into expirations.

  Slower under high expiration volume due to per-event ETS lookups and GenServer
  calls for limited-subscription accounting.
  """

  @behaviour EventBus.SweepStrategy

  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Telemetry

  @impl true
  def init, do: nil

  @impl true
  def handle_batch(batch, state) do
    count =
      Enum.count(batch, fn {topic, id, inserted_at} ->
        expire_event({topic, id}, inserted_at)
      end)

    {count, state}
  end

  @impl true
  def telemetry_metadata(_state), do: %{}

  defp expire_event({topic, id}, inserted_at) do
    case ObservationService.force_expire({topic, id}) do
      {:ok, info} ->
        pending = info.subscribers -- (info.completers ++ info.skippers)
        age = System.monotonic_time() - inserted_at

        Telemetry.execute(
          [:event_bus, :sweep, :expired],
          %{age: age},
          %{
            topic: topic,
            event_id: id,
            pending_subscribers: pending
          }
        )

        true

      :not_found ->
        false
    end
  end
end
