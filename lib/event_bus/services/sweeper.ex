defmodule EventBus.Service.Sweeper do
  @moduledoc false

  use GenServer

  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Telemetry

  @default_sweep_interval 10_000
  @batch_size 100

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def init(_opts) do
    ttl_ms = Application.fetch_env!(:event_bus, :event_ttl)
    interval = Application.get_env(:event_bus, :sweep_interval, @default_sweep_interval)
    ttl_native = System.convert_time_unit(ttl_ms, :millisecond, :native)

    schedule_sweep(interval)
    {:ok, %{ttl_native: ttl_native, interval: interval}}
  end

  @doc false
  def handle_info(:sweep, state) do
    do_sweep(state.ttl_native)
    schedule_sweep(state.interval)
    {:noreply, state}
  end

  @doc """
  Run a sweep with the given TTL in native time units.
  Returns the number of expired events cleaned up.
  """
  @spec sweep(integer()) :: non_neg_integer()
  def sweep(ttl_native) do
    do_sweep(ttl_native)
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end

  defp do_sweep(ttl_native) do
    start_time = System.monotonic_time()
    cutoff = start_time - ttl_native

    expired_count =
      StoreService.select_expired(cutoff, @batch_size)
      |> expire_in_batches(0)

    if expired_count > 0 do
      duration = System.monotonic_time() - start_time

      Telemetry.execute(
        [:event_bus, :sweep, :cycle],
        %{expired_count: expired_count, duration: duration},
        %{}
      )
    end

    expired_count
  end

  defp expire_in_batches(:done, count), do: count

  defp expire_in_batches({batch, continuation}, count) do
    event_shadows = Enum.map(batch, fn {topic, id, _inserted_at} -> {topic, id} end)
    batch_expired = ObservationService.expire_batch(event_shadows)

    StoreService.continue_expired(continuation)
    |> expire_in_batches(count + batch_expired)
  end
end
