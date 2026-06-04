defmodule EventBus.Service.Sweeper do
  @moduledoc false

  use GenServer

  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Telemetry

  @default_sweep_interval 10_000
  @default_batch_size 100

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def init(_opts) do
    ttl_ms = Application.get_env(:event_bus, :event_ttl)

    if !(is_integer(ttl_ms) and ttl_ms > 0) do
      raise ArgumentError,
            "EventBus.Service.Sweeper requires :event_ttl to be a positive integer (milliseconds), got: #{inspect(ttl_ms)}"
    end

    interval =
      Application.get_env(:event_bus, :sweep_interval, @default_sweep_interval)

    batch_size =
      Application.get_env(:event_bus, :sweep_batch_size, @default_batch_size)

    strategy =
      resolve_strategy(
        Application.get_env(:event_bus, :sweep_strategy, :bulk_smart)
      )

    ttl_native = System.convert_time_unit(ttl_ms, :millisecond, :native)

    schedule_sweep(interval)

    {:ok,
     %{
       ttl_native: ttl_native,
       interval: interval,
       batch_size: batch_size,
       strategy: strategy
     }}
  end

  @doc false
  def handle_info(:sweep, state) do
    do_sweep(state.ttl_native, state.batch_size, state.strategy)
    schedule_sweep(state.interval)
    {:noreply, state}
  end

  @doc """
  Run a sweep with the given TTL in native time units.

  Options:

    * `:strategy` — a strategy module implementing `EventBus.SweepStrategy`,
      or a shorthand atom (`:bulk_smart`, `:detailed`). Defaults to the
      application-level `:sweep_strategy` config, which defaults to `:bulk_smart`.

    * `:batch_size` — number of events per cursor chunk. Defaults to the
      application-level `:sweep_batch_size` config, which defaults to `100`.

  Returns the number of expired events cleaned up.
  """
  @spec sweep(integer(), keyword()) :: non_neg_integer()
  def sweep(ttl_native, opts \\ []) do
    strategy =
      Keyword.get_lazy(opts, :strategy, fn ->
        Application.get_env(:event_bus, :sweep_strategy, :bulk_smart)
      end)
      |> resolve_strategy()

    batch_size =
      Keyword.get_lazy(opts, :batch_size, fn ->
        Application.get_env(:event_bus, :sweep_batch_size, @default_batch_size)
      end)

    do_sweep(ttl_native, batch_size, strategy)
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end

  defp do_sweep(ttl_native, batch_size, strategy) do
    start_time = System.monotonic_time()
    cutoff = start_time - ttl_native

    state = strategy.init()

    {expired_count, final_state} =
      StoreService.select_expired(cutoff, batch_size)
      |> expire_in_batches(strategy, state, 0)

    if expired_count > 0 do
      duration = System.monotonic_time() - start_time
      metadata = strategy.telemetry_metadata(final_state)

      Telemetry.execute(
        [:event_bus, :sweep, :cycle],
        %{expired_count: expired_count, duration: duration},
        metadata
      )
    end

    expired_count
  end

  defp expire_in_batches(:done, _strategy, state, count), do: {count, state}

  defp expire_in_batches({batch, continuation}, strategy, state, count) do
    {batch_count, new_state} = strategy.handle_batch(batch, state)

    StoreService.continue_expired(continuation)
    |> expire_in_batches(strategy, new_state, count + batch_count)
  end

  defp resolve_strategy(:bulk_smart), do: EventBus.SweepStrategy.BulkSmart
  defp resolve_strategy(:detailed), do: EventBus.SweepStrategy.Detailed
  defp resolve_strategy(module) when is_atom(module), do: module
end
