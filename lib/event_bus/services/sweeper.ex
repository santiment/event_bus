defmodule EventBus.Service.Sweeper do
  @moduledoc false

  use GenServer

  alias EventBus.Manager.Subscription, as: SubscriptionManager
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
    mode = Application.get_env(:event_bus, :sweep_mode, :bulk_smart)
    ttl_native = System.convert_time_unit(ttl_ms, :millisecond, :native)

    schedule_sweep(interval)
    {:ok, %{ttl_native: ttl_native, interval: interval, mode: mode}}
  end

  @doc false
  def handle_info(:sweep, state) do
    do_sweep(state.ttl_native, state.mode)
    schedule_sweep(state.interval)
    {:noreply, state}
  end

  @doc """
  Run a sweep with the given TTL in native time units.

  Options:

    * `:mode` — `:bulk_smart` (default) or `:detailed`. Falls back to the
      application-level `:sweep_mode` config when not provided.

  Returns the number of expired events cleaned up.
  """
  @spec sweep(integer(), keyword()) :: non_neg_integer()
  def sweep(ttl_native, opts \\ []) do
    mode =
      Keyword.get_lazy(opts, :mode, fn ->
        Application.get_env(:event_bus, :sweep_mode, :bulk_smart)
      end)

    do_sweep(ttl_native, mode)
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end

  defp do_sweep(ttl_native, mode) do
    start_time = System.monotonic_time()
    cutoff = start_time - ttl_native
    initial = StoreService.select_expired(cutoff, @batch_size)

    case mode do
      :bulk_smart -> sweep_bulk_smart(initial, start_time)
      :detailed -> sweep_detailed(initial, start_time)
    end
  end

  # ---------------------------------------------------------------------------
  # bulk_smart: batched expire, one telemetry event with per-topic counts
  # ---------------------------------------------------------------------------

  defp sweep_bulk_smart(initial, start_time) do
    limited_set = SubscriptionManager.limited_subscribers()

    {expired_count, topic_counts} =
      expire_batches_bulk(initial, limited_set, {0, %{}})

    if expired_count > 0 do
      duration = System.monotonic_time() - start_time

      Telemetry.execute(
        [:event_bus, :sweep, :cycle],
        %{expired_count: expired_count, duration: duration},
        %{expired_per_topic: topic_counts}
      )
    end

    expired_count
  end

  defp expire_batches_bulk(:done, _limited_set, acc), do: acc

  defp expire_batches_bulk({batch, continuation}, limited_set, {count, topic_counts}) do
    event_shadows = Enum.map(batch, fn {topic, id, _inserted_at} -> {topic, id} end)
    {batch_count, batch_topics} = ObservationService.expire_batch(event_shadows, limited_set)

    merged = Map.merge(topic_counts, batch_topics, fn _k, v1, v2 -> v1 + v2 end)

    StoreService.continue_expired(continuation)
    |> expire_batches_bulk(limited_set, {count + batch_count, merged})
  end

  # ---------------------------------------------------------------------------
  # detailed: per-event force_expire + per-event telemetry
  # ---------------------------------------------------------------------------

  defp sweep_detailed(initial, start_time) do
    expired_count = expire_batches_detailed(initial, 0)

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

  defp expire_batches_detailed(:done, count), do: count

  defp expire_batches_detailed({batch, continuation}, count) do
    batch_expired =
      Enum.count(batch, fn {topic, id, inserted_at} ->
        expire_event_detailed({topic, id}, inserted_at)
      end)

    StoreService.continue_expired(continuation)
    |> expire_batches_detailed(count + batch_expired)
  end

  defp expire_event_detailed({topic, id}, inserted_at) do
    case ObservationService.force_expire({topic, id}) do
      {:ok, info} ->
        pending = info.subscribers -- info.completers ++ info.skippers
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
