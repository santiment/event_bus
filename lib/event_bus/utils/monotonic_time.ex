defmodule EventBus.Util.MonotonicTime do
  @moduledoc false

  @eb_app :event_bus

  @doc """
  Compute and cache the monotonic offset for the configured time unit.

  Called once at application start so concurrent first readers cannot race
  to compute (and cache) slightly different offsets, which would break the
  monotonicity guarantee of `now/0` across processes.
  """
  @spec init() :: :ok
  def init() do
    save_init_time(configured_time_unit())
    :ok
  end

  @doc """
  Calculates monotonically increasing current time.
  """
  @spec now() :: integer()
  def now() do
    time_unit = configured_time_unit()
    init_time(time_unit) + System.monotonic_time(time_unit)
  end

  defp configured_time_unit() do
    Application.get_env(@eb_app, :time_unit, :microsecond)
  end

  defp init_time(time_unit) do
    case Application.get_env(@eb_app, :init_time) do
      {^time_unit, time} -> time
      _ -> save_init_time(time_unit)
    end
  end

  defp save_init_time(time_unit) do
    time = System.os_time(time_unit) - System.monotonic_time(time_unit)

    Application.put_env(@eb_app, :init_time, {time_unit, time},
      persistent: true
    )

    time
  end
end
