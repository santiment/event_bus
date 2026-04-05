defmodule EventBus.Service.Debug do
  @moduledoc false

  require Logger

  @app :event_bus
  @dispatch_table :eb_dispatch_metadata

  @doc false
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(@app, :debug, false) == true
  end

  @doc false
  @spec toggle(boolean()) :: :ok
  def toggle(enabled) when is_boolean(enabled) do
    Application.put_env(@app, :debug, enabled, persistent: true)

    if enabled do
      Logger.put_module_level(__MODULE__, :debug)
    else
      Logger.delete_module_level(__MODULE__)
    end

    :ok
  end

  @doc false
  @spec setup_table() :: :ok
  def setup_table do
    if :ets.info(@dispatch_table) == :undefined do
      :ets.new(@dispatch_table, [
        :set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])
    end

    :ok
  end

  @doc false
  @spec record_dispatch(term(), atom(), term()) :: :ok
  def record_dispatch(subscriber, topic, id) do
    if enabled?() do
      :ets.insert(@dispatch_table, {{subscriber, topic, id}, System.monotonic_time()})
    end

    :ok
  end

  @doc false
  @spec fetch_and_clear_dispatch_time(term(), atom(), term()) ::
          {:ok, integer()} | :not_found
  def fetch_and_clear_dispatch_time(subscriber, topic, id) do
    key = {subscriber, topic, id}

    case :ets.lookup(@dispatch_table, key) do
      [{^key, start_time}] ->
        :ets.delete(@dispatch_table, key)
        {:ok, start_time}

      _ ->
        :not_found
    end
  end

  @doc false
  @spec clean_dispatch_metadata(atom(), term()) :: :ok
  def clean_dispatch_metadata(topic, id) do
    :ets.match_delete(@dispatch_table, {{:_, topic, id}, :_})
    :ok
  end

  @doc false
  @spec log(String.t()) :: :ok
  def log(message) do
    if enabled?() do
      Logger.debug("[EventBus] #{message}")
    end

    :ok
  end

  @doc false
  @spec log_terminal(String.t(), term(), atom(), term()) :: :ok
  def log_terminal(action, subscriber, topic, id) do
    if enabled?() do
      duration_str =
        case fetch_and_clear_dispatch_time(subscriber, topic, id) do
          {:ok, start_time} ->
            duration_us = System.convert_time_unit(
              System.monotonic_time() - start_time,
              :native,
              :microsecond
            )

            " duration=#{format_duration(duration_us)}"

          :not_found ->
            ""
        end

      Logger.debug(
        "[EventBus] #{action} topic=#{inspect(topic)} id=#{inspect(id)} subscriber=#{inspect(subscriber)}#{duration_str}"
      )
    end

    :ok
  end

  defp format_duration(us) when us >= 1_000_000 do
    "#{Float.round(us / 1_000_000, 1)}s"
  end

  defp format_duration(us) when us >= 1_000 do
    "#{Float.round(us / 1_000, 1)}ms"
  end

  defp format_duration(us) do
    "#{us}µs"
  end
end
