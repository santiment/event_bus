defmodule EventBus.Service.Store do
  @moduledoc false

  require Logger

  alias EventBus.Model.Event

  @typep event :: EventBus.event()
  @typep event_shadow :: EventBus.event_shadow()
  @typep topic :: EventBus.topic()

  @table :eb_event_store
  @table_opts [:set, :public, :named_table, {:read_concurrency, true}]

  @doc false
  @spec setup_table() :: :ok
  def setup_table do
    if :ets.info(@table) == :undefined do
      :ets.new(@table, @table_opts)
    end

    :ok
  end

  @doc false
  @spec table_name() :: atom()
  def table_name, do: @table

  @doc false
  @spec register_topic(topic()) :: :ok
  def register_topic(_topic), do: :ok

  @doc false
  @spec unregister_topic(topic()) :: :ok
  def unregister_topic(topic) do
    :ets.match_delete(@table, {{topic, :_}, :_, :_})
    :ok
  end

  @doc false
  @spec fetch(event_shadow()) :: event() | nil
  def fetch({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{_, %Event{} = event, _metadata}] ->
        event

      [{_, %Event{} = event}] ->
        event

      _ ->
        Logger.log(:info, fn ->
          "[EVENTBUS][STORE]\s#{topic}.#{id}.ets_fetch_error"
        end)

        nil
    end
  end

  @doc false
  @spec fetch_data(event_shadow()) :: any()
  def fetch_data({topic, id}) do
    event = fetch({topic, id}) || %{}
    Map.get(event, :data)
  end

  @doc false
  @spec fetch_metadata(event_shadow()) :: map() | nil
  def fetch_metadata({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{_, _event, metadata}] -> metadata
      _ -> nil
    end
  end

  @doc false
  @spec delete(event_shadow()) :: :ok
  def delete({topic, id}) do
    :ets.delete(@table, {topic, id})
    :ok
  end

  @doc false
  @spec expired_match_spec(integer()) :: :ets.match_spec()
  defp expired_match_spec(cutoff) do
    [
      {{{:"$1", :"$2"}, :_, %{inserted_at: :"$3"}}, [{:<, :"$3", cutoff}],
       [{{:"$1", :"$2", :"$3"}}]}
    ]
  end

  @doc """
  Begin a cursor-based scan for events older than `cutoff`.

  Returns `{batch, continuation}` or `:done`. Each batch element is a
  3-tuple `{topic, id, inserted_at}`. Call `continue_expired/1` with the
  continuation to fetch the next batch.
  """
  @spec select_expired(integer(), pos_integer()) ::
          {[{atom(), term(), integer()}], term()} | :done
  def select_expired(cutoff, batch_size) do
    wrap_select_result(
      :ets.select(@table, expired_match_spec(cutoff), batch_size)
    )
  end

  @doc """
  Continue a cursor-based expired-event scan started by `select_expired/2`.
  """
  @spec continue_expired(term()) ::
          {[{atom(), term(), integer()}], term()} | :done
  def continue_expired(continuation) do
    wrap_select_result(:ets.select(continuation))
  end

  defp wrap_select_result(:"$end_of_table"), do: :done
  defp wrap_select_result({results, continuation}), do: {results, continuation}

  @doc false
  # Return all expired events at once. Suitable for tests and small result sets.
  # For production sweeps, use `select_expired/2` + `continue_expired/1`.
  @spec find_expired(integer()) :: [{event_shadow(), integer()}]
  def find_expired(cutoff) do
    :ets.select(@table, expired_match_spec(cutoff))
    |> Enum.map(fn {topic, id, inserted_at} -> {{topic, id}, inserted_at} end)
  end

  @doc false
  @spec create(event()) :: :ok
  def create(%Event{id: id, topic: topic} = event) do
    metadata = %{inserted_at: System.monotonic_time()}
    :ets.insert(@table, {{topic, id}, event, metadata})
    :ok
  end
end
