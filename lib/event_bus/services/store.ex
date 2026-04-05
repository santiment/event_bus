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
    :ets.match_delete(@table, {{topic, :_}, :_})
    :ok
  end

  @doc false
  @spec fetch(event_shadow()) :: event() | nil
  def fetch({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
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
  @spec delete(event_shadow()) :: :ok
  def delete({topic, id}) do
    :ets.delete(@table, {topic, id})
    :ok
  end

  @doc false
  @spec create(event()) :: :ok
  def create(%Event{id: id, topic: topic} = event) do
    :ets.insert(@table, {{topic, id}, event})
    :ok
  end
end
