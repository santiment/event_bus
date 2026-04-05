defmodule EventBus.Service.Observation do
  @moduledoc false

  require Logger

  alias EventBus.Manager.Store, as: StoreManager
  alias EventBus.Service.Debug
  alias EventBus.Service.Subscription, as: SubscriptionService

  @typep event_shadow :: EventBus.event_shadow()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_event_ref :: EventBus.subscriber_with_event_ref()
  @typep topic :: EventBus.topic()
  @typep watcher :: {subscribers(), subscribers(), subscribers()}

  @table :eb_event_watchers
  @table_opts [
    :set,
    :public,
    :named_table,
    {:write_concurrency, true},
    {:read_concurrency, true}
  ]

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
  @spec mark_as_completed(subscriber_with_event_ref()) :: :ok
  def mark_as_completed({subscriber, {topic, id} = event_shadow}) do
    case fetch(event_shadow) do
      {subscribers, completers, skippers} ->
        if subscriber in completers or subscriber in skippers do
          :ok
        else
          Debug.log_terminal("completed", subscriber, topic, id)
          SubscriptionService.decrement_limit(subscriber)

          save_or_delete(
            event_shadow,
            {subscribers, [subscriber | completers], skippers}
          )
        end

      nil ->
        :ok
    end
  end

  @doc false
  @spec mark_as_skipped(subscriber_with_event_ref()) :: :ok
  def mark_as_skipped({subscriber, {topic, id} = event_shadow}) do
    case fetch(event_shadow) do
      {subscribers, completers, skippers} ->
        if subscriber in completers or subscriber in skippers do
          :ok
        else
          Debug.log_terminal("skipped", subscriber, topic, id)
          SubscriptionService.decrement_limit(subscriber)

          save_or_delete(
            event_shadow,
            {subscribers, completers, [subscriber | skippers]}
          )
        end

      nil ->
        :ok
    end
  end

  @doc false
  @spec fetch(event_shadow()) ::
          {subscribers(), subscribers(), subscribers()} | nil
  def fetch({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{_, data}] ->
        data

      _ ->
        Logger.log(:info, fn ->
          "[EVENTBUS][OBSERVATION]\s#{topic}.#{id}.ets_fetch_error"
        end)

        nil
    end
  end

  @doc false
  @spec save(event_shadow(), watcher()) :: :ok
  def save({topic, id}, watcher) do
    save_or_delete({topic, id}, watcher)
  end

  @spec complete?(watcher()) :: boolean()
  defp complete?({subscribers, completers, skippers}) do
    length(subscribers) == length(completers) + length(skippers)
  end

  @spec save_or_delete(event_shadow(), watcher()) :: :ok
  defp save_or_delete({topic, id}, watcher) do
    if complete?(watcher) do
      delete_with_relations({topic, id})
    else
      :ets.insert(@table, {{topic, id}, watcher})
    end

    :ok
  end

  @spec delete_with_relations(event_shadow()) :: :ok
  defp delete_with_relations({topic, id}) do
    Debug.log("cleaned topic=#{inspect(topic)} id=#{inspect(id)}")
    Debug.clean_dispatch_metadata(topic, id)
    StoreManager.delete({topic, id})
    :ets.delete(@table, {topic, id})

    :ok
  end
end
