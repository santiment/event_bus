defmodule EventBus.Service.Observation do
  @moduledoc false

  require Logger

  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Service.Debug
  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Telemetry

  @typep event_shadow :: EventBus.event_shadow()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_event_ref :: EventBus.subscriber_with_event_ref()
  @typep topic :: EventBus.topic()

  @table :eb_event_watchers
  # Per-subscriber terminal status for lock-free atomic transitions.
  # Keys are {topic, id, subscriber}, values are :pending | :completed | :skipped.
  @status_table :eb_event_watcher_status
  # Stores the per-subscriber subscription generation captured at dispatch time
  # for each event_shadow. Observation consults this snapshot before consuming a
  # subscribe_once/subscribe_n counter.
  @snapshot_table :eb_event_subscription_generations
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
    for table <- [@table, @status_table, @snapshot_table] do
      if :ets.info(table) == :undefined do
        :ets.new(table, @table_opts)
      end
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
    :ets.match_delete(@status_table, {{topic, :_, :_}, :_})
    :ets.match_delete(@snapshot_table, {{topic, :_}, :_})
    :ok
  end

  @doc false
  @spec mark_as_completed(subscriber_with_event_ref()) :: :ok
  def mark_as_completed({subscriber, {topic, id} = event_shadow}) do
    # Atomic CAS: transition :pending -> :completed only if currently :pending.
    # select_replace returns the number of replaced objects (0 or 1).
    # The {:const, key} wrapper is required because bare tuples in match spec
    # guards are interpreted as function calls, not literal values.
    case cas_status({topic, id, subscriber}, :pending, :completed) do
      1 ->
        Debug.log_terminal("completed", subscriber, topic, id)
        decrement_limit(subscriber, event_shadow)
        check_completion({topic, id})

      0 ->
        :ok
    end
  end

  @doc false
  @spec mark_as_skipped(subscriber_with_event_ref()) :: :ok
  def mark_as_skipped({subscriber, {topic, id} = event_shadow}) do
    case cas_status({topic, id, subscriber}, :pending, :skipped) do
      1 ->
        Debug.log_terminal("skipped", subscriber, topic, id)
        decrement_limit(subscriber, event_shadow)
        check_completion({topic, id})

      0 ->
        :ok
    end
  end

  @doc false
  @spec fetch(event_shadow()) ::
          {subscribers(), subscribers(), subscribers()} | nil
  def fetch({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{{^topic, ^id}, subscribers, _remaining}] ->
        {completers, skippers} = collect_terminal(topic, id, subscribers)
        {subscribers, completers, skippers}

      _ ->
        Logger.log(:info, fn ->
          "[EVENTBUS][OBSERVATION]\s#{topic}.#{id}.ets_fetch_error"
        end)

        nil
    end
  end

  @doc false
  @spec save(event_shadow(), {subscribers(), list(), list()}) :: :ok
  def save({topic, id}, {subscribers, [], []}) do
    count = length(subscribers)
    :ets.insert(@table, {{topic, id}, subscribers, count})

    Enum.each(subscribers, fn sub ->
      :ets.insert(@status_table, {{topic, id, sub}, :pending})
    end)

    :ok
  end

  @doc false
  @spec save_snapshot(event_shadow(), %{optional(EventBus.subscriber()) => non_neg_integer()}) ::
          :ok
  def save_snapshot({topic, id}, snapshot) do
    :ets.insert(@snapshot_table, {{topic, id}, snapshot})
    :ok
  end

  # Atomically decrement the remaining counter.
  # Only the process that decrements to 0 runs the cleanup.
  @spec check_completion(event_shadow()) :: :ok
  defp check_completion({topic, id}) do
    case :ets.update_counter(@table, {topic, id}, {3, -1}) do
      0 -> on_complete({topic, id})
      _ -> :ok
    end
  rescue
    # Watcher already cleaned up (e.g., topic unregistered concurrently)
    ArgumentError -> :ok
  end

  @spec on_complete(event_shadow()) :: :ok
  defp on_complete({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{{^topic, ^id}, subscribers, _}] ->
        {completers, skippers} = collect_terminal(topic, id, subscribers)

        Debug.log("cleaned topic=#{inspect(topic)} id=#{inspect(id)}")
        Debug.clean_dispatch_metadata(topic, id)

        Telemetry.execute(
          [:event_bus, :observation, :complete],
          %{subscriber_count: length(subscribers)},
          %{
            topic: topic,
            event_id: id,
            completers: completers,
            skippers: skippers
          }
        )

        # Delete all entries for this event
        :ets.delete(@table, {topic, id})

        Enum.each(subscribers, fn sub ->
          :ets.delete(@status_table, {topic, id, sub})
        end)

        :ets.delete(@snapshot_table, {topic, id})

        StoreService.delete({topic, id})

      _ ->
        :ok
    end

    :ok
  end

  @doc """
  Force-expire a single event, cleaning up all observation, store, and snapshot
  state. Decrements limited subscription counters for pending subscribers via a
  single batch GenServer call.

  Returns `{:ok, info}` with subscriber details, or `:not_found` if the event
  was already cleaned up (e.g., by normal completion).
  """
  @spec force_expire(event_shadow()) ::
          {:ok,
           %{
             subscribers: subscribers(),
             completers: subscribers(),
             skippers: subscribers()
           }}
          | :not_found
  def force_expire({topic, id}) do
    case :ets.lookup(@table, {topic, id}) do
      [{{^topic, ^id}, subscribers, _}] ->
        {completers, skippers} = collect_terminal(topic, id, subscribers)
        pending = subscribers -- completers ++ skippers

        # One GenServer call for all pending subscribers in this event.
        batch_decrement_limits(pending, {topic, id})

        Debug.log("expired topic=#{inspect(topic)} id=#{inspect(id)}")
        Debug.clean_dispatch_metadata(topic, id)

        :ets.delete(@table, {topic, id})
        :ets.match_delete(@status_table, {{topic, id, :_}, :_})
        :ets.delete(@snapshot_table, {topic, id})
        StoreService.delete({topic, id})

        {:ok, %{subscribers: subscribers, completers: completers, skippers: skippers}}

      _ ->
        :not_found
    end
  end

  @doc """
  Expire a batch of event shadows in a single pass.

  For each event, collects pending subscribers that need limit decrements,
  then makes ONE GenServer call for the entire batch. Events already cleaned
  up by normal completion are skipped. Returns the number of events expired.
  """
  @spec expire_batch([event_shadow()]) :: non_neg_integer()
  def expire_batch(event_shadows) do
    # Phase 1: Read watcher entries and collect pending decrements.
    {decrements, to_delete} =
      Enum.reduce(event_shadows, {[], []}, fn {topic, id} = shadow, {dec_acc, del_acc} ->
        case :ets.lookup(@table, {topic, id}) do
          [{{^topic, ^id}, subscribers, _}] ->
            pending_decs = collect_pending_decrements(topic, id, subscribers)
            {pending_decs ++ dec_acc, [shadow | del_acc]}

          _ ->
            # Already cleaned up by normal completion
            {dec_acc, del_acc}
        end
      end)

    # Phase 2: One GenServer call for ALL limit decrements across the batch.
    SubscriptionManager.decrement_limits(decrements)

    # Phase 3: Delete from all ETS tables.
    Enum.each(to_delete, fn {topic, id} ->
      :ets.delete(@table, {topic, id})
      :ets.match_delete(@status_table, {{topic, id, :_}, :_})
      :ets.delete(@snapshot_table, {topic, id})
      Debug.clean_dispatch_metadata(topic, id)
      StoreService.delete({topic, id})
    end)

    length(to_delete)
  end

  # Build [{subscriber, generation}, ...] for pending subscribers only.
  @spec collect_pending_decrements(topic(), EventBus.event_id(), subscribers()) ::
          [{EventBus.subscriber(), non_neg_integer()}]
  defp collect_pending_decrements(topic, id, subscribers) do
    Enum.flat_map(subscribers, fn sub ->
      case :ets.lookup(@status_table, {topic, id, sub}) do
        [{_, :pending}] -> [{sub, snapshot_generation({topic, id}, sub)}]
        _ -> []
      end
    end)
  end

  defp batch_decrement_limits([], _event_shadow), do: :ok

  defp batch_decrement_limits(pending, {topic, id}) do
    subscriber_generations =
      Enum.map(pending, fn sub -> {sub, snapshot_generation({topic, id}, sub)} end)

    SubscriptionManager.decrement_limits(subscriber_generations)
  end

  # Reconstruct completers/skippers lists from per-subscriber status entries.
  @spec collect_terminal(topic(), EventBus.event_id(), subscribers()) ::
          {subscribers(), subscribers()}
  defp collect_terminal(topic, id, subscribers) do
    Enum.reduce(subscribers, {[], []}, fn sub, {comps, skips} ->
      case :ets.lookup(@status_table, {topic, id, sub}) do
        [{_, :completed}] -> {[sub | comps], skips}
        [{_, :skipped}] -> {comps, [sub | skips]}
        _ -> {comps, skips}
      end
    end)
  end

  defp decrement_limit(subscriber, event_shadow) do
    generation = snapshot_generation(event_shadow, subscriber)
    SubscriptionManager.decrement_limit(subscriber, generation)
  end

  # Atomic compare-and-swap on the status table.
  # Transitions the entry from `expected` to `new_status` if and only if
  # the current value matches `expected`. Returns 1 on success, 0 otherwise.
  # Uses {:const, key} in the guard because bare tuples in match spec
  # guards/bodies are interpreted as function calls, not literal values.
  @spec cas_status(term(), atom(), atom()) :: 0 | 1
  defp cas_status(key, expected, new_status) do
    :ets.select_replace(@status_table, [
      {{:"$1", expected}, [{:==, :"$1", {:const, key}}],
       [{{:"$1", new_status}}]}
    ])
  end

  defp snapshot_generation({topic, id}, subscriber) do
    case :ets.lookup(@snapshot_table, {topic, id}) do
      [{{^topic, ^id}, snapshot}] -> Map.get(snapshot, subscriber, 0)
      _ -> 0
    end
  end
end
