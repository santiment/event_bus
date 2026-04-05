defmodule EventBus.Service.Subscription do
  @moduledoc false

  alias EventBus.Service.Debug
  alias EventBus.Util.Regex, as: RegexUtil

  @subscribers_table :eb_subscribers
  @topic_map_table :eb_topic_subscribers
  @limits_table :eb_subscription_limits
  @opts_table :eb_subscription_opts

  @typep subscriber :: EventBus.subscriber()
  @typep subscribers :: EventBus.subscribers()
  @typep subscriber_with_topic_patterns ::
           EventBus.subscriber_with_topic_patterns()
  @typep topic :: EventBus.topic()

  @ets_opts [
    :set,
    :public,
    :named_table,
    {:write_concurrency, true},
    {:read_concurrency, true}
  ]

  @spec subscribed?(subscriber_with_topic_patterns()) :: boolean()
  def subscribed?(subscriber) do
    Enum.member?(subscribers(), subscriber)
  end

  @doc false
  @spec setup_tables() :: :ok
  def setup_tables do
    for table <- [@subscribers_table, @topic_map_table, @limits_table, @opts_table] do
      if :ets.info(table) == :undefined do
        :ets.new(table, @ets_opts)
      end
    end

    :ok
  end

  @doc false
  @spec subscribe(subscriber_with_topic_patterns()) :: :ok
  def subscribe({subscriber, topics}) do
    Debug.log("subscribe subscriber=#{inspect(subscriber)} patterns=#{inspect(topics)}")
    clear_limit(subscriber)
    clear_opts(subscriber)

    :ets.insert(@subscribers_table, {subscriber, topics})
    rebuild_topic_map_for_subscriber(subscriber, topics)

    :ok
  end

  @doc false
  @spec subscribe(subscriber_with_topic_patterns(), keyword()) :: :ok
  def subscribe({subscriber, topics}, opts) when is_list(opts) do
    subscribe({subscriber, topics})
    store_opts(subscriber, opts)
    :ok
  end

  @doc false
  @spec subscribe_n(subscriber_with_topic_patterns(), pos_integer()) :: :ok
  def subscribe_n({subscriber, topics}, count) when is_integer(count) and count > 0 do
    subscribe({subscriber, topics})
    :ets.insert(@limits_table, {subscriber, count})
    :ok
  end

  @doc false
  @spec decrement_limit(term()) :: :ok
  def decrement_limit(subscriber) do
    case :ets.lookup(@limits_table, subscriber) do
      [{^subscriber, count}] when count <= 1 ->
        :ets.delete(@limits_table, subscriber)
        unsubscribe(subscriber)

      [{^subscriber, count}] ->
        :ets.insert(@limits_table, {subscriber, count - 1})

      [] ->
        :ok
    end

    :ok
  end

  @doc false
  @spec unsubscribe(subscriber()) :: :ok
  def unsubscribe(subscriber) do
    Debug.log("unsubscribe subscriber=#{inspect(subscriber)}")
    clear_limit(subscriber)
    clear_opts(subscriber)

    :ets.delete(@subscribers_table, subscriber)
    remove_subscriber_from_all_topics(subscriber)

    :ok
  end

  @doc false
  @spec register_topic(topic()) :: :ok
  def register_topic(topic) do
    topic_subscribers = compute_topic_subscribers(topic)
    :ets.insert(@topic_map_table, {topic, topic_subscribers})
    :ok
  end

  @doc false
  @spec unregister_topic(topic()) :: :ok
  def unregister_topic(topic) do
    :ets.delete(@topic_map_table, topic)
    :ok
  end

  @doc false
  @spec subscribers() :: subscribers()
  def subscribers do
    :ets.tab2list(@subscribers_table)
  end

  @spec subscribers(topic()) :: subscribers()
  def subscribers(topic) do
    case :ets.lookup(@topic_map_table, topic) do
      [{^topic, subs}] -> subs
      [] -> []
    end
  end

  # Recompute which topics this subscriber matches and update topic_map entries
  defp rebuild_topic_map_for_subscriber(subscriber, patterns) do
    :ets.tab2list(@topic_map_table)
    |> Enum.each(fn {topic, topic_subs} ->
      topic_subs = List.delete(topic_subs, subscriber)

      new_subs =
        if RegexUtil.superset?(patterns, topic) do
          [subscriber | topic_subs]
        else
          topic_subs
        end

      :ets.insert(@topic_map_table, {topic, new_subs})
    end)
  end

  defp remove_subscriber_from_all_topics(subscriber) do
    :ets.tab2list(@topic_map_table)
    |> Enum.each(fn {topic, topic_subs} ->
      new_subs = List.delete(topic_subs, subscriber)
      :ets.insert(@topic_map_table, {topic, new_subs})
    end)
  end

  defp compute_topic_subscribers(topic) do
    :ets.tab2list(@subscribers_table)
    |> Enum.reduce([], fn {subscriber, patterns}, acc ->
      if RegexUtil.superset?(patterns, topic), do: [subscriber | acc], else: acc
    end)
  end

  defp clear_limit(subscriber) do
    if :ets.info(@limits_table) != :undefined do
      :ets.delete(@limits_table, subscriber)
    end

    :ok
  end

  defp store_opts(subscriber, opts) do
    priority = Keyword.get(opts, :priority, 0)
    guard = Keyword.get(opts, :guard)
    :ets.insert(@opts_table, {subscriber, %{priority: priority, guard: guard}})
    :ok
  end

  defp clear_opts(subscriber) do
    if :ets.info(@opts_table) != :undefined do
      :ets.delete(@opts_table, subscriber)
    end

    :ok
  end

  @doc false
  @spec fetch_opts(term()) :: %{priority: integer(), guard: function() | nil}
  def fetch_opts(subscriber) do
    case :ets.lookup(@opts_table, subscriber) do
      [{^subscriber, opts}] -> opts
      [] -> %{priority: 0, guard: nil}
    end
  end
end
