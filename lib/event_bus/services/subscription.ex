defmodule EventBus.Service.Subscription do
  @moduledoc false

  alias EventBus.Service.Debug
  alias EventBus.Util.Regex, as: RegexUtil

  @subscribers_table :eb_subscribers
  @topic_map_table :eb_topic_subscribers
  @opts_table :eb_subscription_opts
  @limits_table :eb_subscription_limits

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
  def subscribed?({subscriber, topic_patterns}) do
    Enum.member?(subscribers(), {normalize(subscriber), topic_patterns})
  end

  @doc false
  @spec setup_tables() :: :ok
  def setup_tables do
    for table <- [
          @subscribers_table,
          @topic_map_table,
          @opts_table,
          @limits_table
        ] do
      if :ets.info(table) == :undefined do
        :ets.new(table, @ets_opts)
      end
    end

    :ok
  end

  @doc false
  @spec subscribe(subscriber_with_topic_patterns()) :: :ok
  def subscribe({subscriber, topics}) do
    subscriber = normalize(subscriber)

    Debug.log(fn ->
      "subscribe subscriber=#{inspect(subscriber)} patterns=#{inspect(topics)}"
    end)

    :ets.insert(@subscribers_table, {subscriber, topics})
    rebuild_topic_map_for_subscriber(subscriber, topics)

    :ok
  end

  @doc false
  @spec unsubscribe(subscriber()) :: :ok
  def unsubscribe(subscriber) do
    subscriber = normalize(subscriber)
    Debug.log(fn -> "unsubscribe subscriber=#{inspect(subscriber)}" end)
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
    topic
    |> subscribers_with_opts()
    |> Enum.map(fn {subscriber, _opts} -> subscriber end)
  end

  @doc """
  Per-topic subscriber list with opts attached, pre-sorted by priority
  (highest first) at subscribe/register time. This is the notification
  hot-path read: one ETS lookup, no per-subscriber lookups, no sorting.
  """
  @spec subscribers_with_opts(topic()) :: [{subscriber(), map()}]
  def subscribers_with_opts(topic) do
    case :ets.lookup(@topic_map_table, topic) do
      [{^topic, pairs}] -> pairs
      [] -> []
    end
  end

  # Read-modify-write is safe: topic-map writes go through the manager.
  defp rebuild_topic_map_for_subscriber(subscriber, patterns) do
    opts = fetch_opts(subscriber)

    :ets.tab2list(@topic_map_table)
    |> Enum.each(fn {topic, pairs} ->
      pairs = List.keydelete(pairs, subscriber, 0)

      new_pairs =
        if RegexUtil.superset?(patterns, topic) do
          sort_by_priority([{subscriber, opts} | pairs])
        else
          pairs
        end

      :ets.insert(@topic_map_table, {topic, new_pairs})
    end)
  end

  defp remove_subscriber_from_all_topics(subscriber) do
    :ets.tab2list(@topic_map_table)
    |> Enum.each(fn {topic, pairs} ->
      new_pairs = List.keydelete(pairs, subscriber, 0)
      :ets.insert(@topic_map_table, {topic, new_pairs})
    end)
  end

  defp compute_topic_subscribers(topic) do
    :ets.tab2list(@subscribers_table)
    |> Enum.reduce([], fn {subscriber, patterns}, acc ->
      if RegexUtil.superset?(patterns, topic) do
        [{subscriber, fetch_opts(subscriber)} | acc]
      else
        acc
      end
    end)
    |> sort_by_priority()
  end

  defp sort_by_priority(pairs) do
    Enum.sort_by(pairs, fn {_subscriber, opts} -> opts.priority end, :desc)
  end

  defp fetch_opts(subscriber) do
    case :ets.lookup(@opts_table, subscriber) do
      [{^subscriber, opts}] ->
        Map.take(opts, [:priority, :guard, :limit_generation])

      _ ->
        %{priority: 0, guard: nil, limit_generation: nil}
    end
  end

  defp normalize(subscriber) when is_atom(subscriber), do: {subscriber, nil}
  defp normalize({_module, _config} = subscriber), do: subscriber
end
