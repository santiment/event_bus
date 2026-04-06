defmodule EventBus.Service.Topic do
  @moduledoc false

  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Service.Debug
  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService

  @typep topic :: EventBus.topic()
  @typep topics :: EventBus.topics()

  @table :eb_topics
  @table_opts [:set, :public, :named_table, {:read_concurrency, true}]
  @modules [StoreService, SubscriptionManager, ObservationService]

  @doc false
  @spec setup_table() :: :ok
  def setup_table do
    if :ets.info(@table) == :undefined do
      :ets.new(@table, @table_opts)
    end

    :ok
  end

  @doc false
  @spec all() :: topics()
  def all do
    :ets.tab2list(@table) |> Enum.map(fn {topic} -> topic end)
  end

  @doc false
  @spec exist?(topic()) :: boolean()
  def exist?(topic) do
    :ets.member(@table, topic)
  end

  @doc false
  @spec register_from_config() :: :ok
  def register_from_config do
    topics = Application.get_env(:event_bus, :topics, [])

    Enum.each(topics, fn topic ->
      register(topic)
    end)

    :ok
  end

  @doc false
  @spec register(topic()) :: :ok
  def register(topic) do
    # insert_new is atomic: returns true if inserted, false if already exists.
    if :ets.insert_new(@table, {topic}) do
      Debug.log("register_topic topic=#{inspect(topic)}")
      Enum.each(@modules, fn mod -> mod.register_topic(topic) end)
    end

    :ok
  end

  @doc false
  @spec unregister(topic()) :: :ok
  def unregister(topic) do
    if exist?(topic) do
      Debug.log("unregister_topic topic=#{inspect(topic)}")
      Enum.each(@modules, fn mod -> mod.unregister_topic(topic) end)
      :ets.delete(@table, topic)
    end

    :ok
  end
end
