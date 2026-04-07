defmodule EventBus.SweepStrategy.BulkSmart do
  @moduledoc """
  Default sweep strategy optimized for throughput.

  Expires events in batches using `Observation.expire_batch/2`. When no limited
  subscriptions (`subscribe_once`/`subscribe_n`) exist, batches are pure ETS
  deletes with zero GenServer calls. Status and debug table cleanups use single
  `select_delete` scans per batch instead of per-event `match_delete`.

  Emits one `[:event_bus, :sweep, :cycle]` telemetry event per sweep with
  `%{expired_per_topic: %{topic => count}}` in the metadata.
  """

  @behaviour EventBus.SweepStrategy

  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Service.Observation, as: ObservationService

  @impl true
  def init do
    limited_set = SubscriptionManager.limited_subscribers()
    %{limited_set: limited_set, topic_counts: %{}}
  end

  @impl true
  def handle_batch(batch, %{limited_set: limited_set, topic_counts: topic_counts} = state) do
    event_shadows = Enum.map(batch, fn {topic, id, _inserted_at} -> {topic, id} end)
    {count, batch_topics} = ObservationService.expire_batch(event_shadows, limited_set)

    merged = Map.merge(topic_counts, batch_topics, fn _k, v1, v2 -> v1 + v2 end)
    {count, %{state | topic_counts: merged}}
  end

  @impl true
  def telemetry_metadata(%{topic_counts: topic_counts}) do
    %{expired_per_topic: topic_counts}
  end
end
