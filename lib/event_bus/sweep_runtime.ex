defmodule EventBus.SweepRuntime do
  @moduledoc """
  Public API for custom sweep strategies.

  This module provides the supported entry points for expiring events during
  a sweep cycle. Custom `EventBus.SweepStrategy` implementations should use
  these functions instead of calling internal service or manager modules
  directly.

  ## Functions

    * `expire_event/1` — expire a single event, cleaning up all state
    * `expire_batch/1` — expire a batch of events efficiently

  See `CUSTOM_SWEEPERS.md` for a full guide on writing custom strategies.
  """

  alias EventBus.Manager.Subscription, as: SubscriptionManager
  alias EventBus.Service.Observation, as: ObservationService

  @doc """
  Expire a single event, cleaning up all observation, store, and snapshot state.

  Decrements limited subscription counters for any pending subscribers.

  Returns `{:ok, info}` with subscriber details, or `:not_found` if the event
  was already cleaned up (e.g., by normal completion).

  ## Example

      def handle_batch(batch, state) do
        count =
          Enum.count(batch, fn {topic, id, _inserted_at} ->
            match?({:ok, _}, EventBus.SweepRuntime.expire_event({topic, id}))
          end)

        {count, state}
      end

  """
  @spec expire_event(EventBus.event_shadow()) ::
          {:ok,
           %{
             subscribers: EventBus.subscribers(),
             completers: EventBus.subscribers(),
             skippers: EventBus.subscribers()
           }}
          | :not_found
  def expire_event(event_shadow) do
    ObservationService.force_expire(event_shadow)
  end

  @doc """
  Expire a batch of events in a single pass.

  Handles limited-subscription accounting internally — callers do not need to
  fetch or pass the set of limited subscribers. Events already cleaned up by
  normal completion are skipped.

  Returns a map with:

    * `:expired_count` — number of events actually expired
    * `:expired_per_topic` — `%{topic => count}` breakdown

  ## Example

      def handle_batch(batch, state) do
        event_shadows = Enum.map(batch, fn {topic, id, _inserted_at} -> {topic, id} end)
        %{expired_count: count} = EventBus.SweepRuntime.expire_batch(event_shadows)
        {count, state}
      end

  """
  @spec expire_batch([EventBus.event_shadow()]) ::
          %{
            expired_count: non_neg_integer(),
            expired_per_topic: %{optional(atom()) => non_neg_integer()}
          }
  def expire_batch(event_shadows) do
    limited_set = SubscriptionManager.limited_subscribers()

    {count, topic_counts} =
      ObservationService.expire_batch(event_shadows, limited_set)

    %{expired_count: count, expired_per_topic: topic_counts}
  end
end
