defmodule EventBus.SweepStrategy do
  @moduledoc """
  Behaviour for sweep strategies.

  A sweep strategy controls how expired events are processed during each
  sweep cycle. EventBus ships with two built-in strategies:

    * `EventBus.SweepStrategy.BulkSmart` (`:bulk_smart`) — batched ETS
      operations with smart limited-subscription handling. Default.

    * `EventBus.SweepStrategy.Detailed` (`:detailed`) — per-event processing
      with per-event telemetry.

  ## Custom strategies

  Implement this behaviour to define your own strategy (e.g., routing expired
  events to a dead letter topic before deletion):

      defmodule MyApp.DeadLetterSweep do
        @behaviour EventBus.SweepStrategy

        @impl true
        def init, do: %{dead_lettered: 0}

        @impl true
        def handle_batch(batch, state) do
          event_shadows = Enum.map(batch, fn {topic, id, _inserted_at} -> {topic, id} end)

          Enum.each(event_shadows, fn {topic, id} ->
            event = EventBus.fetch_event({topic, id})
            if event, do: MyApp.DeadLetter.store(event)
          end)

          limited = EventBus.Manager.Subscription.limited_subscribers()
          {count, _topics} = EventBus.Service.Observation.expire_batch(event_shadows, limited)
          {count, %{state | dead_lettered: state.dead_lettered + count}}
        end

        @impl true
        def telemetry_metadata(state) do
          %{dead_lettered: state.dead_lettered}
        end
      end

  Then configure it:

      config :event_bus,
        event_ttl: 300_000,
        sweep_strategy: MyApp.DeadLetterSweep

  """

  @typedoc "A single entry from the expired-event cursor scan."
  @type batch_entry :: {topic :: atom(), id :: term(), inserted_at :: integer()}

  @doc """
  Called once at the start of each sweep cycle.

  Return any state your strategy needs across batches (e.g., a cached
  `MapSet` of limited subscribers, an accumulator for per-topic counts).
  """
  @callback init() :: term()

  @doc """
  Process one batch of expired event entries.

  `batch` is a list of `{topic, id, inserted_at}` tuples from the ETS cursor.
  Return `{expired_count, new_state}` where `expired_count` is the number of
  events actually expired (excluding those already cleaned by normal completion).
  """
  @callback handle_batch(batch :: [batch_entry()], state :: term()) ::
              {expired_count :: non_neg_integer(), new_state :: term()}

  @doc """
  Build the metadata map included in the `[:event_bus, :sweep, :cycle]`
  telemetry event at the end of the sweep.
  """
  @callback telemetry_metadata(state :: term()) :: map()
end
