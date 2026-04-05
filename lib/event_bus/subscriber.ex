defmodule EventBus.Subscriber do
  @moduledoc """
  Optional behaviour for EventBus subscribers.

  Provides compile-time checks for subscriber implementations.
  Existing subscribers without this behaviour continue to work — this
  is purely opt-in.

  ## Usage

      defmodule MySubscriber do
        use EventBus.Subscriber

        @impl true
        def process({topic, id}) do
          event = EventBus.fetch_event({topic, id})
          # handle event...
          EventBus.mark_as_completed({__MODULE__, {topic, id}})
        end
      end

  ## With configuration

      defmodule MyConfigSubscriber do
        use EventBus.Subscriber

        @impl true
        def process({config, topic, id}) do
          event = EventBus.fetch_event({topic, id})
          # handle event with config...
          EventBus.mark_as_completed({{__MODULE__, config}, {topic, id}})
        end
      end
  """

  @doc """
  Process an event shadow.

  Receives either `{topic, id}` for plain subscribers or
  `{config, topic, id}` for configured subscribers.

  Return values:
  - `:ok` or any value — normal completion (subscriber is responsible
    for calling mark_as_completed/mark_as_skipped)
  - `{:cancel, reason}` — cancel propagation to remaining subscribers
  """
  @callback process(
              event_shadow ::
                {topic :: atom(), id :: term()}
                | {config :: term(), topic :: atom(), id :: term()}
            ) :: :ok | {:cancel, term()} | term()

  defmacro __using__(_opts) do
    quote do
      @behaviour EventBus.Subscriber
    end
  end
end
