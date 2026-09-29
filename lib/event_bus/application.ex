defmodule EventBus.Application do
  @moduledoc false

  use Application

  alias EventBus.Manager.Subscription

  alias EventBus.Service.Debug
  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Service.Subscription, as: SubscriptionService
  alias EventBus.Service.Sweeper, as: SweeperService
  alias EventBus.Service.Topic, as: TopicService
  alias EventBus.Util.MonotonicTime

  def start(_type, _args) do
    # Cache the offset before concurrent first callers can race.
    MonotonicTime.init()

    Debug.setup_table()
    StoreService.setup_table()
    ObservationService.setup_table()
    SubscriptionService.setup_tables()
    TopicService.setup_table()

    children =
      [
        {Task.Supervisor, name: EventBus.TaskSupervisor},
        Subscription
      ] ++ maybe_sweeper()

    opts = [strategy: :one_for_one, name: EventBus.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      TopicService.register_from_config()
      {:ok, pid}
    end
  end

  defp maybe_sweeper do
    if Application.get_env(:event_bus, :event_ttl) do
      [SweeperService]
    else
      []
    end
  end
end
