defmodule EventBus.Application do
  @moduledoc false

  use Application

  alias EventBus.Manager.{
    Subscription,
    Topic
  }

  alias EventBus.Service.Debug
  alias EventBus.Service.Observation, as: ObservationService
  alias EventBus.Service.Store, as: StoreService
  alias EventBus.Service.Subscription, as: SubscriptionService

  def start(_type, _args) do
    Debug.setup_table()
    StoreService.setup_table()
    ObservationService.setup_table()
    SubscriptionService.setup_tables()

    children = [
      {Task.Supervisor, name: EventBus.TaskSupervisor},
      Topic,
      Subscription
    ]

    opts = [strategy: :one_for_one, name: EventBus.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      Topic.register_from_config()
      {:ok, pid}
    end
  end
end
