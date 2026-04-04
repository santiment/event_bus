defmodule EventBus.Application do
  @moduledoc false

  use Application

  alias EventBus.Manager.{
    Notification,
    Observation,
    Store,
    Subscription,
    Topic
  }

  def start(_type, _args) do
    children = [
      Topic,
      Subscription,
      Notification,
      Store,
      Observation
    ]

    opts = [strategy: :one_for_one, name: EventBus.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      Topic.register_from_config()
      {:ok, pid}
    end
  end
end
