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
    link = Supervisor.start_link(children, opts)
    Topic.register_from_config()
    link
  end
end
