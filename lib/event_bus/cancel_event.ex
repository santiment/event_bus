defmodule EventBus.CancelEvent do
  @moduledoc """
  Exception that a subscriber can raise during `process/1` to cancel
  event propagation to remaining lower-priority subscribers.
  """

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "event cancelled: #{inspect(reason)}"
  end
end
