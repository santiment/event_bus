defmodule EventBus.SubscriberTest do
  use ExUnit.Case, async: true

  test "module using EventBus.Subscriber compiles with process/1" do
    defmodule ValidSubscriber do
      use EventBus.Subscriber

      @impl true
      def process({_topic, _id}), do: :ok
    end

    assert function_exported?(ValidSubscriber, :process, 1)
  end

  test "module using EventBus.Subscriber compiles with config process/1" do
    defmodule ValidConfigSubscriber do
      use EventBus.Subscriber

      @impl true
      def process({_config, _topic, _id}), do: :ok
    end

    assert function_exported?(ValidConfigSubscriber, :process, 1)
  end

  test "module using EventBus.Subscriber can return cancel tuple" do
    defmodule CancelSubscriber do
      use EventBus.Subscriber

      @impl true
      def process({_topic, _id}), do: {:cancel, "reason"}
    end

    assert {:cancel, "reason"} == CancelSubscriber.process({:test, "1"})
  end
end
