defmodule EventBus.MixProject do
  use Mix.Project

  @source_url "https://github.com/otobus/event_bus"
  @version "1.7.0"

  def project do
    [
      app: :event_bus,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps(),
      docs: docs(),
      dialyzer: [plt_add_deps: :transitive],
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {EventBus.Application, []}
    ]
  end

  defp elixirc_paths(:test) do
    ["lib", "test/support"]
  end

  defp elixirc_paths(_) do
    ["lib"]
  end

  defp deps do
    [
      {:telemetry, "~> 0.4 or ~> 1.0", optional: true},
      {:ex_doc, "~> 0.34", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18", only: :test, runtime: false}
    ]
  end

  defp description do
    """
    Traceable, extendable and minimalist event bus implementation for Elixir
    with built-in event store and event watcher based on ETS
    """
  end

  defp package do
    [
      name: :event_bus,
      description: description(),
      files: ["lib", "mix.exs", "README.md", "CHANGELOG.md", "LICENSE.md"],
      maintainers: ["Mustafa Turan"],
      licenses: ["MIT"],
      links: %{
        "Changelog" => "https://hexdocs.pm/event_bus/changelog.html",
        "GitHub" => @source_url
      }
    ]
  end

  defp docs do
    [
      extras: [
        "CHANGELOG.md": [title: "Changelog"],
        "CONTRIBUTING.md": [title: "Contributing"],
        "CODE_OF_CONDUCT.md": [title: "Code of Conduct"],
        "LICENSE.md": [title: "License"],
        "QUESTIONS.md": [title: "Questions"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
