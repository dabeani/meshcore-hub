"""Tests for the collector CLI."""

from click.testing import CliRunner

from meshcore_hub.collector import cli as collector_cli


class TestCollectorCLI:
    """Tests for collector CLI ingest mode handling."""

    def test_accepts_mc2mqtt_ingest_mode(self, monkeypatch) -> None:
        """The collector CLI accepts MC2MQTT as an ingest mode."""
        runner = CliRunner()
        captured: dict[str, str] = {}

        def fake_run_collector_service(**kwargs) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            collector_cli,
            "_run_collector_service",
            fake_run_collector_service,
        )

        result = runner.invoke(
            collector_cli.collector,
            ["--ingest-mode", "mc2mqtt"],
        )

        assert result.exit_code == 0
        assert captured["ingest_mode"] == "mc2mqtt"

    def test_accepts_mc2mqtt_ingest_mode_from_env(self, monkeypatch) -> None:
        """The collector CLI accepts mc2mqtt from COLLECTOR_INGEST_MODE."""
        runner = CliRunner()
        captured: dict[str, str] = {}

        def fake_run_collector_service(**kwargs) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            collector_cli,
            "_run_collector_service",
            fake_run_collector_service,
        )
        monkeypatch.setenv("COLLECTOR_INGEST_MODE", "mc2mqtt")

        result = runner.invoke(collector_cli.collector, [])

        assert result.exit_code == 0
        assert captured["ingest_mode"] == "mc2mqtt"

    def test_legacy_mc2mqtt_flag_accepts_mc2mqtt_ingest_mode(self, monkeypatch) -> None:
        """Legacy MQTT_MC2MQTT flag remains compatible with mc2mqtt ingest mode."""
        runner = CliRunner()
        captured: dict[str, str] = {}

        def fake_run_collector_service(**kwargs) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            collector_cli,
            "_run_collector_service",
            fake_run_collector_service,
        )

        result = runner.invoke(
            collector_cli.collector,
            ["--mqtt-mc2mqtt", "--ingest-mode", "mc2mqtt"],
        )

        assert result.exit_code == 0
        assert captured["ingest_mode"] == "mc2mqtt"
