"""Tests for the meshcoretomqtt receiver bridge."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from meshcore_hub.interface.receiver_meshcoretomqtt import (
    ReceiverMeshcoreToMqtt,
    _safe_float,
    _safe_int,
    _truncate_key,
    create_receiver_mc2mqtt,
)


# ---------------------------------------------------------------------------
# Test utilities
# ---------------------------------------------------------------------------


def _make_key(seed: str, length: int = 64) -> str:
    """Generate a deterministic fake public key of *length* characters."""
    repeat = (length // len(seed)) + 1
    return (seed * repeat)[:length]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestTruncateKey:
    """Tests for the _truncate_key helper."""

    def test_long_key_truncated(self) -> None:
        assert _truncate_key("a" * 64) == "a" * 12

    def test_short_key_not_truncated(self) -> None:
        assert _truncate_key("abc") == "abc"

    def test_empty_string_returns_empty(self) -> None:
        assert _truncate_key("") == ""

    def test_custom_length(self) -> None:
        assert _truncate_key("abcdef", length=4) == "abcd"

    """Tests for the _safe_int helper."""

    def test_none_returns_none(self) -> None:
        assert _safe_int(None) is None

    def test_int_passthrough(self) -> None:
        assert _safe_int(5) == 5

    def test_string_digit(self) -> None:
        assert _safe_int("42") == 42

    def test_float_string(self) -> None:
        assert _safe_int("3.7") is None

    def test_invalid_string(self) -> None:
        assert _safe_int("abc") is None


class TestSafeFloat:
    """Tests for the _safe_float helper."""

    def test_none_returns_none(self) -> None:
        assert _safe_float(None) is None

    def test_float_passthrough(self) -> None:
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_string_float(self) -> None:
        assert _safe_float("-93") == pytest.approx(-93.0)

    def test_invalid_string(self) -> None:
        assert _safe_float("not_a_number") is None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def source_mqtt() -> MagicMock:
    """Mock source MQTT client (meshcoretomqtt broker)."""
    client = MagicMock()
    client.topic_builder = MagicMock()
    return client


@pytest.fixture
def output_mqtt() -> MagicMock:
    """Mock output MQTT client (meshcore-hub collector broker)."""
    client = MagicMock()
    client.topic_builder = MagicMock()
    client.topic_builder.event_topic.side_effect = (
        lambda pk, ev: f"meshcore/{pk}/event/{ev}"
    )
    return client


@pytest.fixture
def receiver(source_mqtt: MagicMock, output_mqtt: MagicMock) -> ReceiverMeshcoreToMqtt:
    """Create a ReceiverMeshcoreToMqtt with mock MQTT clients."""
    return ReceiverMeshcoreToMqtt(
        source_mqtt=source_mqtt,
        output_mqtt=output_mqtt,
        source_prefix="meshcore",
    )


@pytest.fixture
def receiver_with_iata(
    source_mqtt: MagicMock, output_mqtt: MagicMock
) -> ReceiverMeshcoreToMqtt:
    """Create a receiver with an IATA filter."""
    return ReceiverMeshcoreToMqtt(
        source_mqtt=source_mqtt,
        output_mqtt=output_mqtt,
        source_prefix="meshcore",
        source_iata_filter="SEA",
    )


# ---------------------------------------------------------------------------
# Topic parsing
# ---------------------------------------------------------------------------


class TestParseTopic:
    """Tests for ReceiverMeshcoreToMqtt._parse_topic."""

    def test_standard_packets_topic(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        result = receiver._parse_topic("meshcore/SEA/ABCD1234/packets")
        assert result == ("SEA", "ABCD1234", "packets")

    def test_standard_status_topic(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        result = receiver._parse_topic("meshcore/LAX/PUBKEY123/status")
        assert result == ("LAX", "PUBKEY123", "status")

    def test_standard_debug_topic(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        result = receiver._parse_topic("meshcore/NYC/KEYABC/debug")
        assert result == ("NYC", "KEYABC", "debug")

    def test_wrong_prefix_returns_none(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        result = receiver._parse_topic("other/SEA/PUBKEY/packets")
        assert result is None

    def test_too_short_returns_none(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        result = receiver._parse_topic("meshcore/SEA/PUBKEY")
        assert result is None

    def test_multi_part_prefix(
        self, source_mqtt: MagicMock, output_mqtt: MagicMock
    ) -> None:
        recv = ReceiverMeshcoreToMqtt(
            source_mqtt=source_mqtt,
            output_mqtt=output_mqtt,
            source_prefix="mesh/core",
        )
        result = recv._parse_topic("mesh/core/SEA/PUBKEY/packets")
        assert result == ("SEA", "PUBKEY", "packets")


class TestMakeSubscribeTopic:
    """Tests for ReceiverMeshcoreToMqtt._make_subscribe_topic."""

    def test_no_iata_filter(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        assert receiver._make_subscribe_topic("packets") == "meshcore/+/+/packets"

    def test_with_iata_filter(self, receiver_with_iata: ReceiverMeshcoreToMqtt) -> None:
        assert (
            receiver_with_iata._make_subscribe_topic("packets")
            == "meshcore/SEA/+/packets"
        )

    def test_status_topic(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        assert receiver._make_subscribe_topic("status") == "meshcore/+/+/status"


# ---------------------------------------------------------------------------
# Status handler
# ---------------------------------------------------------------------------


class TestHandleStatus:
    """Tests for ReceiverMeshcoreToMqtt._handle_status."""

    def test_publishes_advertisement(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload: dict[str, Any] = {
            "status": "online",
            "origin": "my-repeater",
            "origin_id": _make_key("ABCD"),
            "firmware_version": "2.3.1",
            "model": "RAK4631",
        }
        receiver._handle_status(_make_key("ABCD"), payload)

        output_mqtt.publish_event.assert_called_once()
        call_args = output_mqtt.publish_event.call_args
        assert call_args[0][1] == "advertisement"
        event_payload = call_args[0][2]
        assert event_payload["public_key"] == _make_key("ABCD")
        assert event_payload["name"] == "my-repeater"
        assert event_payload["adv_type"] == "repeater"
        assert event_payload["firmware_version"] == "2.3.1"
        assert event_payload["model"] == "RAK4631"

    def test_uses_origin_id_over_topic_key(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload: dict[str, Any] = {
            "origin": "node",
            "origin_id": _make_key("REAL"),
        }
        receiver._handle_status(_make_key("DIFFERENTABCDE"), payload)

        call_args = output_mqtt.publish_event.call_args
        assert call_args[0][0] == _make_key("REAL")

    def test_missing_origin_id_skips(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_status("", {"origin": "node"})
        output_mqtt.publish_event.assert_not_called()

    def test_falls_back_to_topic_public_key(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload: dict[str, Any] = {"origin": "fallback-node"}
        receiver._handle_status(_make_key("FALLBACKKEY"), payload)
        output_mqtt.publish_event.assert_called_once()


# ---------------------------------------------------------------------------
# Packet handler
# ---------------------------------------------------------------------------


class TestHandlePacket:
    """Tests for ReceiverMeshcoreToMqtt._handle_packet."""

    def _flood_payload(self) -> dict[str, Any]:
        return {
            "origin": "ag loft rpt",
            "origin_id": _make_key("A1B2"),
            "timestamp": "2025-03-16T00:07:11.191561",
            "type": "PACKET",
            "direction": "rx",
            "time": "00:07:09",
            "date": "16/3/2025",
            "len": "87",
            "packet_type": "5",
            "route": "F",
            "payload_len": "83",
            "raw": "0A1B2C3D",
            "SNR": "4",
            "RSSI": "-93",
            "score": "1000",
            "hash": "AC9D2DDDD8395712",
        }

    def test_publishes_packet_log(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_packet(_make_key("A1B2"), self._flood_payload())

        output_mqtt.publish_event.assert_called_once()
        call_args = output_mqtt.publish_event.call_args
        assert call_args[0][1] == "packet_log"

    def test_rf_metrics_normalised(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_packet(_make_key("A1B2"), self._flood_payload())

        event_payload = output_mqtt.publish_event.call_args[0][2]
        assert event_payload["snr"] == pytest.approx(4.0)
        assert event_payload["rssi"] == pytest.approx(-93.0)
        assert event_payload["score"] == 1000

    def test_packet_type_normalised_to_int(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_packet(_make_key("A1B2"), self._flood_payload())
        event_payload = output_mqtt.publish_event.call_args[0][2]
        assert event_payload["packet_type"] == 5

    def test_direct_packet_includes_path(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload = {
            **self._flood_payload(),
            "route": "D",
            "path": "C2 -> E2",
        }
        receiver._handle_packet(_make_key("A1B2"), payload)
        event_payload = output_mqtt.publish_event.call_args[0][2]
        assert event_payload["path"] == "C2 -> E2"

    def test_none_values_excluded(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload: dict[str, Any] = {
            "origin_id": _make_key("ABCD"),
            "direction": "rx",
            "route": "F",
        }
        receiver._handle_packet(_make_key("ABCD"), payload)
        event_payload = output_mqtt.publish_event.call_args[0][2]
        # Keys with None values should be stripped
        for v in event_payload.values():
            assert v is not None

    def test_missing_origin_id_skips(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_packet("", {"direction": "rx"})
        output_mqtt.publish_event.assert_not_called()


# ---------------------------------------------------------------------------
# Debug handler
# ---------------------------------------------------------------------------


class TestHandleDebug:
    """Tests for ReceiverMeshcoreToMqtt._handle_debug."""

    def test_publishes_debug_log(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        payload: dict[str, Any] = {
            "origin": "node",
            "origin_id": _make_key("DEBUG"),
            "type": "DEBUG",
            "message": "DEBUG some firmware output",
        }
        receiver._handle_debug(_make_key("DEBUG"), payload)

        output_mqtt.publish_event.assert_called_once()
        call_args = output_mqtt.publish_event.call_args
        assert call_args[0][1] == "debug_log"

    def test_missing_origin_id_skips(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_debug("", {"message": "DEBUG something"})
        output_mqtt.publish_event.assert_not_called()


# ---------------------------------------------------------------------------
# Message routing
# ---------------------------------------------------------------------------


class TestHandleSourceMessage:
    """Tests for ReceiverMeshcoreToMqtt._handle_source_message."""

    def test_routes_status(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        with patch.object(receiver, "_handle_status") as mock_handler:
            receiver._handle_source_message(
                topic="meshcore/SEA/PUBKEY123/status",
                pattern="meshcore/+/+/status",
                payload={"origin": "node", "origin_id": "PUBKEY123"},
            )
            mock_handler.assert_called_once_with(
                "PUBKEY123", {"origin": "node", "origin_id": "PUBKEY123"}
            )

    def test_routes_packets(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        with patch.object(receiver, "_handle_packet") as mock_handler:
            receiver._handle_source_message(
                topic="meshcore/SEA/PUBKEY123/packets",
                pattern="meshcore/+/+/packets",
                payload={"origin_id": "PUBKEY123", "direction": "rx"},
            )
            mock_handler.assert_called_once()

    def test_routes_debug(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        with patch.object(receiver, "_handle_debug") as mock_handler:
            receiver._handle_source_message(
                topic="meshcore/SEA/PUBKEY123/debug",
                pattern="meshcore/+/+/debug",
                payload={"origin_id": "PUBKEY123", "message": "DEBUG"},
            )
            mock_handler.assert_called_once()

    def test_unknown_feed_ignored(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        # Should not raise
        receiver._handle_source_message(
            topic="meshcore/SEA/PUBKEY123/internal",
            pattern="meshcore/+/+/#",
            payload={},
        )
        output_mqtt.publish_event.assert_not_called()

    def test_invalid_topic_ignored(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        receiver._handle_source_message(
            topic="unknown/topic",
            pattern="#",
            payload={},
        )
        output_mqtt.publish_event.assert_not_called()

    def test_handler_exception_does_not_propagate(
        self, receiver: ReceiverMeshcoreToMqtt, output_mqtt: MagicMock
    ) -> None:
        output_mqtt.publish_event.side_effect = RuntimeError("broker down")
        # Should not raise
        receiver._handle_source_message(
            topic="meshcore/SEA/ABCDEF/status",
            pattern="meshcore/+/+/status",
            payload={"origin": "node", "origin_id": "ABCDEF"},
        )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestReceiverLifecycle:
    """Tests for start / stop lifecycle of ReceiverMeshcoreToMqtt."""

    def test_start_connects_both_brokers(
        self,
        receiver: ReceiverMeshcoreToMqtt,
        source_mqtt: MagicMock,
        output_mqtt: MagicMock,
    ) -> None:
        receiver.start()
        try:
            source_mqtt.connect.assert_called_once()
            source_mqtt.start_background.assert_called_once()
            output_mqtt.connect.assert_called_once()
            output_mqtt.start_background.assert_called_once()
        finally:
            receiver.stop()

    def test_start_subscribes_to_all_feeds(
        self,
        receiver: ReceiverMeshcoreToMqtt,
        source_mqtt: MagicMock,
    ) -> None:
        receiver.start()
        try:
            subscribed_topics = {
                call[0][0] for call in source_mqtt.subscribe.call_args_list
            }
            assert "meshcore/+/+/packets" in subscribed_topics
            assert "meshcore/+/+/status" in subscribed_topics
            assert "meshcore/+/+/debug" in subscribed_topics
        finally:
            receiver.stop()

    def test_stop_disconnects_both_brokers(
        self,
        receiver: ReceiverMeshcoreToMqtt,
        source_mqtt: MagicMock,
        output_mqtt: MagicMock,
    ) -> None:
        receiver.start()
        receiver.stop()

        source_mqtt.stop.assert_called_once()
        source_mqtt.disconnect.assert_called_once()
        output_mqtt.stop.assert_called_once()
        output_mqtt.disconnect.assert_called_once()

    def test_stop_is_idempotent(
        self,
        receiver: ReceiverMeshcoreToMqtt,
        source_mqtt: MagicMock,
    ) -> None:
        receiver.start()
        receiver.stop()
        receiver.stop()  # second call should be a no-op
        assert source_mqtt.stop.call_count == 1

    def test_is_healthy_after_start(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        receiver.start()
        try:
            assert receiver.is_healthy
        finally:
            receiver.stop()

    def test_is_unhealthy_after_stop(self, receiver: ReceiverMeshcoreToMqtt) -> None:
        receiver.start()
        receiver.stop()
        assert not receiver.is_healthy


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


class TestCreateReceiverMc2Mqtt:
    """Tests for create_receiver_mc2mqtt factory."""

    def test_creates_receiver(self) -> None:
        with (
            patch("meshcore_hub.interface.receiver_meshcoretomqtt.MQTTClient"),
            patch("meshcore_hub.interface.receiver_meshcoretomqtt.MQTTConfig"),
        ):
            recv = create_receiver_mc2mqtt()
            assert isinstance(recv, ReceiverMeshcoreToMqtt)

    def test_source_and_output_have_distinct_client_ids(self) -> None:
        created_configs: list[Any] = []

        def _capture(**kwargs: Any) -> MagicMock:
            created_configs.append(kwargs)
            return MagicMock()

        with patch(
            "meshcore_hub.interface.receiver_meshcoretomqtt.MQTTConfig",
            side_effect=_capture,
        ):
            with patch("meshcore_hub.interface.receiver_meshcoretomqtt.MQTTClient"):
                create_receiver_mc2mqtt(
                    source_mqtt_host="broker-a",
                    output_mqtt_host="broker-b",
                )

        client_ids = [c.get("client_id") for c in created_configs]
        assert (
            len(set(client_ids)) == 2
        ), "source and output must have different client IDs"

    def test_iata_filter_propagated(self) -> None:
        with (
            patch("meshcore_hub.interface.receiver_meshcoretomqtt.MQTTClient"),
            patch("meshcore_hub.interface.receiver_meshcoretomqtt.MQTTConfig"),
        ):
            recv = create_receiver_mc2mqtt(source_iata_filter="SEA")
            assert recv.source_iata_filter == "SEA"
