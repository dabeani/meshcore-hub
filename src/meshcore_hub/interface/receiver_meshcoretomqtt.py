"""Receiver that bridges meshcoretomqtt MQTT data to meshcore-hub format.

This module implements a receiver that subscribes to MQTT topics published
by the `meshcoretomqtt` tool (https://github.com/Cisien/meshcoretomqtt) and
translates them into the standard meshcore-hub event format consumed by the
collector.

Workflow
--------
MeshCore repeater (debug firmware)
  → USB serial
  → Pi running *meshcoretomqtt*
  → MQTT broker  (source topics: ``{source_prefix}/{IATA}/{PUBLIC_KEY}/…``)
  → THIS RECEIVER (subscribes + translates)
  → MQTT broker  (output topics: ``{output_prefix}/{PUBLIC_KEY}/event/…``)
  → meshcore-hub collector

Source topic structure (meshcoretomqtt defaults)
-------------------------------------------------
``meshcore/{IATA}/{PUBLIC_KEY}/packets``
    Flood or direct LoRa packets observed by the repeater.
``meshcore/{IATA}/{PUBLIC_KEY}/status``
    Online/offline status (Last Will & Testament).
``meshcore/{IATA}/{PUBLIC_KEY}/debug``
    Debug lines from the firmware (requires ``-D MESH_DEBUG=1``).

Output event types (meshcore-hub)
----------------------------------
``advertisement``  – generated from *status* messages (node online/offline).
``packet_log``     – generated from *packets* messages (RF stats, hash, route).
``debug_log``      – generated from *debug* messages (raw firmware debug text).

The collector's fallback ``handle_event_log`` handler will store any event
type it does not recognise (e.g. ``packet_log``, ``debug_log``) in the
``events_log`` table automatically.
"""

import logging
import signal
import threading
from typing import Any, Optional

from meshcore_hub.common.health import HealthReporter
from meshcore_hub.common.mqtt import MQTTClient, MQTTConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _safe_int(value: Any) -> Optional[int]:
    """Convert a value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _truncate_key(key: str, length: int = 12) -> str:
    """Return the first *length* characters of *key* for log messages.

    Python slicing is always safe for short strings; this helper exists to
    make the intent explicit and keep log format consistent.
    """
    return key[:length] if key else ""


# ---------------------------------------------------------------------------
# Main receiver class
# ---------------------------------------------------------------------------


class ReceiverMeshcoreToMqtt:
    """Bridge between meshcoretomqtt MQTT topics and meshcore-hub events.

    Two ``MQTTClient`` instances are used:

    * **source_mqtt** – connects to the broker where *meshcoretomqtt* publishes
      raw packet/status/debug data.
    * **output_mqtt** – connects to the broker where the *meshcore-hub
      collector* listens for normalised events.

    Both clients may point to the same physical broker (typical single-broker
    deployment) or to different brokers (multi-broker deployment).
    """

    def __init__(
        self,
        source_mqtt: MQTTClient,
        output_mqtt: MQTTClient,
        source_prefix: str = "meshcore",
        source_iata_filter: Optional[str] = None,
    ) -> None:
        """Initialise the receiver.

        Args:
            source_mqtt: MQTT client connected to the meshcoretomqtt broker.
            output_mqtt: MQTT client connected to the meshcore-hub broker.
            source_prefix: Topic prefix used by meshcoretomqtt (default
                ``"meshcore"``).
            source_iata_filter: Optional IATA code to subscribe to a single
                location only.  ``None`` subscribes to all locations (``+``
                wildcard).
        """
        self.source_mqtt = source_mqtt
        self.output_mqtt = output_mqtt
        self.source_prefix = source_prefix.strip("/")
        self.source_iata_filter = source_iata_filter

        self._running = False
        self._shutdown_event = threading.Event()
        self._source_connected = False
        self._output_connected = False
        self._health_reporter: Optional[HealthReporter] = None

    # ------------------------------------------------------------------
    # Topic helpers
    # ------------------------------------------------------------------

    def _make_subscribe_topic(self, feed: str) -> str:
        """Build the MQTT subscription topic for *feed*.

        Example outputs::

            meshcore/+/+/packets     (no IATA filter)
            meshcore/SEA/+/packets   (IATA filter "SEA")
        """
        iata = self.source_iata_filter if self.source_iata_filter else "+"
        return f"{self.source_prefix}/{iata}/+/{feed}"

    def _parse_topic(self, topic: str) -> Optional[tuple[str, str, str]]:
        """Parse a meshcoretomqtt topic into ``(iata, public_key, feed)``.

        Returns ``None`` if the topic does not match the expected structure.
        """
        parts = topic.strip("/").split("/")
        prefix_parts = self.source_prefix.strip("/").split("/")
        prefix_len = len(prefix_parts)

        # Need at least: prefix + iata + public_key + feed
        if len(parts) < prefix_len + 3:
            return None

        if parts[:prefix_len] != prefix_parts:
            return None

        iata = parts[prefix_len]
        public_key = parts[prefix_len + 1]
        feed = parts[prefix_len + 2]
        return (iata, public_key, feed)

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @property
    def is_healthy(self) -> bool:
        """Return ``True`` when both MQTT connections are up."""
        return self._running and self._source_connected and self._output_connected

    def get_health_status(self) -> dict[str, Any]:
        """Return a health status dictionary."""
        return {
            "healthy": self.is_healthy,
            "running": self._running,
            "source_connected": self._source_connected,
            "output_connected": self._output_connected,
        }

    # ------------------------------------------------------------------
    # Message handlers
    # ------------------------------------------------------------------

    def _handle_source_message(
        self, topic: str, pattern: str, payload: dict[str, Any]
    ) -> None:
        """Dispatch an incoming meshcoretomqtt message to the correct handler."""
        parsed = self._parse_topic(topic)
        if not parsed:
            logger.warning("Cannot parse meshcoretomqtt topic: %s", topic)
            return

        iata, public_key, feed = parsed
        logger.debug(
            "Received %s message from %s (IATA=%s)",
            feed,
            _truncate_key(public_key),
            iata,
        )

        try:
            if feed == "status":
                self._handle_status(public_key, payload)
            elif feed == "packets":
                self._handle_packet(public_key, payload)
            elif feed == "debug":
                self._handle_debug(public_key, payload)
            else:
                logger.debug("Ignoring unknown feed type: %s", feed)
        except Exception as exc:
            logger.error(
                "Error handling %s message from %s: %s",
                feed,
                _truncate_key(public_key),
                exc,
            )

    def _handle_status(self, public_key: str, payload: dict[str, Any]) -> None:
        """Translate a *status* message into an ``advertisement`` event.

        A meshcoretomqtt status message signals that a repeater node is
        online or offline.  We convert it to an advertisement so the
        collector can maintain up-to-date node records.

        Source payload example::

            {
              "status": "online",
              "timestamp": "2025-03-16T00:07:11.191561",
              "origin": "ag loft rpt",
              "origin_id": "A1B2...",
              "radio": {...},
              "model": "RAK4631",
              "firmware_version": "2.3.1",
              "client_version": "meshcoretomqtt/1.1.0.0"
            }
        """
        origin_name = payload.get("origin", "")
        origin_id = payload.get("origin_id") or public_key

        if not origin_id:
            logger.warning("Status message missing origin_id – skipping")
            return

        adv_payload: dict[str, Any] = {
            "public_key": origin_id,
            "name": origin_name,
            "adv_type": "repeater",
        }

        status = payload.get("status")
        if status:
            adv_payload["status"] = status

        radio_info = payload.get("radio")
        if radio_info:
            adv_payload["radio"] = radio_info

        firmware = payload.get("firmware_version")
        if firmware:
            adv_payload["firmware_version"] = firmware

        model = payload.get("model")
        if model:
            adv_payload["model"] = model

        self.output_mqtt.publish_event(origin_id, "advertisement", adv_payload)
        logger.debug(
            "Published advertisement for %r (%s...)",
            origin_name,
            _truncate_key(origin_id),
        )

    def _handle_packet(self, public_key: str, payload: dict[str, Any]) -> None:
        """Translate a *packets* message into a ``packet_log`` event.

        A meshcoretomqtt packet message contains RF metrics (SNR, RSSI,
        hash) about a LoRa packet observed by the repeater.  The actual
        packet payload is encrypted so we store it as raw metadata.

        Source payload example (flood)::

            {
              "origin": "ag loft rpt",
              "origin_id": "A1B2...",
              "timestamp": "2025-03-16T00:07:11.191561",
              "type": "PACKET",
              "direction": "rx",
              "time": "00:07:09",
              "date": "16/3/2025",
              "len": "87",
              "packet_type": "5",
              "route": "F",
              "payload_len": "83",
              "raw": "0A1B2C...",
              "SNR": "4",
              "RSSI": "-93",
              "score": "1000",
              "hash": "AC9D2DDDD8395712"
            }

        Source payload example (direct)::

            { ..., "route": "D", "path": "C2 -> E2" }
        """
        origin_id = payload.get("origin_id") or public_key

        if not origin_id:
            logger.warning("Packet message missing origin_id – skipping")
            return

        packet_payload: dict[str, Any] = {
            "hash": payload.get("hash"),
            "direction": payload.get("direction"),
            "route": payload.get("route"),
            "packet_type": _safe_int(payload.get("packet_type")),
            "len": _safe_int(payload.get("len")),
            "payload_len": _safe_int(payload.get("payload_len")),
            "raw": payload.get("raw"),
        }

        # RF quality metrics (present for RX packets)
        snr = _safe_float(payload.get("SNR"))
        rssi = _safe_float(payload.get("RSSI"))
        score = _safe_int(payload.get("score"))
        duration = _safe_int(payload.get("duration"))

        if snr is not None:
            packet_payload["snr"] = snr
        if rssi is not None:
            packet_payload["rssi"] = rssi
        if score is not None:
            packet_payload["score"] = score
        if duration is not None:
            packet_payload["duration"] = duration

        # Path (direct packets only)
        path = payload.get("path")
        if path:
            packet_payload["path"] = path

        # Preserve source timestamp
        timestamp = payload.get("timestamp")
        if timestamp:
            packet_payload["timestamp"] = timestamp

        # Remove None values to keep payloads clean
        packet_payload = {k: v for k, v in packet_payload.items() if v is not None}

        self.output_mqtt.publish_event(origin_id, "packet_log", packet_payload)
        pkt_hash = payload.get("hash") or ""
        logger.debug(
            "Published packet_log for %s... (hash=%s)",
            _truncate_key(origin_id),
            pkt_hash[:8] if pkt_hash else "?",
        )

    def _handle_debug(self, public_key: str, payload: dict[str, Any]) -> None:
        """Translate a *debug* message into a ``debug_log`` event.

        Source payload example::

            {
              "origin": "ag loft rpt",
              "origin_id": "A1B2...",
              "timestamp": "...",
              "type": "DEBUG",
              "message": "DEBUG ..."
            }
        """
        origin_id = payload.get("origin_id") or public_key

        if not origin_id:
            logger.debug("Debug message missing origin_id – skipping")
            return

        self.output_mqtt.publish_event(origin_id, "debug_log", payload)
        logger.debug("Published debug_log for %s...", _truncate_key(origin_id))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Connect to both MQTT brokers and start listening."""
        logger.info("Starting meshcoretomqtt receiver")

        # Register subscriptions BEFORE connecting so they survive reconnects
        # (MQTTClient._on_connect resubscribes from _message_handlers)
        for feed in ("packets", "status", "debug"):
            topic = self._make_subscribe_topic(feed)
            self.source_mqtt.subscribe(topic, self._handle_source_message, qos=0)
            logger.info("Will subscribe to: %s", topic)

        # Connect output broker first so events can flow as soon as data arrives
        self.output_mqtt.connect()
        self.output_mqtt.start_background()
        self._output_connected = True
        logger.info("Connected to output MQTT broker")

        # Connect source broker
        self.source_mqtt.connect()
        self.source_mqtt.start_background()
        self._source_connected = True
        logger.info("Connected to source MQTT broker")

        self._running = True

        self._health_reporter = HealthReporter(
            component="interface",
            status_fn=self.get_health_status,
            interval=10.0,
        )
        self._health_reporter.start()

    def run(self) -> None:
        """Start the receiver and block until stopped."""
        if not self._running:
            self.start()

        logger.info("meshcoretomqtt receiver running – press Ctrl+C to stop")

        try:
            # Block here; actual work happens in paho-mqtt background threads
            while not self._shutdown_event.wait(timeout=1.0):
                pass
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()

    def stop(self) -> None:
        """Disconnect from both brokers and clean up."""
        if not self._running:
            return

        logger.info("Stopping meshcoretomqtt receiver")
        self._running = False
        self._shutdown_event.set()

        if self._health_reporter:
            self._health_reporter.stop()
            self._health_reporter = None

        self.source_mqtt.stop()
        self.source_mqtt.disconnect()
        self._source_connected = False

        self.output_mqtt.stop()
        self.output_mqtt.disconnect()
        self._output_connected = False

        logger.info("meshcoretomqtt receiver stopped")


# ---------------------------------------------------------------------------
# Factory & entry point
# ---------------------------------------------------------------------------


def create_receiver_mc2mqtt(
    source_mqtt_host: str = "localhost",
    source_mqtt_port: int = 1883,
    source_mqtt_username: Optional[str] = None,
    source_mqtt_password: Optional[str] = None,
    source_mqtt_tls: bool = False,
    source_prefix: str = "meshcore",
    source_iata_filter: Optional[str] = None,
    output_mqtt_host: str = "localhost",
    output_mqtt_port: int = 1883,
    output_mqtt_username: Optional[str] = None,
    output_mqtt_password: Optional[str] = None,
    output_mqtt_tls: bool = False,
    output_prefix: str = "meshcore",
) -> ReceiverMeshcoreToMqtt:
    """Create a configured :class:`ReceiverMeshcoreToMqtt` instance.

    Args:
        source_mqtt_host: Hostname of the MQTT broker where meshcoretomqtt
            publishes (default ``"localhost"``).
        source_mqtt_port: Port of the source MQTT broker.
        source_mqtt_username: Optional username for the source broker.
        source_mqtt_password: Optional password for the source broker.
        source_mqtt_tls: Enable TLS/SSL for the source broker connection.
        source_prefix: Topic prefix used by meshcoretomqtt (default
            ``"meshcore"``).  Matches the ``[topics]`` section of the
            meshcoretomqtt ``config.toml``.
        source_iata_filter: Restrict subscriptions to a single IATA location
            code.  ``None`` subscribes to all locations.
        output_mqtt_host: Hostname of the MQTT broker where the meshcore-hub
            collector is listening (default ``"localhost"``).
        output_mqtt_port: Port of the output MQTT broker.
        output_mqtt_username: Optional username for the output broker.
        output_mqtt_password: Optional password for the output broker.
        output_mqtt_tls: Enable TLS/SSL for the output broker connection.
        output_prefix: Topic prefix used by meshcore-hub events (default
            ``"meshcore"``).

    Returns:
        A configured :class:`ReceiverMeshcoreToMqtt` ready to call
        :meth:`~ReceiverMeshcoreToMqtt.run`.
    """
    source_config = MQTTConfig(
        host=source_mqtt_host,
        port=source_mqtt_port,
        username=source_mqtt_username,
        password=source_mqtt_password,
        prefix=source_prefix,
        client_id=f"meshcore-mc2mqtt-source-{source_mqtt_host}",
        tls=source_mqtt_tls,
    )
    source_client = MQTTClient(source_config)

    output_config = MQTTConfig(
        host=output_mqtt_host,
        port=output_mqtt_port,
        username=output_mqtt_username,
        password=output_mqtt_password,
        prefix=output_prefix,
        client_id=f"meshcore-mc2mqtt-output-{output_mqtt_host}",
        tls=output_mqtt_tls,
    )
    output_client = MQTTClient(output_config)

    return ReceiverMeshcoreToMqtt(
        source_mqtt=source_client,
        output_mqtt=output_client,
        source_prefix=source_prefix,
        source_iata_filter=source_iata_filter,
    )


def run_receiver_mc2mqtt(
    source_mqtt_host: str = "localhost",
    source_mqtt_port: int = 1883,
    source_mqtt_username: Optional[str] = None,
    source_mqtt_password: Optional[str] = None,
    source_mqtt_tls: bool = False,
    source_prefix: str = "meshcore",
    source_iata_filter: Optional[str] = None,
    output_mqtt_host: str = "localhost",
    output_mqtt_port: int = 1883,
    output_mqtt_username: Optional[str] = None,
    output_mqtt_password: Optional[str] = None,
    output_mqtt_tls: bool = False,
    output_prefix: str = "meshcore",
) -> None:
    """Run the meshcoretomqtt receiver (blocking).

    This is the main entry point called from the CLI.

    All arguments are passed through to :func:`create_receiver_mc2mqtt`.
    """
    receiver = create_receiver_mc2mqtt(
        source_mqtt_host=source_mqtt_host,
        source_mqtt_port=source_mqtt_port,
        source_mqtt_username=source_mqtt_username,
        source_mqtt_password=source_mqtt_password,
        source_mqtt_tls=source_mqtt_tls,
        source_prefix=source_prefix,
        source_iata_filter=source_iata_filter,
        output_mqtt_host=output_mqtt_host,
        output_mqtt_port=output_mqtt_port,
        output_mqtt_username=output_mqtt_username,
        output_mqtt_password=output_mqtt_password,
        output_mqtt_tls=output_mqtt_tls,
        output_prefix=output_prefix,
    )

    def _signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received signal %s – stopping receiver", signum)
        receiver.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    receiver.run()
