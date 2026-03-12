"""CLI for the Interface component."""

import click

from meshcore_hub.common.logging import configure_logging


@click.group()
def interface() -> None:
    """Interface component for MeshCore device communication.

    Runs in RECEIVER or SENDER mode to bridge between
    MeshCore devices and MQTT broker.
    """
    pass


@interface.command("run")
@click.option(
    "--mode",
    type=click.Choice(["RECEIVER", "SENDER"], case_sensitive=False),
    required=True,
    envvar="INTERFACE_MODE",
    help="Interface mode: RECEIVER or SENDER",
)
@click.option(
    "--port",
    type=str,
    default="/dev/ttyUSB0",
    envvar="SERIAL_PORT",
    help="Serial port path",
)
@click.option(
    "--baud",
    type=int,
    default=115200,
    envvar="SERIAL_BAUD",
    help="Serial baud rate",
)
@click.option(
    "--mock",
    is_flag=True,
    default=False,
    envvar="MOCK_DEVICE",
    help="Use mock device for testing",
)
@click.option(
    "--node-address",
    type=str,
    default=None,
    envvar="NODE_ADDRESS",
    help="Override for device public key/address (hex string)",
)
@click.option(
    "--device-name",
    type=str,
    default=None,
    envvar="MESHCORE_DEVICE_NAME",
    help="Device/node name (optional)",
)
@click.option(
    "--mqtt-host",
    type=str,
    default="localhost",
    envvar="MQTT_HOST",
    help="MQTT broker host",
)
@click.option(
    "--mqtt-port",
    type=int,
    default=1883,
    envvar="MQTT_PORT",
    help="MQTT broker port",
)
@click.option(
    "--mqtt-username",
    type=str,
    default=None,
    envvar="MQTT_USERNAME",
    help="MQTT username",
)
@click.option(
    "--mqtt-password",
    type=str,
    default=None,
    envvar="MQTT_PASSWORD",
    help="MQTT password",
)
@click.option(
    "--prefix",
    type=str,
    default="meshcore",
    envvar="MQTT_PREFIX",
    help="MQTT topic prefix",
)
@click.option(
    "--mqtt-tls",
    is_flag=True,
    default=False,
    envvar="MQTT_TLS",
    help="Enable TLS/SSL for MQTT connection",
)
@click.option(
    "--contact-cleanup/--no-contact-cleanup",
    default=True,
    envvar="CONTACT_CLEANUP_ENABLED",
    help="Enable/disable automatic removal of stale contacts (RECEIVER mode only)",
)
@click.option(
    "--contact-cleanup-days",
    type=int,
    default=7,
    envvar="CONTACT_CLEANUP_DAYS",
    help="Remove contacts not advertised for this many days (RECEIVER mode only)",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    envvar="LOG_LEVEL",
    help="Log level",
)
def run(
    mode: str,
    port: str,
    baud: int,
    mock: bool,
    node_address: str | None,
    device_name: str | None,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
    prefix: str,
    mqtt_tls: bool,
    contact_cleanup: bool,
    contact_cleanup_days: int,
    log_level: str,
) -> None:
    """Run the interface component.

    The interface bridges MeshCore devices to an MQTT broker.

    In RECEIVER mode:
    - Connects to a MeshCore device
    - Subscribes to device events
    - Publishes events to MQTT

    In SENDER mode:
    - Connects to a MeshCore device
    - Subscribes to MQTT command topics
    - Executes commands on the device
    """
    configure_logging(level=log_level)

    click.echo(f"Starting interface in {mode} mode")
    click.echo(f"Serial: {port} @ {baud} baud")
    click.echo(f"MQTT: {mqtt_host}:{mqtt_port} (prefix: {prefix})")
    click.echo(f"Mock device: {mock}")
    if node_address:
        click.echo(f"Node address: {node_address}")

    mode_upper = mode.upper()

    if mode_upper == "RECEIVER":
        from meshcore_hub.interface.receiver import run_receiver

        run_receiver(
            port=port,
            baud=baud,
            mock=mock,
            node_address=node_address,
            device_name=device_name,
            mqtt_host=mqtt_host,
            mqtt_port=mqtt_port,
            mqtt_username=mqtt_username,
            mqtt_password=mqtt_password,
            mqtt_prefix=prefix,
            mqtt_tls=mqtt_tls,
            contact_cleanup_enabled=contact_cleanup,
            contact_cleanup_days=contact_cleanup_days,
        )
    elif mode_upper == "SENDER":
        from meshcore_hub.interface.sender import run_sender

        run_sender(
            port=port,
            baud=baud,
            mock=mock,
            node_address=node_address,
            device_name=device_name,
            mqtt_host=mqtt_host,
            mqtt_port=mqtt_port,
            mqtt_username=mqtt_username,
            mqtt_password=mqtt_password,
            mqtt_prefix=prefix,
            mqtt_tls=mqtt_tls,
        )
    else:
        click.echo(f"Unknown mode: {mode}", err=True)
        raise click.Abort()


@interface.command("receiver")
@click.option(
    "--port",
    type=str,
    default="/dev/ttyUSB0",
    envvar="SERIAL_PORT",
    help="Serial port path",
)
@click.option(
    "--baud",
    type=int,
    default=115200,
    envvar="SERIAL_BAUD",
    help="Serial baud rate",
)
@click.option(
    "--mock",
    is_flag=True,
    default=False,
    envvar="MOCK_DEVICE",
    help="Use mock device for testing",
)
@click.option(
    "--node-address",
    type=str,
    default=None,
    envvar="NODE_ADDRESS",
    help="Override for device public key/address (hex string)",
)
@click.option(
    "--device-name",
    type=str,
    default=None,
    envvar="MESHCORE_DEVICE_NAME",
    help="Device/node name (optional)",
)
@click.option(
    "--mqtt-host",
    type=str,
    default="localhost",
    envvar="MQTT_HOST",
    help="MQTT broker host",
)
@click.option(
    "--mqtt-port",
    type=int,
    default=1883,
    envvar="MQTT_PORT",
    help="MQTT broker port",
)
@click.option(
    "--mqtt-username",
    type=str,
    default=None,
    envvar="MQTT_USERNAME",
    help="MQTT username",
)
@click.option(
    "--mqtt-password",
    type=str,
    default=None,
    envvar="MQTT_PASSWORD",
    help="MQTT password",
)
@click.option(
    "--prefix",
    type=str,
    default="meshcore",
    envvar="MQTT_PREFIX",
    help="MQTT topic prefix",
)
@click.option(
    "--mqtt-tls",
    is_flag=True,
    default=False,
    envvar="MQTT_TLS",
    help="Enable TLS/SSL for MQTT connection",
)
@click.option(
    "--contact-cleanup/--no-contact-cleanup",
    default=True,
    envvar="CONTACT_CLEANUP_ENABLED",
    help="Enable/disable automatic removal of stale contacts",
)
@click.option(
    "--contact-cleanup-days",
    type=int,
    default=7,
    envvar="CONTACT_CLEANUP_DAYS",
    help="Remove contacts not advertised for this many days",
)
def receiver(
    port: str,
    baud: int,
    mock: bool,
    node_address: str | None,
    device_name: str | None,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
    prefix: str,
    mqtt_tls: bool,
    contact_cleanup: bool,
    contact_cleanup_days: int,
) -> None:
    """Run interface in RECEIVER mode.

    Shortcut for: meshcore-hub interface run --mode RECEIVER
    """
    from meshcore_hub.interface.receiver import run_receiver

    click.echo("Starting interface in RECEIVER mode")
    click.echo(f"Serial: {port} @ {baud} baud")
    click.echo(f"MQTT: {mqtt_host}:{mqtt_port} (prefix: {prefix})")
    click.echo(f"Mock device: {mock}")
    if node_address:
        click.echo(f"Node address: {node_address}")

    run_receiver(
        port=port,
        baud=baud,
        mock=mock,
        node_address=node_address,
        device_name=device_name,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_prefix=prefix,
        mqtt_tls=mqtt_tls,
        contact_cleanup_enabled=contact_cleanup,
        contact_cleanup_days=contact_cleanup_days,
    )


@interface.command("sender")
@click.option(
    "--port",
    type=str,
    default="/dev/ttyUSB0",
    envvar="SERIAL_PORT",
    help="Serial port path",
)
@click.option(
    "--baud",
    type=int,
    default=115200,
    envvar="SERIAL_BAUD",
    help="Serial baud rate",
)
@click.option(
    "--mock",
    is_flag=True,
    default=False,
    envvar="MOCK_DEVICE",
    help="Use mock device for testing",
)
@click.option(
    "--node-address",
    type=str,
    default=None,
    envvar="NODE_ADDRESS",
    help="Override for device public key/address (hex string)",
)
@click.option(
    "--device-name",
    type=str,
    default=None,
    envvar="MESHCORE_DEVICE_NAME",
    help="Device/node name (optional)",
)
@click.option(
    "--mqtt-host",
    type=str,
    default="localhost",
    envvar="MQTT_HOST",
    help="MQTT broker host",
)
@click.option(
    "--mqtt-port",
    type=int,
    default=1883,
    envvar="MQTT_PORT",
    help="MQTT broker port",
)
@click.option(
    "--mqtt-username",
    type=str,
    default=None,
    envvar="MQTT_USERNAME",
    help="MQTT username",
)
@click.option(
    "--mqtt-password",
    type=str,
    default=None,
    envvar="MQTT_PASSWORD",
    help="MQTT password",
)
@click.option(
    "--prefix",
    type=str,
    default="meshcore",
    envvar="MQTT_PREFIX",
    help="MQTT topic prefix",
)
@click.option(
    "--mqtt-tls",
    is_flag=True,
    default=False,
    envvar="MQTT_TLS",
    help="Enable TLS/SSL for MQTT connection",
)
def sender(
    port: str,
    baud: int,
    mock: bool,
    node_address: str | None,
    device_name: str | None,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
    prefix: str,
    mqtt_tls: bool,
) -> None:
    """Run interface in SENDER mode.

    Shortcut for: meshcore-hub interface run --mode SENDER
    """
    from meshcore_hub.interface.sender import run_sender

    click.echo("Starting interface in SENDER mode")
    click.echo(f"Serial: {port} @ {baud} baud")
    click.echo(f"MQTT: {mqtt_host}:{mqtt_port} (prefix: {prefix})")
    click.echo(f"Mock device: {mock}")
    if node_address:
        click.echo(f"Node address: {node_address}")

    run_sender(
        port=port,
        baud=baud,
        mock=mock,
        node_address=node_address,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_prefix=prefix,
        mqtt_tls=mqtt_tls,
    )


# ---------------------------------------------------------------------------
# receiver-mc2mqtt command
# Bridges meshcoretomqtt MQTT topics into meshcore-hub event format.
# All implementation lives in receiver_meshcoretomqtt.py (new helper file).
# ---------------------------------------------------------------------------


@interface.command("receiver-mc2mqtt")
@click.option(
    "--source-mqtt-host",
    type=str,
    default="localhost",
    envvar="MC2MQTT_SOURCE_HOST",
    help="Hostname of MQTT broker where meshcoretomqtt publishes",
)
@click.option(
    "--source-mqtt-port",
    type=int,
    default=1883,
    envvar="MC2MQTT_SOURCE_PORT",
    help="Port of source MQTT broker",
)
@click.option(
    "--source-mqtt-username",
    type=str,
    default=None,
    envvar="MC2MQTT_SOURCE_USERNAME",
    help="Username for source MQTT broker",
)
@click.option(
    "--source-mqtt-password",
    type=str,
    default=None,
    envvar="MC2MQTT_SOURCE_PASSWORD",
    help="Password for source MQTT broker",
)
@click.option(
    "--source-mqtt-tls",
    is_flag=True,
    default=False,
    envvar="MC2MQTT_SOURCE_TLS",
    help="Enable TLS/SSL for source MQTT connection",
)
@click.option(
    "--source-prefix",
    type=str,
    default="meshcore",
    envvar="MC2MQTT_SOURCE_PREFIX",
    help="Topic prefix used by meshcoretomqtt (default: meshcore)",
)
@click.option(
    "--source-iata",
    type=str,
    default=None,
    envvar="MC2MQTT_SOURCE_IATA",
    help="Restrict to a single IATA location code (default: all locations)",
)
@click.option(
    "--mqtt-host",
    type=str,
    default="localhost",
    envvar="MQTT_HOST",
    help="Hostname of output MQTT broker (meshcore-hub collector)",
)
@click.option(
    "--mqtt-port",
    type=int,
    default=1883,
    envvar="MQTT_PORT",
    help="Port of output MQTT broker",
)
@click.option(
    "--mqtt-username",
    type=str,
    default=None,
    envvar="MQTT_USERNAME",
    help="Username for output MQTT broker",
)
@click.option(
    "--mqtt-password",
    type=str,
    default=None,
    envvar="MQTT_PASSWORD",
    help="Password for output MQTT broker",
)
@click.option(
    "--mqtt-tls",
    is_flag=True,
    default=False,
    envvar="MQTT_TLS",
    help="Enable TLS/SSL for output MQTT connection",
)
@click.option(
    "--prefix",
    type=str,
    default="meshcore",
    envvar="MQTT_PREFIX",
    help="Topic prefix for meshcore-hub output events",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    envvar="LOG_LEVEL",
    help="Log level",
)
def receiver_mc2mqtt(
    source_mqtt_host: str,
    source_mqtt_port: int,
    source_mqtt_username: str | None,
    source_mqtt_password: str | None,
    source_mqtt_tls: bool,
    source_prefix: str,
    source_iata: str | None,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
    mqtt_tls: bool,
    prefix: str,
    log_level: str,
) -> None:
    """Run a receiver that bridges meshcoretomqtt MQTT data to meshcore-hub.

    Subscribes to topics published by the `meshcoretomqtt` tool and
    re-publishes them in the standard meshcore-hub event format so the
    collector can store RF packet stats and node status information.

    Source topics (meshcoretomqtt)::

        {source_prefix}/{IATA}/{PUBLIC_KEY}/packets
        {source_prefix}/{IATA}/{PUBLIC_KEY}/status
        {source_prefix}/{IATA}/{PUBLIC_KEY}/debug

    Output topics (meshcore-hub)::

        {prefix}/{PUBLIC_KEY}/event/advertisement   (from status)
        {prefix}/{PUBLIC_KEY}/event/packet_log       (from packets)
        {prefix}/{PUBLIC_KEY}/event/debug_log        (from debug)

    See https://github.com/Cisien/meshcoretomqtt for source details.
    """
    from meshcore_hub.interface.receiver_meshcoretomqtt import run_receiver_mc2mqtt

    configure_logging(level=log_level)

    click.echo("Starting meshcoretomqtt receiver")
    click.echo(
        f"Source MQTT: {source_mqtt_host}:{source_mqtt_port} (prefix: {source_prefix})"
    )
    if source_iata:
        click.echo(f"Source IATA filter: {source_iata}")
    click.echo(f"Output MQTT: {mqtt_host}:{mqtt_port} (prefix: {prefix})")

    run_receiver_mc2mqtt(
        source_mqtt_host=source_mqtt_host,
        source_mqtt_port=source_mqtt_port,
        source_mqtt_username=source_mqtt_username,
        source_mqtt_password=source_mqtt_password,
        source_mqtt_tls=source_mqtt_tls,
        source_prefix=source_prefix,
        source_iata_filter=source_iata,
        output_mqtt_host=mqtt_host,
        output_mqtt_port=mqtt_port,
        output_mqtt_username=mqtt_username,
        output_mqtt_password=mqtt_password,
        output_mqtt_tls=mqtt_tls,
        output_prefix=prefix,
    )
