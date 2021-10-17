import asyncio
import click
import logging
import signal

from openttd_helpers import click_helper
from openttd_helpers.logging_helper import click_logging
from openttd_helpers.sentry_helper import click_sentry

from openttd_protocol.protocol.coordinator import CoordinatorProtocol
from openttd_protocol.protocol.stun import StunProtocol
from openttd_protocol.protocol.turn import TurnProtocol

from .application.coordinator import (
    Application as CoordinatorApplication,
    click_application_coordinator,
)
from .application.turn import (
    Application as TurnApplication,
    click_application_turn,
)
from .database.redis import click_database_redis
from .tracer import click_tracer
from .web import start_webserver

log = logging.getLogger(__name__)


async def run_server(application, bind, port, ProtocolClass):
    loop = asyncio.get_event_loop()

    server = await loop.create_server(
        lambda: ProtocolClass(application),
        host=bind,
        port=port,
        reuse_port=True,
        start_serving=True,
    )
    log.info(f"Listening on {bind}:{port} ...")

    return server


async def close_server(loop, app_instance, server):
    # Stop accepting new connections.
    server.close()
    await server.wait_closed()

    # Shut down the application, allowing it to do cleanup.
    await app_instance.shutdown()

    # Cancel all the remaining tasks and wait for them to have stopped.
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    # Wait for all tasks to actually cancel.
    await asyncio.gather(*tasks, return_exceptions=True)

    # We should now be able to stop the loop safely.
    log.info("Server gracefully shut down")
    loop.stop()


@click_helper.command()
@click_logging  # Should always be on top, as it initializes the logging
@click_sentry
@click_tracer
@click.option(
    "--bind", help="The IP to bind the server to", multiple=True, default=["::1", "127.0.0.1"], show_default=True
)
@click.option("--coordinator-port", help="Port of the Game Coordinator", default=3976, show_default=True)
@click.option("--stun-port", help="Port of the STUN server", default=3975, show_default=True)
@click.option("--turn-port", help="Port of the TURN server", default=3974, show_default=True)
@click.option("--web-port", help="Port of the web server.", default=80, show_default=True, metavar="PORT")
@click.option(
    "--app",
    type=click.Choice(["coordinator", "stun", "turn"], case_sensitive=False),
    required=True,
    callback=click_helper.import_module("game_coordinator.application", "Application"),
)
@click.option(
    "--db",
    type=click.Choice(["redis"], case_sensitive=False),
    required=True,
    callback=click_helper.import_module("game_coordinator.database", "Database"),
)
@click.option(
    "--proxy-protocol",
    help="Enable Proxy Protocol (v1), and expect all incoming streams to have this header "
    "(HINT: for nginx, configure proxy_requests to 1).",
    is_flag=True,
)
@click_database_redis
@click_application_coordinator
@click_application_turn
def main(
    bind,
    app,
    coordinator_port,
    stun_port,
    turn_port,
    web_port,
    db,
    proxy_protocol,
):
    loop = asyncio.get_event_loop()

    db_instance = db()
    app_instance = app(db_instance)
    loop.run_until_complete(app_instance.startup())

    CoordinatorProtocol.proxy_protocol = proxy_protocol
    StunProtocol.proxy_protocol = proxy_protocol
    TurnProtocol.proxy_protocol = proxy_protocol

    if isinstance(app_instance, CoordinatorApplication):
        port = coordinator_port
        protocol = CoordinatorProtocol
    elif isinstance(app_instance, TurnApplication):
        port = turn_port
        protocol = TurnProtocol
    else:
        port = stun_port
        protocol = StunProtocol

    server = loop.run_until_complete(run_server(app_instance, bind, port, protocol))

    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(close_server(loop, app_instance, server)))
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(close_server(loop, app_instance, server)))

    start_webserver(bind, web_port, db_instance)
    loop.run_forever()


if __name__ == "__main__":
    main(auto_envvar_prefix="GAME_COORDINATOR")
