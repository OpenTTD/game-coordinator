import asyncio
import click
import logging

from openttd_helpers import click_helper
from openttd_helpers.logging_helper import click_logging
from openttd_helpers.sentry_helper import click_sentry

from openttd_protocol.protocol.coordinator import CoordinatorProtocol

from .application.coordinator import Application as CoordinatorApplication
from .database.redis import click_database_redis
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


@click_helper.command()
@click_logging  # Should always be on top, as it initializes the logging
@click_sentry
@click.option(
    "--bind", help="The IP to bind the server to", multiple=True, default=["::1", "127.0.0.1"], show_default=True
)
@click.option("--coordinator-port", help="Port of the Game Coordinator", default=3976, show_default=True)
@click.option("--web-port", help="Port of the web server.", default=80, show_default=True, metavar="PORT")
@click.option("--shared-secret", help="Shared secret to validate invite-code-secrets with", required=True)
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
@click.option(
    "--socks-proxy",
    help="Use a SOCKS proxy to query game servers.",
)
@click_database_redis
def main(bind, coordinator_port, web_port, shared_secret, db, proxy_protocol, socks_proxy):
    loop = asyncio.get_event_loop()

    db_instance = db()
    loop.run_until_complete(db_instance.startup())

    app_instance = CoordinatorApplication(shared_secret, db_instance, socks_proxy)

    CoordinatorProtocol.proxy_protocol = proxy_protocol

    server = loop.run_until_complete(run_server(app_instance, bind, coordinator_port, CoordinatorProtocol))

    try:
        start_webserver(bind, web_port)
    except KeyboardInterrupt:
        pass

    log.info("Shutting down game_coordinator ...")
    server.close()


if __name__ == "__main__":
    main(auto_envvar_prefix="GAME_COORDINATOR")
