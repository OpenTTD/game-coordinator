import asyncio
import click
import logging

from openttd_helpers import click_helper
from openttd_helpers.logging_helper import click_logging
from openttd_helpers.sentry_helper import click_sentry

from openttd_protocol.protocol.coordinator import CoordinatorProtocol

from .application.coordinator import Application as CoordinatorApplication
from .database.redis import click_database_redis

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
@click.option(
    "--db",
    type=click.Choice(["redis"], case_sensitive=False),
    required=True,
    callback=click_helper.import_module("game_coordinator.database", "Database"),
)
@click_database_redis
def main(bind, coordinator_port, db):
    loop = asyncio.get_event_loop()

    db_instance = db()
    loop.run_until_complete(db_instance.startup())

    app_instance = CoordinatorApplication(db_instance)

    server = loop.run_until_complete(run_server(app_instance, bind, coordinator_port, CoordinatorProtocol))

    try:
        loop.run_until_complete(server.serve_forever())
    except KeyboardInterrupt:
        pass

    log.info("Shutting down game_coordinator ...")
    server.close()


if __name__ == "__main__":
    main(auto_envvar_prefix="GAME_COORDINATOR")
