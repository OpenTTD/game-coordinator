import asyncio
import beeline
import click
import logging
import random
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
from .web import start_webserver

TRACES_PER_HOUR = 100
TRACES_SAMPLE_RATE = 10

log = logging.getLogger(__name__)

# Current active samples.
samples = {}
# Bucket for the amount of allowed samples to go out.
samples_bucket = 0.0
# Window-based stats about how many samples were send and how many were there
# in total. Used to estimate the sample rate.
samples_accepted = [0] * 60
samples_total = [0] * 60


def beeline_sampler(event):
    global samples_bucket, samples_skipped

    trace_id = event["trace.trace_id"]

    # New trace. Check if we want to sample it.
    if trace_id not in samples:
        samples_total[0] += 1
        samples[trace_id] = False

        # Check if we can send this trace.
        if samples_bucket > 1 and random.randint(1, TRACES_SAMPLE_RATE) == 1:
            samples_bucket -= 1
            samples_accepted[0] += 1

            samples[trace_id] = True

    # Calculate the result and sample-rate.
    result = samples[trace_id]
    sample_rate = sum(samples_total) // sum(samples_accepted) if result else 0

    # This trace is closing. So forget any information about it.
    if event["trace.parent_id"] is None:
        del samples[trace_id]

    return result, sample_rate


async def fill_samples_bucket():
    global samples_bucket

    count = 0

    # Every five seconds, fill the bucket a bit, so we can sample randomly
    # during the hour.
    # Every minute, move the window of samples_accepted / sample_total.
    while True:
        await asyncio.sleep(5)

        # Don't overflow the bucket past the size of an hour.
        if samples_bucket < TRACES_PER_HOUR:
            samples_bucket += TRACES_PER_HOUR * 5 / 3600

        # Update the samples-stats every minute.
        count += 1
        if count > 60 / 5:
            count = 0

            # Move the window of the samples-stats.
            samples_accepted[:] = [0] + samples_accepted[:-1]
            samples_total[:] = [0] + samples_total[:-1]


async def run_server(application, bind, port, ProtocolClass, honeycomb_api_key):
    if honeycomb_api_key:
        beeline.init(
            writekey=honeycomb_api_key,
            dataset="game-coordinator",
            service_name=application.name,
            sampler_hook=beeline_sampler,
        )
        asyncio.create_task(fill_samples_bucket())
        log.info("Honeycomb beeline initialized")

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
@click.option("--honeycomb-api-key", help="Honeycomb API key.")
@click.option(
    "--honeycomb-rate-limit", help="How many traces to send to Honeycomb per hour.", default=100, show_default=True
)
@click.option(
    "--honeycomb-sample-rate", help="The sample rate of traces to send to Honeycomb.", default=10, show_default=True
)
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
    honeycomb_api_key,
    honeycomb_rate_limit,
    honeycomb_sample_rate,
    bind,
    app,
    coordinator_port,
    stun_port,
    turn_port,
    web_port,
    db,
    proxy_protocol,
):
    global TRACES_PER_HOUR, TRACES_SAMPLE_RATE

    TRACES_PER_HOUR = int(honeycomb_rate_limit)
    TRACES_SAMPLE_RATE = int(honeycomb_sample_rate)

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

    server = loop.run_until_complete(run_server(app_instance, bind, port, protocol, honeycomb_api_key))

    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(close_server(loop, app_instance, server)))
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(close_server(loop, app_instance, server)))

    start_webserver(bind, web_port, db_instance)
    loop.run_forever()


if __name__ == "__main__":
    main(auto_envvar_prefix="GAME_COORDINATOR")
