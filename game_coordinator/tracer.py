import asyncio
import beeline
import click
import logging
import math

from collections import defaultdict
from openttd_helpers import click_helper

log = logging.getLogger(__name__)


MAX_INT32 = math.pow(2, 32) - 1

TRACES_PER_HOUR = 5
TRACES_SAMPLE_RATE = 10


class Sample:
    def __init__(self):
        self._traces_per_hour = TRACES_PER_HOUR
        self._sample_rate = TRACES_SAMPLE_RATE

        self._accepted = [0] * 60
        self._total = [0] * 60
        self._bucket = 1
        self._traces = {}

    def sampler(self, trace_id, parent_id):
        # A new trace. Calculate if we want to sample it.
        if trace_id not in self._traces:
            self._total[0] += 1
            self._traces[trace_id] = False

            # Check if we can and want to sample this trace.
            if self._bucket >= 1 and int(trace_id[:8], 16) < MAX_INT32 / self._sample_rate:
                self._bucket -= 1
                self._accepted[0] += 1

                self._traces[trace_id] = True

        # Calculate the result and sample-rate.
        result = self._traces[trace_id]
        sample_rate = sum(self._total) // sum(self._accepted) if result else 0

        # This is the last event of this trace.
        if parent_id is None:
            del self._traces[trace_id]

        return result, sample_rate

    def fill_bucket(self, delta):
        if self._bucket < self._traces_per_hour:
            self._bucket += self._traces_per_hour * delta / 3600

    def update_window(self):
        self._accepted[:] = [0] + self._accepted[:-1]
        self._total[:] = [0] + self._total[:-1]


samples = defaultdict(Sample)


def beeline_sampler(event):
    # Some redis backend calls are not called from a trace (like disconnect).
    # Simply do not sample these traces.
    if "app.command" not in event:
        return False, 0

    return samples[event["app.command"]].sampler(event["trace.trace_id"], event["trace.parent_id"])


async def fill_bucket():
    while True:
        await asyncio.sleep(5)

        for sample in samples.values():
            sample.fill_bucket(5)


async def update_window():
    while True:
        await asyncio.sleep(60)

        for sample in samples.values():
            sample.update_window()


async def tracer_init(honeycomb_api_key, honeycomb_service_name):
    beeline.init(
        writekey=honeycomb_api_key,
        dataset="game-coordinator",
        service_name=honeycomb_service_name,
        sampler_hook=beeline_sampler,
    )
    asyncio.create_task(fill_bucket())
    asyncio.create_task(update_window())
    log.info("Honeycomb initialized with traces-per-hour=%d and sample-rate=%d", TRACES_PER_HOUR, TRACES_SAMPLE_RATE)


@click_helper.extend
@click.option("--honeycomb-api-key", help="Honeycomb API key.")
@click.option(
    "--honeycomb-service-name",
    help="Honeycomb service name to use.",
    default="coordinator",
    show_default=True,
)
@click.option(
    "--honeycomb-rate-limit",
    help="How many traces per command per hour to send to Honeycomb.",
    default=5,
    show_default=True,
)
@click.option(
    "--honeycomb-sample-rate",
    help="The sample rate of traces to send to Honeycomb.",
    default=10,
    show_default=True,
)
def click_tracer(honeycomb_api_key, honeycomb_service_name, honeycomb_rate_limit, honeycomb_sample_rate):
    global TRACES_PER_HOUR, TRACES_SAMPLE_RATE

    if not honeycomb_api_key:
        return

    TRACES_PER_HOUR = int(honeycomb_rate_limit)
    TRACES_SAMPLE_RATE = int(honeycomb_sample_rate)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tracer_init(honeycomb_api_key, honeycomb_service_name))
