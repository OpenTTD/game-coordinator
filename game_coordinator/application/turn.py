import asyncio
import click
import logging
import time

from openttd_helpers import click_helper
from prometheus_client import (
    Counter,
    Gauge,
    Summary,
)

log = logging.getLogger(__name__)

TTL = 15
TIMEOUT = 20  # After how many seconds we give up on connecting client and server.

_turn_address = None


class Application:
    NAME = "turn"

    def __init__(self, database):
        if not _turn_address:
            raise Exception("Please set --turn-address for this application")

        self.database = database
        self.turn_address = _turn_address

        self.stats_turn_bytes = Summary("coordinator_turn_tcp_bytes", "Bytes relayed")
        self.stats_turn_connections = Gauge(
            "coordinator_turn_tcp_connections", "Amount of connections currently being relayed"
        )
        self.stats_turn_duration = Summary("coordinator_turn_tcp_duration", "How long the connection was established")
        self.stats_turn_ticket = Counter("coordinator_turn_tcp_ticket", "Amount of tickets processed", ["reason"])

        self._ticket_pair = {}
        self._ticket_task = {}
        self._active_sources = set()
        self._shutdown = None

        log.info("Starting TURN server for %s ...", _turn_address)

    async def startup(self):
        asyncio.create_task(self._guard(self._keep_turn_server_alive()))

    async def shutdown(self):
        log.info("Shutting down TURN server ...")
        self._shutdown = asyncio.Event()

        # Wait till all relay sessions are terminated.
        while self._active_sources:
            log.info(f"{len(self._active_sources) // 2} active connections left, waiting ...")
            # Update every disconnect and every 30s since last disconnect how we are doing.
            self._shutdown.clear()
            try:
                await asyncio.wait_for(self._shutdown.wait(), 30)
            except asyncio.TimeoutError:
                # Timeout; print a message and continue.
                pass

    def disconnect(self, source):
        if source not in self._active_sources:
            return

        # Only do this administration on one side.
        self._active_sources.remove(source)
        self._active_sources.remove(source.peer)

        # Make sure we close the other side too.
        source.peer.protocol.transport.close()

        self.stats_turn_duration.observe(time.time() - source.connected_since)
        self.stats_turn_connections.dec()

        if self._shutdown:
            self._shutdown.set()

    async def _guard(self, coroutine):
        try:
            await coroutine
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("System co-routine failed, killing server ..")

            import sys

            sys.exit(1)

    async def _keep_turn_server_alive(self):
        # Update the redis key every 10 seconds. The TTL of the key is set at
        # 15 seconds. If for what-ever reason we stop working, we will miss
        # our deadline, redis will expire our key, which informs the cluster
        # we are no longer available for serving any request.
        while self._shutdown is None:
            await self.database.announce_turn_server(self.turn_address)
            await asyncio.sleep(10)

    async def receive_PACKET_TURN_SERCLI_CONNECT(self, source, protocol_version, ticket):
        if not await self.database.validate_turn_ticket(ticket):
            source.protocol.transport.close()
            self.stats_turn_ticket.labels(reason="invalid").inc()
            return

        source.connected_since = time.time()

        if ticket not in self._ticket_pair:
            self._ticket_pair[ticket] = source
            self._ticket_task[ticket] = asyncio.create_task(self._expire_ticket(ticket))
            return

        # Found a pair; cancel the expire task.
        self._ticket_task[ticket].cancel()
        del self._ticket_task[ticket]

        # Match the pair together.
        source.peer = self._ticket_pair[ticket]
        self._ticket_pair[ticket].peer = source

        self._active_sources.add(source)
        self._active_sources.add(source.peer)

        # Inform both parties we are now relaying.
        await source.protocol.send_PACKET_TURN_TURN_CONNECTED(protocol_version, str(self._ticket_pair[ticket].ip))
        await self._ticket_pair[ticket].protocol.send_PACKET_TURN_TURN_CONNECTED(protocol_version, str(source.ip))

        self.stats_turn_ticket.labels(reason="paired").inc()
        self.stats_turn_connections.inc()

        del self._ticket_pair[ticket]

    async def _expire_ticket(self, ticket):
        await asyncio.sleep(TIMEOUT)

        self._ticket_pair[ticket].protocol.transport.close()
        self.stats_turn_ticket.labels(reason="expired").inc()

        # Expire the ticket after 10 seconds if there was no match.
        del self._ticket_pair[ticket]
        del self._ticket_task[ticket]

    async def receive_raw(self, source, data):
        if not hasattr(source, "peer"):
            return False

        self.stats_turn_bytes.observe(len(data))

        # Send out all incoming packets to the other side.
        await source.peer.protocol.send_packet(data)
        return True


@click_helper.extend
@click.option(
    "--turn-address",
    help="Full address of TURN server as seen by remote users.",
    default="",
)
def click_application_turn(turn_address):
    global _turn_address

    _turn_address = turn_address
