import asyncio
import click
import logging
import time

from openttd_helpers import click_helper

log = logging.getLogger(__name__)

TTL = 15
TIMEOUT = 20  # After how many seconds we give up on connecting client and server.

_turn_address = None


class Application:
    def __init__(self, database):
        if not _turn_address:
            raise Exception("Please set --turn-address for this application")

        self.database = database
        self.turn_address = _turn_address

        self._ticket_pair = {}
        self._ticket_task = {}
        self._active_sources = set()

        log.info("Starting TURN server for %s ...", _turn_address)

    async def startup(self):
        asyncio.create_task(self._guard(self._keep_turn_server_alive()))
        asyncio.create_task(self._guard(self._update_stats()))

    async def shutdown(self):
        log.info("Shutting down TURN server ...")

        for source in self._active_sources:
            source.protocol.transport.close()

    def disconnect(self, source):
        if source not in self._active_sources:
            return
        self._active_sources.remove(source)

        # Make sure we close the other side too.
        source.peer.protocol.transport.close()

        asyncio.create_task(self.database.stats_turn_usage(source.total_bytes, time.time() - source.connected_since))
        source.total_bytes = 0
        source.connected_since = time.time()

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
        while True:
            await self.database.announce_turn_server(self.turn_address)
            await asyncio.sleep(10)

    async def _update_stats(self):
        # On a regular interval, update the TURN usage based on the current
        # active connections. This is especially useful for people who keep
        # their connection open for days.
        while True:
            await asyncio.sleep(1800)

            for source in list(self._active_sources):
                if source.total_bytes == 0:
                    continue

                total_bytes = source.total_bytes
                source.total_bytes = 0

                connected_since = source.connected_since
                source.connected_since = time.time()

                await self.database.stats_turn_usage(total_bytes, time.time() - connected_since)

    async def receive_PACKET_TURN_SERCLI_CONNECT(self, source, protocol_version, ticket):
        if not await self.database.validate_turn_ticket(ticket):
            source.protocol.transport.close()
            await self.database.stats_turn("invalid")
            return

        # Create variable to track bandwidth.
        source.total_bytes = 0
        source.connected_since = time.time()

        if ticket not in self._ticket_pair:
            self._ticket_pair[ticket] = source
            self._ticket_task[ticket] = asyncio.create_task(self._expire_ticket(ticket))
            await self.database.stats_turn("one-side")
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
        await self.database.stats_turn("two-sided")

        del self._ticket_pair[ticket]

    async def _expire_ticket(self, ticket):
        await asyncio.sleep(TIMEOUT)

        self._ticket_pair[ticket].protocol.transport.close()

        # Expire the ticket after 10 seconds if there was no match.
        del self._ticket_pair[ticket]
        del self._ticket_task[ticket]

    async def receive_raw(self, source, data):
        if not hasattr(source, "peer"):
            return False

        source.total_bytes += len(data)

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
