import asyncio
import click
import logging
import time

from openttd_helpers import click_helper

log = logging.getLogger(__name__)

TTL = 15

_turn_address = None


class Application:
    def __init__(self, database):
        if not _turn_address:
            raise Exception("Please set --turn-address for this application")

        self.database = database
        self.turn_address = _turn_address

        self._ticket_pair = {}
        self._ticket_task = {}

        log.info("Starting TURN server for %s ...", _turn_address)

    def disconnect(self, source):
        if not hasattr(source, "peer"):
            return

        # Make sure we close the other side too.
        source.peer.protocol.transport.close()

        asyncio.create_task(self.database.stats_turn_usage(source.total_bytes, time.time() - source.connected_since))

    async def startup(self):
        asyncio.create_task(self._keep_turn_server_alive())

    async def _keep_turn_server_alive(self):
        # Update the redis key every 10 seconds. The TTL of the key is set at
        # 15 seconds. If for what-ever reason we stop working, we will miss
        # our deadline, redis will expire our key, which informs the cluster
        # we are no longer available for serving any request.
        while True:
            try:
                await self.database.announce_turn_server(self.turn_address)
            except Exception:
                log.exception("System co-routine failed, killing server ..")

                import sys

                sys.exit(1)
            await asyncio.sleep(10)

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

        # Inform both parties we are now relaying.
        await source.protocol.send_PACKET_TURN_TURN_CONNECTED(protocol_version, str(self._ticket_pair[ticket].ip))
        await self._ticket_pair[ticket].protocol.send_PACKET_TURN_TURN_CONNECTED(protocol_version, str(source.ip))
        await self.database.stats_turn("two-sided")

    async def _expire_ticket(self, ticket):
        await asyncio.sleep(10)

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
