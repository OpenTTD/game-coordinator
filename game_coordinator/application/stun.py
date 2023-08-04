import asyncio
import ipaddress
import logging

from prometheus_client import Counter

log = logging.getLogger(__name__)


class Application:
    NAME = "stun"

    def __init__(self, database):
        self.database = database

        self.stats_stun_count = Counter("coordinator_stun_tcp", "Amount of stun connections", ["address_type"])

        log.info("Starting STUN server ...")

    async def startup(self):
        asyncio.create_task(self._guard(self._check_database_connection()))

    async def shutdown(self):
        log.info("Shutting down STUN server ...")

    async def _guard(self, coroutine):
        try:
            await coroutine
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("System co-routine failed, killing server ..")

            import sys

            sys.exit(1)

    async def _check_database_connection(self):
        # Check every 10 seconds if the database connection is still there.
        while True:
            await self.database.ping()
            await asyncio.sleep(10)

    async def receive_PACKET_STUN_SERCLI_STUN(self, source, protocol_version, token, interface_number):
        address_type = "ipv6" if isinstance(source.ip, ipaddress.IPv6Address) else "ipv4"
        self.stats_stun_count.labels(address_type=address_type).inc()
        await self.database.stun_result(token, interface_number, source.ip, source.port)
