import asyncio
import logging

log = logging.getLogger(__name__)


class Application:
    def __init__(self, database):
        self.database = database

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
        await self.database.stun_result(token, interface_number, source.ip, source.port)
