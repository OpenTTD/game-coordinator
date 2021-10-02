import logging

log = logging.getLogger(__name__)


class Application:
    def __init__(self, database):
        self.database = database

        log.info("Starting STUN server ...")

    async def startup(self):
        pass

    async def shutdown(self):
        log.info("Shutting down STUN server ...")

    async def receive_PACKET_STUN_SERCLI_STUN(self, source, protocol_version, token, interface_number):
        await self.database.stun_result(token, interface_number, source.ip, source.port)
