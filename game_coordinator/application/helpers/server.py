import asyncio
import logging

from openttd_protocol.wire.exceptions import SocketClosed
from openttd_protocol.protocol.coordinator import ConnectionType

log = logging.getLogger(__name__)


class ConnectAndCloseProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        transport.close()


class ServerExternal:
    def __init__(self, connection_string, info):
        self.info = info
        self.game_type = info["game_type"]
        self.connection_string = connection_string

    async def disconnect(self):
        pass

    async def update(self, info):
        self.info = info


class Server:
    def __init__(self, application, source, game_type, server_port):
        self._application = application
        self._source = source
        self._server_port = server_port
        self._task = None

        self.info = {}
        self.game_type = game_type

        self.connection_string = f"{str(self._source.ip)}:{self._server_port}"

    async def disconnect(self):
        await self._application.database.server_offline(self.connection_string)

        if self._task:
            self._task.cancel()

    async def update(self, info):
        self.info = info
        self.info["game_type"] = self.game_type

        await self._application.database.update_info(self.connection_string, self.info)

    async def detect_connection(self):
        self._task = asyncio.create_task(self._start_detection())

    async def _start_detection(self):
        try:
            await self._real_start_detection()
        except SocketClosed:
            raise
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Internal error: start_detection triggered an exception")

    async def _real_start_detection(self):
        try:
            await asyncio.wait_for(
                asyncio.get_event_loop().create_connection(
                    lambda: ConnectAndCloseProtocol(), host=str(self._source.ip), port=self._server_port
                ),
                1,
            )
            connection_type = ConnectionType.CONNECTION_TYPE_DIRECT
        except (OSError, ConnectionRefusedError, asyncio.TimeoutError):
            connection_type = ConnectionType.CONNECTION_TYPE_ISOLATED

        await self._source.protocol.send_PACKET_COORDINATOR_SERVER_REGISTER_ACK(connection_type=connection_type)
        self._task = None
