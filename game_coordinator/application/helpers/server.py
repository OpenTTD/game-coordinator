import asyncio
import ipaddress
import logging
import pproxy

from openttd_protocol.wire.exceptions import SocketClosed
from openttd_protocol.protocol.coordinator import ConnectionType
from openttd_protocol.protocol.game import GameProtocol

log = logging.getLogger(__name__)


class ConnectAndCloseProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        transport.close()


class ServerExternal:
    def __init__(self, server_id):
        self.info = {}
        self.game_type = None
        self.server_id = server_id
        self.direct_ips = []

        if self.server_id[0] == "+":
            self.connection_string = self.server_id
        else:
            self.connection_string = None
        self.connection_type = ConnectionType.CONNECTION_TYPE_ISOLATED

    async def disconnect(self):
        pass

    async def update(self, info):
        self.game_type = info["game_type"]
        self.info = info

    async def update_direct_ip(self, ip_type, server):
        # Do not overwrite the connection_string if we are named by invite-code.
        if self.server_id[0] != "+":
            # Always use the IPv4 if possible, and only IPv6 if there is no IPv4.
            if ip_type == "ipv6":
                if self.connection_string is None:
                    self.connection_string = f"[{server['ip']}]:{server['port']}"
            else:
                self.connection_string = f"{server['ip']}:{server['port']}"

        if ip_type == "ipv6":
            server["ip"] = f"[{server['ip']}]"

        self.direct_ips.append(server)
        self.connection_type = ConnectionType.CONNECTION_TYPE_DIRECT


class Server:
    def __init__(self, application, server_id, game_type, source, server_port, invite_code_secret):
        self._application = application
        self._source = source
        self._server_port = server_port
        self._invite_code_secret = invite_code_secret
        self._task = None

        self.info = {}
        self.game_type = game_type
        self.server_id = server_id
        self.direct_ips = []

        if invite_code_secret:
            self.connection_string = server_id
        else:
            self.connection_string = f"{str(self._source.ip)}:{self._server_port}"
        self.connection_type = ConnectionType.CONNECTION_TYPE_ISOLATED

    async def disconnect(self):
        await self._application.database.server_offline(self.server_id)

        if self._task:
            self._task.cancel()

    async def update(self, info):
        self.info = info
        self.info["game_type"] = self.game_type.value
        self.info["connection_type"] = self.connection_type.value

        await self._application.database.update_info(self.server_id, self.info)

    async def detect_connection(self, protocol_version):
        self._task = asyncio.create_task(self._start_detection(protocol_version))

    async def _start_detection(self, protocol_version):
        try:
            await self._real_start_detection(protocol_version)
        except SocketClosed:
            raise
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Internal error: start_detection triggered an exception")

    async def _real_start_detection(self, protocol_version):
        try:
            await asyncio.wait_for(self._create_connection(self._source.ip, self._server_port), 1)
            await self._application.database.direct_ip(self.server_id, self._source.ip, self._server_port)
            if isinstance(self._source.ip, ipaddress.IPv6Address):
                self.direct_ips.append({"ip": f"[{self._source.ip}]", "port": self._server_port})
            else:
                self.direct_ips.append({"ip": str(self._source.ip), "port": self._server_port})
            self.connection_type = ConnectionType.CONNECTION_TYPE_DIRECT
        except (OSError, ConnectionRefusedError, asyncio.TimeoutError):
            # These all indicate a connection could not be created, so the server is not reachable.
            pass

        await self._source.protocol.send_PACKET_COORDINATOR_GC_REGISTER_ACK(
            protocol_version, self.connection_type, self.server_id, self._invite_code_secret
        )
        self._task = None

    async def _create_connection(self, server_ip, server_port):
        connected = asyncio.Event()

        if self._application.socks_proxy:
            socks_conn = pproxy.Connection(self._application.socks_proxy)
            _, writer = await socks_conn.tcp_connect(str(server_ip), server_port)

            # Hand over the socket to our own Protocol.
            sock = writer.transport.get_extra_info("socket")
            server = await asyncio.get_event_loop().create_connection(
                lambda: GameProtocol(DetectGame(connected)),
                sock=sock,
            )
        else:
            server = await asyncio.get_event_loop().create_connection(
                lambda: GameProtocol(DetectGame(connected)),
                host=str(server_ip),
                port=server_port,
            )

        try:
            # Wait for a signal we exchanged GAME_INFO packets.
            await connected.wait()
        finally:
            # Make sure to never leave with the connection open.
            server[0].close()


class DetectGame:
    def __init__(self, connected):
        self._connected = connected

    def connected(self, source):
        asyncio.create_task(source.protocol.send_PACKET_CLIENT_GAME_INFO())

    async def receive_PACKET_SERVER_GAME_INFO(self, source, **info):
        source.protocol.transport.close()

        # Inform caller that we have successful connected to the valid server.
        self._connected.set()
