import asyncio
import ipaddress
import logging
import pproxy

from openttd_protocol.protocol.coordinator import ConnectionType
from openttd_protocol.protocol.game import GameProtocol

log = logging.getLogger(__name__)


class DetectGame:
    def __init__(self, connected):
        self._connected = connected

    def connected(self, source):
        asyncio.create_task(source.protocol.send_PACKET_CLIENT_GAME_INFO())

    async def receive_PACKET_SERVER_GAME_INFO(self, source, **info):
        source.protocol.transport.close()

        # Inform caller that we have successful connected to the valid server.
        self._connected.set()


class TokenVerify:
    def __init__(self, application, source, protocol_version, token, server):
        self.token = token
        self._application = application
        self._source = source
        self._protocol_version = protocol_version
        self._server = server

        self.verify_token = f"V{self.token}"

    async def connect(self):
        self._pending_detection_tasks = []

        if self._protocol_version == 2:
            task = asyncio.create_task(self._start_detection(self._source.ip))
            self._pending_detection_tasks.append(task)
        else:
            await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_REQUEST(
                self._protocol_version, self.verify_token
            )

        asyncio.create_task(self._conclude_detection())

    async def stun_result(self, prefix, interface_number, peer_type, peer_ip, peer_port):
        peer_ip = ipaddress.IPv6Address(peer_ip) if peer_type == "ipv6" else ipaddress.IPv4Address(peer_ip)

        # If we get a STUN result, at the very least the server is STUN capable.
        if self._server.connection_type == ConnectionType.CONNECTION_TYPE_ISOLATED:
            self._server.connection_type = ConnectionType.CONNECTION_TYPE_STUN

        task = asyncio.create_task(self._start_detection(peer_ip))
        self._pending_detection_tasks.append(task)

    async def _start_detection(self, server_ip):
        try:
            await asyncio.wait_for(self._create_connection(server_ip, self._server.server_port), 1)

            # We found a direct-ip to connect to. That is always the better one to use.
            self._server.connection_type = ConnectionType.CONNECTION_TYPE_DIRECT

            # Record the direct-ip in various of places.
            server_ip_str = f"[{server_ip}]" if isinstance(server_ip, ipaddress.IPv6Address) else str(server_ip)
            self._server.direct_ips.append({"ip": server_ip_str, "port": self._server.server_port})
            await self._application.database.direct_ip(self._server.server_id, server_ip, self._server.server_port)
        except (OSError, ConnectionRefusedError, asyncio.TimeoutError):
            # These all indicate a connection could not be created, so the server is not reachable.
            pass
        except Exception:
            log.exception("Internal error: start_detection triggered an exception")

    async def _conclude_detection(self):
        # Give STUN 2 seconds to get back to us; after that, send conclusion.
        await asyncio.sleep(2)

        for task in self._pending_detection_tasks:
            if not task.done():
                task.cancel()
        self._pending_detection_tasks.clear()

        if self._protocol_version == 3:
            # Ensure all STUN connections are closed.
            await self._source.protocol.send_PACKET_COORDINATOR_GC_CONNECT_FAILED(
                self._protocol_version, self.verify_token
            )

        await self._server.send_register_ack(self._protocol_version)

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
