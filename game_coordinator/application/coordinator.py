import asyncio
import hashlib
import ipaddress
import logging

from openttd_protocol.protocol.coordinator import ConnectionType

from .helpers.server import (
    Server,
    ServerExternal,
)

log = logging.getLogger(__name__)


class Application:
    def __init__(self, database):
        self.database = database
        self._servers = {}

        self.database.application = self

    def disconnect(self, source):
        if hasattr(source, "server"):
            self.remove_server(source.server.server_id)

    async def update_external_server(self, server_id, info):
        if server_id not in self._servers:
            self._servers[server_id] = ServerExternal(server_id, info["game_type"])

        if not isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: update_external_server() called on a server managed by us")
            return

        await self._servers[server_id].update(info)

    async def update_external_direct_ip(self, ip_type, server_id, server):
        if server_id not in self._servers:
            return

        if not isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: update_external_direct_ip() called on a server managed by us")
            return

        await self._servers[server_id].update_direct_ip(ip_type, server)

    def remove_server(self, server_id):
        if server_id not in self._servers:
            return

        asyncio.create_task(self._servers[server_id].disconnect())
        del self._servers[server_id]

    async def receive_PACKET_COORDINATOR_CLIENT_REGISTER(self, source, protocol_version, game_type, server_port):
        if isinstance(source.ip, ipaddress.IPv6Address):
            server_id_str = f"[{source.ip}]:{server_port}"
        else:
            server_id_str = f"{source.ip}:{server_port}"

        server_id = hashlib.md5(server_id_str.encode()).digest().hex()

        source.server = Server(self, server_id, game_type, source, server_port)
        self._servers[source.server.server_id] = source.server
        await source.server.detect_connection()

    async def receive_PACKET_COORDINATOR_CLIENT_UPDATE(self, source, protocol_version, **info):
        await source.server.update(info)

    async def receive_PACKET_COORDINATOR_CLIENT_LISTING(
        self, source, protocol_version, game_info_version, openttd_version
    ):
        # Ensure servers matching "openttd_version" are at the top.
        servers_match = []
        servers_other = []
        for server in self._servers.values():
            # Servers that are not reachable shouldn't be listed.
            if server.connection_type == ConnectionType.CONNECTION_TYPE_ISOLATED:
                continue

            if server.info["openttd_version"] == openttd_version:
                servers_match.append(server)
            else:
                servers_other.append(server)

        await source.protocol.send_PACKET_COORDINATOR_SERVER_LISTING(game_info_version, servers_match + servers_other)
