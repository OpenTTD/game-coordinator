import asyncio
import logging

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
            self.remove_server(source.server.connection_string)

    async def update_external_server(self, connection_string, info):
        if connection_string not in self._servers:
            self._servers[connection_string] = ServerExternal(connection_string, info)
            return

        await self._servers[connection_string].update(info)

    def remove_server(self, connection_string):
        if connection_string not in self._servers:
            return

        asyncio.create_task(self._servers[connection_string].disconnect())
        del self._servers[connection_string]

    async def receive_PACKET_COORDINATOR_CLIENT_REGISTER(self, source, protocol_version, game_type, server_port):
        source.server = Server(self, source, game_type, server_port)
        self._servers[source.server.connection_string] = source.server
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
            if server.info["openttd_version"] == openttd_version:
                servers_match.append(server)
            else:
                servers_other.append(server)

        await source.protocol.send_PACKET_COORDINATOR_SERVER_LISTING(game_info_version, servers_match + servers_other)
