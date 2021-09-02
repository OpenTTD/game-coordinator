import asyncio
import click
import logging
import secrets

from openttd_helpers import click_helper
from openttd_protocol.protocol.coordinator import (
    ConnectionType,
    NetworkCoordinatorErrorType,
)

from .helpers.client import Client
from .helpers.invite_code import (
    generate_invite_code,
    generate_invite_code_secret,
    validate_invite_code_secret,
)
from .helpers.server import (
    Server,
    ServerExternal,
)
from .helpers.token_connect import TokenConnect
from .helpers.token_verify import TokenVerify

log = logging.getLogger(__name__)

_socks_proxy = None
_shared_secret = None


class Application:
    def __init__(self, database):
        if not _shared_secret:
            raise Exception("Please set --shared-secret for this application")

        log.info("Starting Game Coordinator ...")

        self._shared_secret = _shared_secret
        self.database = database
        self.socks_proxy = _socks_proxy
        self._servers = {}
        self._tokens = {}
        self._newgrf_lookup_table = {}
        self.turn_servers = []

        self.database.application = self

    async def startup(self):
        await self.database.sync_and_monitor()

    def disconnect(self, source):
        if hasattr(source, "server"):
            asyncio.create_task(self.remove_server(source.server.server_id))

    def delete_token(self, token):
        self._tokens[token].delete_client_token()
        del self._tokens[token]

    def _remove_broken_server(self, server_id, error_no, error_detail):
        broken_server = self._servers[server_id]
        if isinstance(broken_server, ServerExternal):
            return

        asyncio.create_task(broken_server.send_error_and_close(error_no, error_detail))
        del self._servers[server_id]

    async def add_turn_server(self, connection_string):
        if connection_string not in self.turn_servers:
            self.turn_servers.append(connection_string)

    async def remove_turn_server(self, connection_string):
        if connection_string in self.turn_servers:
            self.turn_servers.remove(connection_string)

    async def newgrf_added(self, index, newgrf):
        self._newgrf_lookup_table[index] = newgrf

    async def remove_newgrf_from_table(self, grfid, md5sum):
        for index, newgrf in self._newgrf_lookup_table.items():
            if newgrf["grfid"] == grfid and newgrf["md5sum"] == md5sum:
                del self._newgrf_lookup_table[index]
                return

    async def update_external_server(self, server_id, info):
        if server_id not in self._servers:
            self._servers[server_id] = ServerExternal(self, server_id)

        if not isinstance(self._servers[server_id], ServerExternal):
            # Two servers could announce themselves with the same server-id.
            # Best way to deal with the situation is to assume the new instance
            # is in good contact with the server, and for us to drop our
            # connection with the old.

            self._remove_broken_server(
                server_id, NetworkCoordinatorErrorType.NETWORK_COORDINATOR_ERROR_REUSE_OF_INVITE_CODE, server_id
            )
            self._servers[server_id] = ServerExternal(self, server_id)

        await self._servers[server_id].update(info)

    async def update_newgrf_external_server(self, server_id, newgrfs_indexed):
        if server_id not in self._servers:
            self._servers[server_id] = ServerExternal(self, server_id)

        if not isinstance(self._servers[server_id], ServerExternal):
            # Two servers could announce themselves with the same server-id.
            # Best way to deal with the situation is to assume the new instance
            # is in good contact with the server, and for us to drop our
            # connection with the old.

            self._remove_broken_server(
                server_id, NetworkCoordinatorErrorType.NETWORK_COORDINATOR_ERROR_REUSE_OF_INVITE_CODE, server_id
            )
            self._servers[server_id] = ServerExternal(self, server_id)

        await self._servers[server_id].update_newgrf(newgrfs_indexed)

    async def update_external_direct_ip(self, server_id, type, ip, port):
        if server_id not in self._servers:
            self._servers[server_id] = ServerExternal(self, server_id)

        if not isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: update_external_direct_ip() called on a server managed by us")
            return

        await self._servers[server_id].update_direct_ip(type, ip, port)

    async def send_server_stun_request(self, server_id, protocol_version, token):
        if server_id not in self._servers:
            return

        if isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: server_stun_request() called on a server NOT managed by us")
            return

        await self._servers[server_id].send_stun_request(protocol_version, token)

    async def send_server_stun_connect(
        self, server_id, protocol_version, token, tracking_number, interface_number, peer_ip, peer_port
    ):
        if server_id not in self._servers:
            return

        if isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: server_stun_connect() called on a server NOT managed by us")
            return

        await self._servers[server_id].send_stun_connect(
            protocol_version, token, tracking_number, interface_number, peer_ip, peer_port
        )

    async def send_server_turn_connect(
        self, server_id, protocol_version, token, tracking_number, ticket, connection_string
    ):
        if server_id not in self._servers:
            return

        if isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: server_turn_connect() called on a server NOT managed by us")
            return

        await self._servers[server_id].send_turn_connect(
            protocol_version, token, tracking_number, ticket, connection_string
        )

    async def send_server_connect_failed(self, server_id, protocol_version, token):
        if server_id not in self._servers:
            return

        if isinstance(self._servers[server_id], ServerExternal):
            log.error("Internal error: server_connect_failed() called on a server NOT managed by us")
            return

        await self._servers[server_id].send_connect_failed(protocol_version, token)

    async def stun_result(self, token, interface_number, peer_type, peer_ip, peer_port):
        prefix = token[0]
        token = self._tokens.get(token[1:])
        if not token:
            return

        await token.stun_result(prefix, interface_number, peer_type, peer_ip, peer_port)

    async def remove_server(self, server_id):
        if server_id not in self._servers:
            return

        # Check if there are any pending connections to this server.
        abort_tokens = []
        for token in self._tokens.values():
            if token._server.server_id == server_id:
                abort_tokens.append(token)

        # Abort all of those pending connections.
        for token in abort_tokens:
            await token.abort_attempt("server")

        asyncio.create_task(self._servers[server_id].disconnect())
        del self._servers[server_id]

    async def gc_connect_failed(self, token, tracking_number):
        token = self._tokens.get(token)
        if token is None:
            # Assume that another instance is handling this token.
            return

        await token.connect_failed(tracking_number)

    async def gc_stun_result(self, prefix, token, interface_number, result):
        token = self._tokens.get(token)
        if token is None:
            # Assume that another instance is handling this token.
            return

        await token.stun_result_concluded(prefix, interface_number, result)

    async def receive_PACKET_COORDINATOR_SERVER_REGISTER(
        self, source, protocol_version, game_type, server_port, invite_code, invite_code_secret
    ):
        if (
            invite_code
            and invite_code_secret
            and invite_code[0] == "+"
            and validate_invite_code_secret(self._shared_secret, invite_code, invite_code_secret)
        ):
            # Invite code given is valid, so re-use it.
            server_id = invite_code
        else:
            while True:
                server_id = generate_invite_code(self.database.get_server_id())
                if server_id not in self._servers:
                    break

            invite_code_secret = generate_invite_code_secret(self._shared_secret, server_id)

        old_server_id = source.server.server_id if hasattr(source, "server") else None

        source.server = Server(self, server_id, game_type, source, protocol_version, server_port, invite_code_secret)

        if source.server.server_id == old_server_id:
            # If the old server-id is the same, it means this server was
            # already registered via this connection, and the user is most
            # likely changing something like "game-type".
            pass
        elif source.server.server_id in self._servers:
            # We replace a server already known; possibly two servers are using
            # the same invite-code. There is not much we can do about this,
            # other than disconnect the old, and hope the server-owner notices
            # that they are constantly battling for the same invite-code.
            self._remove_broken_server(
                source.server.server_id,
                NetworkCoordinatorErrorType.NETWORK_COORDINATOR_ERROR_REUSE_OF_INVITE_CODE,
                source.server.server_id,
            )

        self._servers[source.server.server_id] = source.server

        # Find an unused token.
        while True:
            token = secrets.token_hex(16)
            if token not in self._tokens:
                break

        # Create a token to connect server and client.
        token = TokenVerify(self, source, protocol_version, token, source.server)
        self._tokens[token.token] = token

        await token.connect()

    async def receive_PACKET_COORDINATOR_SERVER_UPDATE(
        self, source, protocol_version, newgrf_serialization_type, newgrfs, **info
    ):
        await source.server.update_newgrf(newgrf_serialization_type, newgrfs)
        await source.server.update(info)

    async def receive_PACKET_COORDINATOR_CLIENT_LISTING(
        self, source, protocol_version, game_info_version, openttd_version, newgrf_lookup_table_cursor
    ):
        if protocol_version >= 4 and self._newgrf_lookup_table:
            await source.protocol.send_PACKET_COORDINATOR_GC_NEWGRF_LOOKUP(
                protocol_version, newgrf_lookup_table_cursor, self._newgrf_lookup_table
            )

        # Ensure servers matching "openttd_version" are at the top.
        servers_match = []
        servers_other = []
        for server in self._servers.values():
            # Servers that are not reachable shouldn't be listed.
            if server.connection_type == ConnectionType.CONNECTION_TYPE_ISOLATED:
                continue
            # Server is announced but hasn't finished registration.
            if not server.info:
                continue

            if server.info["openttd_version"] == openttd_version:
                servers_match.append(server)
            else:
                servers_other.append(server)

        await source.protocol.send_PACKET_COORDINATOR_GC_LISTING(
            protocol_version, game_info_version, servers_match + servers_other, self._newgrf_lookup_table
        )
        await self.database.stats_listing(game_info_version, openttd_version)

    async def receive_PACKET_COORDINATOR_CLIENT_CONNECT(self, source, protocol_version, invite_code):
        if not invite_code or invite_code[0] != "+" or invite_code not in self._servers:
            await source.protocol.send_PACKET_COORDINATOR_GC_ERROR(
                protocol_version, NetworkCoordinatorErrorType.NETWORK_COORDINATOR_ERROR_INVALID_INVITE_CODE, invite_code
            )
            # Don't close connection, as the server might just be restarting.
            return

        if not hasattr(source, "client"):
            source.client = Client()

        # Find an unused token.
        while True:
            token = secrets.token_hex(16)
            if token not in self._tokens:
                break

        # A client is always connected to a single GC instance. So on that
        # instance we track if the client tries to connect to the same server
        # twice. If so, we abort the previous connection and create a new one.
        if invite_code in source.client.connections:
            await source.client.connections[invite_code].abort_attempt("client")

        # Create a token to connect server and client.
        token = TokenConnect(self, source, protocol_version, token, self._servers[invite_code])
        source.client.connections[invite_code] = token
        self._tokens[token.token] = token

        # Inform client of token value, and start the connection attempt(s).
        await source.protocol.send_PACKET_COORDINATOR_GC_CONNECTING(protocol_version, token.client_token, invite_code)
        await token.connect()

    async def receive_PACKET_COORDINATOR_SERCLI_CONNECT_FAILED(self, source, protocol_version, token, tracking_number):
        if token[1:] not in self._tokens:
            if token[0] == "S":
                # The server tells us information about a token we do not track.
                # So broadcast it to the other instances, as they might want to
                # know.
                await self.database.gc_connect_failed(token[1:], tracking_number)
            return

        # Client or server noticed the connection attempt failed.
        await self.gc_connect_failed(token[1:], tracking_number)

    async def receive_PACKET_COORDINATOR_CLIENT_CONNECTED(self, source, protocol_version, token):
        token = self._tokens.get(token[1:])
        if token is None:
            source.protocol.transport.close()
            return

        # Client and server are connected; clean the token.
        await token.connected()
        self.delete_token(token.token)

    async def receive_PACKET_COORDINATOR_SERCLI_STUN_RESULT(
        self, source, protocol_version, token, interface_number, result
    ):
        prefix = token[0]
        if token[1:] not in self._tokens:
            if token[0] == "S":
                # The server tells us information about a token we do not track.
                # So broadcast it to the other instances, as they might want to
                # know.
                await self.database.gc_stun_result(prefix, token[1:], interface_number, result)
            return

        await self.gc_stun_result(prefix, token[1:], interface_number, result)


@click_helper.extend
@click.option("--shared-secret", help="Shared secret to validate invite-code-secrets with")
@click.option(
    "--socks-proxy",
    help="Use a SOCKS proxy to query game servers.",
)
def click_application_coordinator(socks_proxy, shared_secret):
    global _socks_proxy, _shared_secret

    _socks_proxy = socks_proxy
    _shared_secret = shared_secret
