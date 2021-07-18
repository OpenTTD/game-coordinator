import asyncio
import logging

from openttd_protocol.protocol.coordinator import (
    ConnectionType,
    NewGRFSerializationType,
    ServerGameType,
)

log = logging.getLogger(__name__)


class ConnectAndCloseProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        transport.close()


class ServerExternal:
    def __init__(self, application, server_id):
        self._application = application
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
        self.game_type = ServerGameType(info["game_type"])
        self.connection_type = ConnectionType(info["connection_type"])
        self.info = info

    async def update_newgrf(self, newgrfs_indexed):
        self.newgrfs_indexed = newgrfs_indexed

    async def update_direct_ip(self, ip_type, ip, port):
        # Do not overwrite the connection_string if we are named by invite-code.
        if self.server_id[0] != "+":
            # Always use the IPv4 if possible, and only IPv6 if there is no IPv4.
            if ip_type == "ipv6":
                if self.connection_string is None:
                    self.connection_string = f"[{ip}]:{port}"
            else:
                self.connection_string = f"{ip}:{port}"

        if ip_type == "ipv6":
            ip = f"[{ip}]"

        self.direct_ips.append({"ip": ip, "port": port})
        self.connection_type = ConnectionType.CONNECTION_TYPE_DIRECT

    async def send_stun_request(self, protocol_version, token):
        await self._application.database.send_server_stun_request(self.server_id, protocol_version, token)

    async def send_stun_connect(self, protocol_version, token, tracking_number, interface_number, peer_ip, peer_port):
        await self._application.database.send_server_stun_connect(
            self.server_id, protocol_version, token, tracking_number, interface_number, peer_ip, peer_port
        )

    async def send_connect_failed(self, protocol_version, token):
        await self._application.database.send_server_connect_failed(self.server_id, protocol_version, token)


class Server:
    def __init__(self, application, server_id, game_type, source, server_port, invite_code_secret):
        self._application = application
        self._source = source
        self._invite_code_secret = invite_code_secret

        self.info = {}
        self.game_type = game_type
        self.server_id = server_id
        self.server_port = server_port
        self.direct_ips = []

        if invite_code_secret:
            self.connection_string = server_id
        else:
            self.connection_string = f"{str(self._source.ip)}:{self.server_port}"
        self.connection_type = ConnectionType.CONNECTION_TYPE_ISOLATED

    async def disconnect(self):
        await self._application.database.server_offline(self.server_id)

    async def update_newgrf(self, newgrf_serialization_type, newgrfs):
        if newgrfs is None:
            return

        # This update is sent after the first, and is meant just as
        # notification the NewGRFs are still in use. A server cannot change
        # NewGRFs in a running game, so as long as this packet is sent, the
        # server is not actually changing NewGRFs.
        if newgrf_serialization_type == NewGRFSerializationType.NST_GRFID_MD5:
            for newgrf in newgrfs:
                await self._application.database.newgrf_in_use(newgrf)
            return

        if newgrf_serialization_type not in (
            NewGRFSerializationType.NST_GRFID_MD5_NAME,
            NewGRFSerializationType.NST_CONVERSION_GRFID_MD5,
        ):
            log.error("Unexpected NewGRF serialization type %s", newgrf_serialization_type.name)
            return

        # This is the first update; convert the NewGRFs into an index-based
        # variant.
        newgrfs_indexed = []
        for newgrf in newgrfs:
            index = await self._application.database.newgrf_assign_index(newgrf)
            newgrfs_indexed.append(index)

        self.newgrfs_indexed = newgrfs_indexed
        await self._application.database.update_newgrf(self.server_id, newgrfs_indexed)

    async def update(self, info):
        self.info = info
        self.info["game_type"] = self.game_type.value
        self.info["connection_type"] = self.connection_type.value

        await self._application.database.update_info(self.server_id, self.info)

    async def send_register_ack(self, protocol_version):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_REGISTER_ACK(
            protocol_version, self.connection_type, self.server_id, self._invite_code_secret
        )

    async def send_stun_request(self, protocol_version, token):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_REQUEST(protocol_version, token)

    async def send_stun_connect(self, protocol_version, token, tracking_number, interface_number, peer_ip, peer_port):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_CONNECT(
            protocol_version, token, tracking_number, interface_number, peer_ip, peer_port
        )

    async def send_connect_failed(self, protocol_version, token):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_CONNECT_FAILED(protocol_version, token)
