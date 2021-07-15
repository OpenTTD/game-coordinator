import asyncio
import logging

from openttd_protocol.wire.exceptions import SocketClosed

log = logging.getLogger(__name__)


class TokenConnect:
    def __init__(self, application, source, protocol_version, token, server):
        self.token = token
        self._application = application
        self._source = source
        self._protocol_version = protocol_version
        self._server = server

        self._tracking_number = 0
        self.client_token = f"C{self.token}"
        self.server_token = f"S{self.token}"

        self._stun_result = {
            "C": {},
            "S": {},
        }

    async def connect(self):
        self.tracking_number = 0

        # Create a queue with methods we have to connect the client and the
        # server. As some methods (like STUN) are dynamic, a task will be
        # reading this queue for a few seconds, awaiting other methods if
        # needed.
        self._connect_methods = asyncio.Queue()
        for direct_ip in self._server.direct_ips:
            self._connect_methods.put_nowait(lambda: self._connect_direct_connect(direct_ip))
        self._connect_methods.put_nowait(lambda: self._connect_stun_request())

        # The peer-type of STUN requests we allow to match.
        self._stun_ip_type = [
            "ipv4",
            "ipv6",
        ]

        self._connect_task = asyncio.create_task(self._connect())
        self._timeout_task = asyncio.create_task(self._timeout())

    async def connected(self):
        self._connect_task.cancel()
        self._timeout_task.cancel()
        self._connect_task = None
        self._timeout_task = None

    async def stun_result(self, prefix, interface_number, peer_type, peer_ip, peer_port):
        self._stun_result[prefix][peer_type] = (interface_number, peer_ip, peer_port)

        for ip_type in self._stun_ip_type:
            if ip_type in self._stun_result["C"] and ip_type in self._stun_result["S"]:
                self._stun_ip_type.remove(ip_type)

                client_peer = self._stun_result["C"][ip_type]
                server_peer = self._stun_result["S"][ip_type]

                self._connect_methods.put_nowait(lambda: self._connect_stun_connect(client_peer, server_peer))

    async def _timeout(self):
        try:
            await asyncio.sleep(10)

            # If we reach here, we haven't managed to get a connection within 10 seconds. Time to call it a day.
            await self._connect_failed()
        except Exception:
            log.exception("Exception during _timeout()")

    async def _connect(self):
        self._connect_next_event = asyncio.Event()

        while True:
            try:
                await asyncio.wait_for(self._connect_next_wait(), 2)
            except asyncio.TimeoutError:
                # It took more than 2 seconds to get a new method for
                # connecting. At this point it is safe to assume there will
                # not be any other methods presenting itself, so call it a
                # day.
                await self._connect_failed()
            except Exception:
                log.exception("Exception during _connect_next_wait()")

            await self._connect_next_event.wait()

    async def _connect_next_wait(self):
        proc = await self._connect_methods.get()
        await proc()

    async def connect_failed(self, tracking_number):
        # Check if this is our current attempt. Server and client send
        # failures. This way we act on which ever reports the failure first,
        # while safely ignoring the other.
        if tracking_number != self._tracking_number:
            return

        # Try the next attempt now.
        self._tracking_number += 1
        self._connect_next_event.set()

    async def _connect_direct_connect(self, server):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_DIRECT_CONNECT(
            self._protocol_version, self.client_token, self._tracking_number, server["ip"], server["port"]
        )

    async def _connect_stun_request(self):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_REQUEST(self._protocol_version, self.client_token)
        await self._server.send_stun_request(self._protocol_version, self.server_token)

        # This is not really an attempt, but the STUN result will queue a new
        # method to try when-ever it is ready. So already continue to the next
        # iteration.
        self._connect_next_event.set()

    async def _connect_stun_connect(self, client_peer, server_peer):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_CONNECT(
            self._protocol_version,
            self.client_token,
            self._tracking_number,
            client_peer[0],
            server_peer[1],
            server_peer[2],
        )
        await self._server.send_stun_connect(
            self._protocol_version,
            self.server_token,
            self._tracking_number,
            server_peer[0],
            client_peer[1],
            client_peer[2],
        )

    async def _connect_failed(self):
        if self._connect_task:
            self._connect_task.cancel()
            self._connect_task = None
        if self._timeout_task:
            self._timeout_task.cancel()
            self._timeout_task = None

        try:
            await self._source.protocol.send_PACKET_COORDINATOR_GC_CONNECT_FAILED(
                self._protocol_version, self.client_token
            )
        except SocketClosed:
            # If the client already left, that is fine.
            pass
        await self._server.send_connect_failed(self._protocol_version, self.server_token)

        self._application.delete_token(self.token)