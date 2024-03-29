import asyncio
import logging
import random

from openttd_protocol.wire.exceptions import SocketClosed

log = logging.getLogger(__name__)

TIMEOUT = 20  # After how many seconds we give up on connecting client and server.


class TokenConnect:
    def __init__(self, application, source, protocol_version, token, server):
        self.token = token
        self._application = application
        self._source = source
        self._protocol_version = protocol_version
        self._server = server

        self._tracking_number = 1
        self.client_token = f"C{self.token}"
        self.server_token = f"S{self.token}"

        self._connect_task = None
        self._timeout_task = None
        self._given_up = False

    def delete_client_token(self):
        if self._source.client.connections[self._server.server_id] == self:
            del self._source.client.connections[self._server.server_id]

    async def connect(self):
        self._tracking_number = 1
        self._connect_result_event = asyncio.Event()

        # The peer-type of STUN requests we allow to match.
        self._stun_ip_type = [
            "ipv4",
            "ipv6",
        ]
        self._stun_result = {
            "C": {},
            "S": {},
        }
        self._stun_result_seen = {
            "C": set(),
            "S": set(),
        }
        self._stun_pairs = asyncio.Queue()

        self._connect_task = asyncio.create_task(self._connect_guard())
        self._timeout_task = asyncio.create_task(self._timeout())

    async def connected(self):
        self._given_up = True
        self._connect_task.cancel()
        self._timeout_task.cancel()
        self._connect_task = None
        self._timeout_task = None

        return self._connect_method

    async def abort_attempt(self, reason):
        asyncio.create_task(self._connect_give_up(f"abort-{reason}"))

    async def connect_failed(self, tracking_number):
        if tracking_number == 0:
            # Client requested we stop with this connection attempt. So clean it up!
            asyncio.create_task(self._connect_give_up("stop"))
            # Make sure any late-arrivers for the current attempt are not counted.
            self._tracking_number = -1
            return

        # Check if this is our current attempt. Server and client send
        # failures. This way we act on which ever reports the failure first,
        # while safely ignoring the other.
        if tracking_number != self._tracking_number:
            return

        # Try the next attempt now.
        self._tracking_number += 1
        self._connect_result_event.set()

    async def stun_result(self, prefix, interface_number, peer_type, peer_ip, peer_port):
        if peer_type == "ipv6":
            peer_ip = f"[{peer_ip}]"

        self._stun_result[prefix][peer_type] = (interface_number, peer_ip, peer_port)

        for ip_type in self._stun_ip_type:
            if ip_type in self._stun_result["C"] and ip_type in self._stun_result["S"]:
                self._stun_ip_type.remove(ip_type)
                self._stun_pairs.put_nowait(ip_type)

        self._stun_result_seen[prefix].add(interface_number)
        if len(self._stun_result_seen["C"]) == 2 and len(self._stun_result_seen["S"]) == 2:
            # Both sides reported all their STUN results. Inform _connect().
            self._stun_pairs.put_nowait(None)

    async def stun_result_concluded(self, prefix, interface_number, result):
        if result:
            # Successful STUN results will call stun_result() eventually too.
            return

        self._stun_result_seen[prefix].add(interface_number)
        if len(self._stun_result_seen["C"]) == 2 and len(self._stun_result_seen["S"]) == 2:
            # Both sides reported all their STUN results. Inform _connect().
            self._stun_pairs.put_nowait(None)

    async def _timeout(self):
        try:
            await asyncio.sleep(TIMEOUT)

            # If we reach here, we haven't managed to get a connection within TIMEOUT seconds. Time to call it a day.
            self._timeout_task = None
            asyncio.create_task(self._connect_give_up("timeout"))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Exception during _timeout()")

    async def _connect_guard(self):
        try:
            await self._connect()
        except asyncio.CancelledError:
            raise
        except SocketClosed:
            # Either of the two sides closed the Game Coordinator
            # connection. So cancel the connection attempt.
            asyncio.create_task(self._connect_give_up("closed"))
        except Exception:
            log.exception("Exception during _connect()")
            asyncio.create_task(self._connect_give_up("exception"))

        self._connect_task = None

    async def _connect(self):
        # Try connecting via direct-IPs first.
        for direct_ip in self._server.direct_ips:
            server_ip, _, server_port = direct_ip.rpartition(":")

            await self._connect_direct_connect(server_ip, int(server_port))
            await self._connect_result_event.wait()

        # Send out STUN requests.
        await self._connect_stun_request()

        # Wait for STUN pairs to arrive.
        while True:
            ip_type = await self._stun_pairs.get()
            if ip_type is None:
                # We are being told no further STUN pairs will arrive.
                break

            await self._connect_stun_connect(ip_type)
            await self._connect_result_event.wait()

        # If all of the above fails, try TURN.
        await self._connect_turn_connect()
        await self._connect_result_event.wait()

        # There are no more methods.
        asyncio.create_task(self._connect_give_up("out-of-methods"))

    async def _connect_direct_connect(self, server_ip, server_port):
        ip_type = "ipv6" if server_ip.startswith("[") else "ipv4"
        self._connect_method = f"direct-{ip_type}"
        self._connect_result_event.clear()

        # Client requested an abort, but due to async behaviour we can still be executed.
        if self._tracking_number == -1:
            return

        await self._source.protocol.send_PACKET_COORDINATOR_GC_DIRECT_CONNECT(
            self._protocol_version, self.client_token, self._tracking_number, server_ip, server_port
        )

    async def _connect_stun_request(self):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_STUN_REQUEST(self._protocol_version, self.client_token)
        await self._server.send_stun_request(self._protocol_version, self.server_token)

    async def _connect_stun_connect(self, ip_type):
        self._connect_method = f"stun-{ip_type}"
        self._connect_result_event.clear()

        # Client requested an abort, but due to async behaviour we can still be executed.
        if self._tracking_number == -1:
            return

        client_peer = self._stun_result["C"][ip_type]
        server_peer = self._stun_result["S"][ip_type]

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

    async def _connect_turn_connect(self):
        self._connect_method = "turn"
        self._connect_result_event.clear()

        # Client requested an abort, but due to async behaviour we can still be executed.
        if self._tracking_number == -1:
            return

        if self._protocol_version < 5 or not self._application.turn_servers:
            await self.connect_failed(self._tracking_number)
            return

        connection_string = random.choice(self._application.turn_servers)

        turn_ticket = await self._application.database.create_turn_ticket()

        await self._source.protocol.send_PACKET_COORDINATOR_GC_TURN_CONNECT(
            self._protocol_version,
            self.client_token,
            self._tracking_number,
            turn_ticket,
            connection_string,
        )
        await self._server.send_turn_connect(
            self._protocol_version,
            self.server_token,
            self._tracking_number,
            turn_ticket,
            connection_string,
        )

    async def _connect_give_up(self, failure_reason):
        # Because we are async, it can happen more than one way suggests we
        # should give up. For example, a user sends a "stop", but directly
        # after that reconnects. This triggers an "abort". Both tasks are
        # still pending, so ignore "abort" after processing "stop".
        if self._given_up:
            return
        self._given_up = True

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

        try:
            await self._server.send_connect_failed(self._protocol_version, self.server_token)
        except SocketClosed:
            # If the server already left, that is fine.
            pass

        self._application.stats_coordinator_tcp_connect_result.labels(result=failure_reason).inc()

        self._application.delete_token(self.token)
