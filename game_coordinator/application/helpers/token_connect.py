import asyncio


class TokenConnect:
    def __init__(self, application, source, protocol_version, token, server):
        self.token = token
        self._application = application
        self._source = source
        self._protocol_version = protocol_version
        self._server = server

        self._tracking_number = 0
        self.client_token = f"C{self.token}"

        self._timeout_task = asyncio.create_task(self._timeout())

    async def connect(self):
        if self._server.direct_ips:
            self.tracking_number = 0
            await self.next_direct_connect()
            return

        # No public IPs available for this server, so no way to connect.
        await self._connect_failed()

    async def connected(self):
        self._timeout_task.cancel()
        self._timeout_task = None

    async def connect_failed(self, tracking_number):
        # Check if this is our current attempt.
        if tracking_number != self._tracking_number:
            return

        # Check if there is another public IP listed for this server.
        if len(self._server.direct_ips) > self._tracking_number + 1:
            self._tracking_number += 1
            await self.next_direct_connect()
            return

        # We exhausted all possible ways to connect client to server.
        await self._connect_failed()

    async def _timeout(self):
        await asyncio.sleep(10)

        # If we reach here, we haven't managed to get a connection within 10 seconds. Time to call it a day.
        self._timeout_task = None
        await self._connect_failed()

    async def _connect_failed(self):
        await self._source.protocol.send_PACKET_COORDINATOR_GC_CONNECT_FAILED(self._protocol_version, self.client_token)
        self._application.delete_token(self.token)

        if self._timeout_task:
            self._timeout_task.cancel()
            self._timeout_task = None

    async def next_direct_connect(self):
        server = self._server.direct_ips[self._tracking_number]
        await self._source.protocol.send_PACKET_COORDINATOR_GC_DIRECT_CONNECT(
            self._protocol_version, self.client_token, self._tracking_number, server["ip"], server["port"]
        )
