import asyncio
import aioredis
import click
import ipaddress
import json
import logging
import time

from aioredis import ResponseError

from openttd_helpers import click_helper

log = logging.getLogger(__name__)

_redis_url = None


class Database:
    def __init__(self):
        # The application claiming this Database instance should set this.
        self.application = None
        # Set by startup() on start-up.
        self.gc_id = None

        self._redis = aioredis.from_url(_redis_url, decode_responses=True)

    async def startup(self):
        # Check with redis if any of the keys are available.
        while True:
            for i in range(16):
                res = await self._redis.set(f"gc-id:{i}", 1, ex=60, nx=True)
                if res is not None:
                    self._gc_id = str(i)
                    break
            else:
                # We couldn't find a free slot. Possibly we are just crashing
                # a lot, so give it some time and try again.
                asyncio.sleep(30)
                continue
            break

        log.info("Game Coordinator ID: %s", self._gc_id)

        # We could start, so start populating the server-list and follow the change-stream.
        asyncio.ensure_future(self._guard(self._keep_gc_id_alive()))
        asyncio.ensure_future(self._guard(self._monitor_expire()))
        asyncio.ensure_future(self._guard(self._scan_existing_servers()))
        asyncio.ensure_future(self._guard(self._follow_stream()))

    async def _guard(self, coroutine):
        try:
            await coroutine
        except Exception:
            log.exception("System co-routine failed, killing server ..")

            import sys

            sys.exit(1)

    async def _keep_gc_id_alive(self):
        # Update the fact that we use this server-id every 30 seconds. This
        # means that only if we are so busy we cannot do this for 30 seconds,
        # we release our id. So track how long we take and crash if we are
        # getting close to that value.
        while True:
            last_time = time.time()

            await asyncio.sleep(30)
            await self._redis.set(f"gc-id:{self._gc_id}", 1, ex=60)

            if time.time() - last_time > 50:
                raise Exception("We were about to lose our GC-id, so we crash instead.")

    async def _monitor_expire(self):
        try:
            await self._redis.config_set("notify-keyspace-events", "Ex")
        except ResponseError:
            log.warning("Couldn't set configuration setting 'notify-keyspace-events' to 'Ex'. Please do this manually.")

        channel = self._redis.pubsub()
        await channel.subscribe("__keyevent@0__:expired")

        while True:
            async for message in channel.listen():
                if message["type"] != "message":
                    continue

                if message["data"].startswith("gc-server:"):
                    _, _, server_id = message["data"].partition(":")

                    await self._redis.delete(f"gc-direct-ipv4:{server_id}")
                    await self._redis.delete(f"gc-direct-ipv6:{server_id}")

                    await self.application.remove_server(server_id)

    async def _scan_existing_servers(self):
        servers = await self._redis.keys("gc-server:*")
        for server in servers:
            _, _, server_id = server.partition(":")

            info_str = await self._redis.get(server)
            info = json.loads(info_str)
            await self.application.update_external_server(server_id, info)

        direct_ipv4s = await self._redis.keys("gc-direct-ipv4:*")
        for direct_ipv4 in direct_ipv4s:
            _, _, server_id = direct_ipv4.partition(":")

            server_str = await self._redis.get(direct_ipv4)
            server = json.loads(server_str)
            await self.application.update_external_direct_ip("ipv4", server_id, server)

        direct_ipv6s = await self._redis.keys("gc-direct-ipv6:*")
        for direct_ipv6 in direct_ipv6s:
            _, _, server_id = direct_ipv6.partition(":")

            server_str = await self._redis.get(direct_ipv6)
            server = json.loads(server_str)
            await self.application.update_external_direct_ip("ipv6", server_id, server)

        # We wait for 70 seconds, well past the TTL of servers, and query all
        # keys. This forces redis under all condition to expire servers that
        # are past the TTL. This is picked up by _monitor_expire() and the
        # server is removed. This is needed, as redis otherwise gives far less
        # guarantees servers are expired after their TTL, and they can stick
        # around longer. This gives a bit of a guarantee they do not live past
        # this point. The most likely scenario for this is a crashed GC, and
        # not all servers reconnecting.
        await asyncio.sleep(70)
        await self._redis.keys("gc-server:*")

    async def _follow_stream(self):
        while True:
            data = await self._redis.xread({"gc-stream": "$"}, block=0)
            for _, entry in data[0][1]:
                # Ignore messages from ourselves.
                if entry["gc-id"] == self._gc_id:
                    continue

                if "new-direct-ipv4" in entry:
                    server_id = entry["new-direct-ipv4"]
                    server = json.loads(entry["server"])
                    await self.application.update_external_direct_ip("ipv4", server_id, server)
                    continue

                if "new-direct-ipv6" in entry:
                    server_id = entry["new-direct-ipv6"]
                    server = json.loads(entry["server"])
                    await self.application.update_external_direct_ip("ipv6", server_id, server)
                    continue

                if "update" in entry:
                    server_id = entry["update"]
                    info = json.loads(entry["info"])
                    await self.application.update_external_server(server_id, info)
                    continue

                if "delete" in entry:
                    server_id = entry["delete"]
                    self.application.remove_server(server_id)
                    continue

                log.error("Internal error: saw invalid entry on stream: %s", entry)

    async def update_info(self, server_id, info):
        info_str = json.dumps(info)
        await self._redis.set(f"gc-server:{server_id}", info_str, ex=60)
        await self._redis.xadd(
            "gc-stream", {"gc-id": self._gc_id, "update": server_id, "info": info_str}, approximate=1000
        )

    async def direct_ip(self, server_id, server_ip, server_port):
        # Keep track of the IP this server has.
        if isinstance(server_ip, ipaddress.IPv6Address):
            type = "ipv6"
        else:
            type = "ipv4"
        server_str = json.dumps({"ip": str(server_ip), "port": server_port})
        if await self._redis.set(f"gc-direct-{type}:{server_id}", server_str) > 0:
            await self._redis.xadd(
                "gc-stream",
                {"gc-id": self._gc_id, f"new-direct-{type}": server_id, "server": server_str},
                approximate=1000,
            )

    async def server_offline(self, server_id):
        await self._redis.delete(f"gc-direct-ipv4:{server_id}")
        await self._redis.delete(f"gc-direct-ipv6:{server_id}")
        await self._redis.delete(f"gc-server:{server_id}")
        await self._redis.xadd("gc-stream", {"gc-id": self._gc_id, "delete": server_id}, approximate=1000)


@click_helper.extend
@click.option(
    "--redis-url",
    help="URL of the redis server.",
    default="redis://localhost",
)
def click_database_redis(redis_url):
    global _redis_url

    _redis_url = redis_url
