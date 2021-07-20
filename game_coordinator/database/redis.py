import asyncio
import aioredis
import click
import ipaddress
import json
import logging
import secrets
import time

from aioredis import ResponseError

from openttd_helpers import click_helper

log = logging.getLogger(__name__)

_redis_url = None

# Server update every 30 seconds, so if we haven't seen it for twice that it
# means it is no longer there.
TTL_SERVER = 60
# Give a bit of grace period to forget about NewGRFs, so server restarts don't
# bump the counter.
TTL_NEWGRF = TTL_SERVER + 60
# GC claims are refreshed every 30 seconds, so after twice that, a GC has
# crashed / exited and it can be reclaimed.
TTL_GC_ID = 60
# Keep statistics for 30 days.
TTL_STATS = 3600 * 24 * 30
# Slightly more than the interval the TURN server announce itself.
TTL_TURN_SERVER = 15


class Database:
    def __init__(self):
        # The application claiming this Database instance should set this.
        self.application = None
        # Set by sync_and_monitor() on start-up.
        self._gc_id = -1

        self._redis = aioredis.from_url(_redis_url, decode_responses=True)

        log.info("Using redis as backend")

    async def sync_and_monitor(self):
        # Check with redis if any of the keys are available.
        while True:
            for i in range(16):
                res = await self._redis.set(f"gc-id:{i}", 1, ex=TTL_GC_ID, nx=True)
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
            await self._redis.set(f"gc-id:{self._gc_id}", 1, ex=TTL_GC_ID)

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

                if message["data"].startswith("turn-server:"):
                    _, _, turn_server = message["data"].partition(":")

                    await self.application.remove_turn_server(turn_server)

                if message["data"].startswith("gc-newgrf:"):
                    _, _, grfid_md5sum = message["data"].partition(":")
                    grfid, _, md5sum = grfid_md5sum.partition("-")

                    await self.application.remove_newgrf_from_table(grfid, md5sum)

                if message["data"].startswith("gc-server:"):
                    _, _, server_id = message["data"].partition(":")

                    await self._redis.delete(f"gc-direct-ipv4:{server_id}")
                    await self._redis.delete(f"gc-direct-ipv6:{server_id}")

                    await self.application.remove_server(server_id)

    async def _scan_existing_servers(self):
        turn_servers = await self._redis.keys("turn-server:*")
        for turn_server in turn_servers:
            _, _, connection_string = turn_server.partition(":")
            await self.application.add_turn_server(connection_string)

        newgrfs = await self._redis.keys("gc-newgrf:*")
        for newgrf in newgrfs:
            _, _, grfid_md5sum = newgrf.partition(":")
            grfid, _, md5sum = grfid_md5sum.partition("-")

            newgrf_lookup_str = await self._redis.get(newgrf)
            if newgrf_lookup_str is None:
                continue

            newgrf_lookup = json.loads(newgrf_lookup_str)
            newgrf = {
                "grfid": int(grfid),
                "md5sum": md5sum,
                "name": newgrf_lookup["name"],
            }
            await self.application.newgrf_added(newgrf_lookup["index"], newgrf)

        servers = await self._redis.keys("gc-server:*")
        for server in servers:
            _, _, server_id = server.partition(":")

            info_str = await self._redis.get(server)
            if info_str is None:
                # Server left in the meantime. The stream will update the rest.
                continue

            info = json.loads(info_str)
            await self.application.update_external_server(server_id, info)

        servers = await self._redis.keys("gc-server-newgrf:*")
        for server in servers:
            _, _, server_id = server.partition(":")

            newgrf_indexed_str = await self._redis.get(server)
            if newgrf_indexed_str is None:
                # Server left in the meantime. The stream will update the rest.
                continue

            newgrf_indexed = json.loads(newgrf_indexed_str)
            await self.application.update_newgrf_external_server(server_id, newgrf_indexed)

        direct_ipv4s = await self._redis.keys("gc-direct-ipv4:*")
        for direct_ipv4 in direct_ipv4s:
            _, _, server_id = direct_ipv4.partition(":")

            server_str = await self._redis.get(direct_ipv4)
            if server_str is None:
                # Server left in the meantime. The stream will update the rest.
                continue

            server = json.loads(server_str)
            await self.application.update_external_direct_ip(server_id, "ipv4", server["ip"], server["port"])

        direct_ipv6s = await self._redis.keys("gc-direct-ipv6:*")
        for direct_ipv6 in direct_ipv6s:
            _, _, server_id = direct_ipv6.partition(":")

            server_str = await self._redis.get(direct_ipv6)
            if server_str is None:
                # Server left in the meantime. The stream will update the rest.
                continue

            server = json.loads(server_str)
            await self.application.update_external_direct_ip(server_id, "ipv6", server["ip"], server["port"])

        # We wait for 70 seconds, well past the TTL of servers, and query all
        # keys. This forces redis under all condition to expire servers that
        # are past the TTL. This is picked up by _monitor_expire() and the
        # server is removed. This is needed, as redis otherwise gives far less
        # guarantees servers are expired after their TTL, and they can stick
        # around longer. This gives a bit of a guarantee they do not live past
        # this point. The most likely scenario for this is a crashed GC, and
        # not all servers reconnecting.
        await asyncio.sleep(TTL_SERVER + 10)
        await self._redis.keys("gc-server:*")

    async def _follow_stream(self):
        lookup_table = {
            "turn-server": self.application.add_turn_server,
            "new-direct-ip": self.application.update_external_direct_ip,
            "update": self.application.update_external_server,
            "update-newgrf": self.application.update_newgrf_external_server,
            "newgrf-added": self.application.newgrf_added,
            "delete": self.application.remove_server,
            "stun-result": self.application.stun_result,
            "send-stun-request": self.application.send_server_stun_request,
            "send-stun-connect": self.application.send_server_stun_connect,
            "send-turn-connect": self.application.send_server_turn_connect,
            "send-connect-failed": self.application.send_server_connect_failed,
        }
        current_id = "$"

        while True:
            data = await self._redis.xread({"gc-stream": current_id}, block=0)
            for entry_id, entry in data[0][1]:
                current_id = entry_id

                # Ignore messages from ourselves.
                if entry["gc-id"] == self._gc_id:
                    continue

                if "type" not in entry:
                    log.error("Internal error: saw unknown entry on stream: %r", entry)
                    continue

                proc = lookup_table.get(entry["type"])
                if proc is None:
                    log.error("Internal error: saw unknown type on stream: %s", entry["type"])
                    continue
                payload = json.loads(entry["payload"])
                await proc(**payload)

    def get_server_id(self):
        return int(self._gc_id)

    async def add_to_stream(self, entry_type, payload):
        await self._redis.xadd(
            "gc-stream", {"gc-id": self._gc_id, "type": entry_type, "payload": json.dumps(payload)}, maxlen=1000
        )

    async def announce_turn_server(self, connection_string):
        if await self._redis.set(f"turn-server:{connection_string}", 1, ex=TTL_TURN_SERVER):
            await self.add_to_stream("turn-server", {"connection_string": connection_string})

    async def newgrf_in_use(self, newgrf):
        await self._redis.expire(f"gc-newgrf:{newgrf['grfid']}-{newgrf['md5sum']}", TTL_NEWGRF)

    async def newgrf_assign_index(self, newgrf):
        newgrf_lookup_str = await self._redis.get(f"gc-newgrf:{newgrf['grfid']}-{newgrf['md5sum']}")
        if newgrf_lookup_str is not None:
            newgrf_lookup = json.loads(newgrf_lookup_str)

            if newgrf_lookup["name"] is not None or newgrf["name"] is None:
                # Make sure the entry lives a bit longer.
                await self.newgrf_in_use(newgrf)
                return newgrf_lookup["index"]

            # There is an entry in the table, but it doesn't have a name. This
            # happens when old servers announced the NewGRF. But we do have a
            # name now. So update the entry to tell the name.
            newgrf_lookup["name"] = newgrf["name"]
            await self._redis.set(
                f"gc-newgrf:{newgrf['grfid']}-{newgrf['md5sum']}", json.dumps(newgrf_lookup), ex=TTL_NEWGRF
            )

            await self.application.newgrf_added(newgrf_lookup["index"], newgrf)
            await self.add_to_stream("newgrf-added", {"index": newgrf_lookup["index"], "newgrf": newgrf})

            return newgrf_lookup["index"]

        newgrf_lookup = {
            "index": await self._redis.incr("gc-newgrf-counter"),
            "name": newgrf["name"],
        }
        res = await self._redis.set(
            f"gc-newgrf:{newgrf['grfid']}-{newgrf['md5sum']}", json.dumps(newgrf_lookup), nx=True, ex=TTL_NEWGRF
        )
        if res is not None:
            await self.application.newgrf_added(newgrf_lookup["index"], newgrf)
            await self.add_to_stream("newgrf-added", {"index": newgrf_lookup["index"], "newgrf": newgrf})

            return newgrf_lookup["index"]

        # Another instance sneaked in between our get and set, so fetch
        # the key again. This time it is guaranteed to exist.
        newgrf_lookup_str = await self._redis.get(f"gc-newgrf:{newgrf['grfid']}-{newgrf['md5sum']}")
        newgrf_lookup = json.loads(newgrf_lookup_str)

        await self.newgrf_in_use(newgrf)
        return newgrf_lookup["index"]

    async def update_newgrf(self, server_id, newgrfs_indexed):
        await self._redis.set(f"gc-server-newgrf:{server_id}", json.dumps(newgrfs_indexed), ex=TTL_SERVER)
        await self.add_to_stream("update-newgrf", {"server_id": server_id, "newgrfs_indexed": newgrfs_indexed})

    async def update_info(self, server_id, info):
        await self._redis.set(f"gc-server:{server_id}", json.dumps(info), ex=TTL_SERVER)
        await self.add_to_stream("update", {"server_id": server_id, "info": info})

    async def direct_ip(self, server_id, server_ip, server_port):
        # Keep track of the IP this server has.
        type = "ipv6" if isinstance(server_ip, ipaddress.IPv6Address) else "ipv4"
        res = await self._redis.set(
            f"gc-direct-{type}:{server_id}", json.dumps({"ip": str(server_ip), "port": server_port})
        )
        if res > 0:
            await self.add_to_stream(
                "new-direct-ip", {"server_id": server_id, "type": type, "ip": str(server_ip), "port": server_port}
            )

    async def server_offline(self, server_id):
        await self._redis.delete(f"gc-direct-ipv4:{server_id}")
        await self._redis.delete(f"gc-direct-ipv6:{server_id}")
        await self._redis.delete(f"gc-server:{server_id}")
        await self._redis.delete(f"gc-server-newgrf:{server_id}")
        await self.add_to_stream("delete", {"server_id": server_id})

    async def stats_verify(self, connection_type_name):
        await self._stats("verify", connection_type_name)

    async def stats_connect(self, method_name, result):
        key = "connect" if result else "connect-failed"

        await self._stats(key, method_name)

    async def stats_listing(self, game_info_version):
        await self._stats("listing", game_info_version)

    async def stats_turn(self, side):
        await self._stats("turn", side)

    async def stats_turn_usage(self, total_bytes, time_connected):
        await self._stats("turn", "bytes", amount=total_bytes)
        await self._stats("turn", "time", amount=int(time_connected))

    async def _stats(self, key, subkey, amount=1):
        # Put all stats of a single day in one bucket.
        day_since_1970 = int(time.time()) // (3600 * 24)

        key = f"stats-{key}:{day_since_1970}-{subkey}"

        # Keep statistics for one month.
        await self._redis.expire(key, TTL_STATS)
        await self._redis.incr(key, amount)

    async def get_stats(self, key):
        result = {}

        stats = await self._redis.keys(f"stats-{key}:*")
        for stat in stats:
            _, _, time_subkey = stat.partition(":")
            day_since_1970, _, subkey = time_subkey.partition("-")

            if day_since_1970 not in result:
                result[day_since_1970] = {}

            result[day_since_1970][subkey] = await self._redis.get(stat)

        return result

    async def create_turn_ticket(self):
        while True:
            ticket_left = secrets.token_hex(8)
            ticket_right = secrets.token_hex(8)
            if await self._redis.set(f"turn-ticket:{ticket_left}", ticket_right, ex=30, nx=True):
                break

        return f"{ticket_left}:{ticket_right}"

    async def validate_turn_ticket(self, ticket):
        ticket_left, ticket_right = ticket.split(":")
        validation = await self._redis.get(f"turn-ticket:{ticket_left}")
        return validation == ticket_right

    async def stun_result(self, token, interface_number, peer_ip, peer_port):
        await self.add_to_stream(
            "stun-result",
            {
                "token": token,
                "interface_number": interface_number,
                "peer_type": "ipv6" if isinstance(peer_ip, ipaddress.IPv6Address) else "ipv4",
                "peer_ip": str(peer_ip),
                "peer_port": peer_port,
            },
        )

    async def send_server_stun_request(self, server_id, protocol_version, token):
        await self.add_to_stream(
            "send-stun-request",
            {
                "server_id": server_id,
                "protocol_version": protocol_version,
                "token": token,
            },
        )

    async def send_server_stun_connect(
        self, server_id, protocol_version, token, tracking_number, interface_number, peer_ip, peer_port
    ):
        await self.add_to_stream(
            "send-stun-connect",
            {
                "server_id": server_id,
                "protocol_version": protocol_version,
                "token": token,
                "tracking_number": tracking_number,
                "interface_number": interface_number,
                "peer_ip": peer_ip,
                "peer_port": peer_port,
            },
        )

    async def send_server_turn_connect(
        self, server_id, protocol_version, token, tracking_number, ticket, connection_string
    ):
        await self.add_to_stream(
            "send-turn-connect",
            {
                "server_id": server_id,
                "protocol_version": protocol_version,
                "token": token,
                "tracking_number": tracking_number,
                "ticket": ticket,
                "connection_string": connection_string,
            },
        )

    async def send_server_connect_failed(self, server_id, protocol_version, token):
        await self.add_to_stream(
            "send-connect-failed",
            {
                "server_id": server_id,
                "protocol_version": protocol_version,
                "token": token,
            },
        )


@click_helper.extend
@click.option(
    "--redis-url",
    help="URL of the redis server.",
    default="redis://localhost",
)
def click_database_redis(redis_url):
    global _redis_url

    _redis_url = redis_url
