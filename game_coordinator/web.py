import asyncio
import logging

from aiohttp import web
from aiohttp.web_log import AccessLogger

log = logging.getLogger(__name__)
routes = web.RouteTableDef()

DB_INSTANCE = None


@routes.get("/healthz")
async def healthz_handler(request):
    return web.HTTPOk()


@routes.get("/stats")
async def stats_handler(request):
    return web.json_response(
        {
            "verify": await DB_INSTANCE.get_stats("verify"),
            "listing": await DB_INSTANCE.get_stats("listing"),
            "listing-version": await DB_INSTANCE.get_stats("listing-version"),
            "connect": await DB_INSTANCE.get_stats("connect"),
            "connect-failed": await DB_INSTANCE.get_stats("connect-failed"),
            "connect-method-failed": await DB_INSTANCE.get_stats("connect-method-failed"),
            "turn": await DB_INSTANCE.get_stats("turn"),
        }
    )


@routes.route("*", "/{tail:.*}")
async def fallback(request):
    log.warning("Unexpected URL: %s", request.url)
    return web.HTTPNotFound()


class ErrorOnlyAccessLogger(AccessLogger):
    def log(self, request, response, time):
        # Only log if the status was not successful
        if not (200 <= response.status < 400):
            super().log(request, response, time)


def start_webserver(bind, web_port, db_instance):
    global DB_INSTANCE
    DB_INSTANCE = db_instance

    webapp = web.Application()
    webapp.add_routes(routes)

    # aiohttp normally takes over all kind of asyncio function, especially on
    # shutdown. But we need to be in control of the shutdown to ensure it is
    # done graceful. This means we need to call an internal aiohttp function
    # to prevent the asyncio setup aiohttp normally does.
    asyncio.ensure_future(
        web._run_app(webapp, host=bind, port=web_port, access_log_class=ErrorOnlyAccessLogger, handle_signals=False)
    )
