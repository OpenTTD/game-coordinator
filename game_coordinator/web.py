import logging

from aiohttp import web
from aiohttp.web_log import AccessLogger

log = logging.getLogger(__name__)
routes = web.RouteTableDef()


@routes.get("/healthz")
async def healthz_handler(request):
    return web.HTTPOk()


@routes.route("*", "/{tail:.*}")
async def fallback(request):
    log.warning("Unexpected URL: %s", request.url)
    return web.HTTPNotFound()


class ErrorOnlyAccessLogger(AccessLogger):
    def log(self, request, response, time):
        # Only log if the status was not successful
        if not (200 <= response.status < 400):
            super().log(request, response, time)


def start_webserver(bind, web_port):
    webapp = web.Application()
    webapp.add_routes(routes)

    web.run_app(webapp, host=bind, port=web_port, access_log_class=ErrorOnlyAccessLogger)
