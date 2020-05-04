from types import TracebackType
from typing import Callable, List, Optional, Type

from aiohttp import web

from ._mock_client import AmqpMockClient
from .amqp_server import AmqpRunner, AmqpServer, AmqpSite
from .http_server import HttpRoute, HttpServer


class AmqpMockServer:
    def __init__(self, http_server: HttpServer, amqp_server: AmqpServer,
                 client_factory: Callable[[str, int], AmqpMockClient] = AmqpMockClient) -> None:
        self._http_server = http_server
        self._amqp_server = amqp_server
        self._client_factory = client_factory
        self._http_runner: Optional[web.AppRunner] = None
        self._amqp_runner: Optional[AmqpRunner] = None
        self._client: Optional[AmqpMockClient] = None

    @property
    def http_server(self) -> HttpServer:
        return self._http_server

    @property
    def amqp_server(self) -> AmqpServer:
        return self._amqp_server

    @property
    def client(self) -> AmqpMockClient:
        if self._client is None:
            self._client = self._client_factory(self._http_server.host, self._http_server.port)
        return self._client

    async def start(self) -> None:
        app = web.Application()
        routes: List[web.RouteDef] = []
        for name in dir(self._http_server):
            handler = getattr(self._http_server, name)
            route = HttpRoute.get_route(handler)
            if route:
                routes += [web.route(route.method, route.path, handler)]
        app.add_routes(routes)

        self._http_runner = web.AppRunner(app)
        await self._http_runner.setup()

        http_site = web.TCPSite(self._http_runner,
                                host=self._http_server.host,
                                port=self._http_server.port)
        await http_site.start()

        self._amqp_runner = AmqpRunner(self._amqp_server)
        await self._amqp_runner.setup()

        amqp_site = AmqpSite(self._amqp_runner,
                             host=self._amqp_server.host,
                             port=self._amqp_server.port)
        await amqp_site.start()

    async def stop(self) -> None:
        if self._http_runner:
            await self._http_runner.cleanup()
        if self._amqp_runner:
            await self._amqp_runner.cleanup()

    def __enter__(self) -> None:
        raise TypeError("Use async with instead")

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        pass

    async def __aenter__(self) -> 'AmqpMockServer':
        await self.start()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        await self.stop()

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name} http_server={self._http_server} amqp_server={self._amqp_server}>"
