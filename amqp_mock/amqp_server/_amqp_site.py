from asyncio import start_server
from asyncio.streams import StreamReader, StreamWriter
from typing import Any, Callable, Optional, cast

from aiohttp.web import BaseSite

from ._amqp_runner import AmqpRunner

__all__ = ("AmqpSite",)


class AmqpSite(BaseSite):
    def __init__(self, runner: AmqpRunner, *, host: str, port: Optional[int]):
        super().__init__(runner)
        self._host = host
        self._port = port

    @property
    def port(self) -> Optional[int]:
        return self._port

    async def start(self) -> None:
        await super().start()
        callback = cast(Callable[[StreamReader, StreamWriter], Any], self._runner.server)
        self._server = await start_server(callback, host=self._host, port=self._port)
        if self._server.sockets is not None and len(self._server.sockets) > 0:
            self._port = self._server.sockets[0].getsockname()[1]

    def name(self) -> str:
        return "ampq://{host}:{port}".format(host=self._host, port=self._port)
