from asyncio import start_server
from asyncio.streams import StreamReader, StreamWriter
from typing import Any, Callable, cast

from aiohttp.web import BaseSite

from ._amqp_runner import AmqpRunner

__all__ = ("AmqpSite",)


class AmqpSite(BaseSite):
    def __init__(self, runner: AmqpRunner, *, host: str, port: int):
        super().__init__(runner)
        self._host = host
        self._port = port

    async def start(self) -> None:
        await super().start()
        callback = cast(Callable[[StreamReader, StreamWriter], Any], self._runner.server)
        self._server = await start_server(callback, host=self._host, port=self._port)

    def name(self) -> str:
        return "ampq://{host}:{port}".format(host=self._host, port=self._port)
