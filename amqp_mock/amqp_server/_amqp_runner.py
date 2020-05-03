from aiohttp.web import BaseRunner

from ._amqp_server import AmqpServer

__all__ = ("AmqpRunner",)


class AmqpRunner(BaseRunner):
    def __init__(self, server: AmqpServer) -> None:
        super().__init__()
        self._server = server  # type: ignore

    async def shutdown(self) -> None:
        pass

    async def _make_server(self) -> AmqpServer:  # type: ignore
        return self._server  # type: ignore

    async def _cleanup_server(self) -> None:
        pass
