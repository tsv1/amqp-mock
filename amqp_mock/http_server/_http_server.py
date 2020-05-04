from aiohttp import web
from aiohttp.web import json_response

from .._message import Message
from .._storage import Storage
from ._http_route import route

__all__ = ("HttpServer",)


class HttpServer:
    def __init__(self, storage: Storage, host: str = "0.0.0.0", port: int = 80) -> None:
        self._storage = storage
        self._host = host
        self._port = port

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @route("GET", "/healthcheck")
    async def healthcheck(self, request: web.Request) -> web.Response:
        return json_response("200 OK")

    @route("DELETE", "/")
    async def reset(self, request: web.Request) -> web.Response:
        await self._storage.clear()
        return json_response()

    @route("GET", "/exchanges/{exchange:.*}/messages")
    async def get_published_messages(self, request: web.Request) -> web.Response:
        exchange = request.match_info["exchange"]
        messages = await self._storage.get_messages_from_exchange(exchange)
        return json_response([msg.to_dict() for msg in messages])

    @route("DELETE", "/exchanges/{exchange:.*}/messages")
    async def delete_published_messages(self, request: web.Request) -> web.Response:
        exchange = request.match_info["exchange"]
        await self._storage.delete_messages_from_exchange(exchange)
        return json_response()

    @route("POST", "/queues/{queue:.*}/messages")
    async def publish_message(self, request: web.Request) -> web.Response:
        queue = request.match_info["queue"]
        payload = await request.json()
        await self._storage.add_message_to_queue(queue, Message.from_dict(payload))
        return json_response()

    @route("GET", "/queues/{queue:.*}/messages/history")
    async def get_consumed_messages(self, request: web.Request) -> web.Response:
        queue = request.match_info["queue"]
        messages = await self._storage.get_history()
        return json_response([msg.to_dict() for msg in messages if msg.queue == queue])

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name} host={self._host!r} port={self._port!r}>"
