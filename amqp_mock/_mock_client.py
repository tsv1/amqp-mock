from typing import Callable, List

from aiohttp import ClientSession

from ._message import Message, QueuedMessage


class AmqpMockClient:
    def __init__(self, host: str = "localhost", port: int = 80, *,
                 session_factory: Callable[[], ClientSession] = ClientSession):
        self._session_factory = session_factory
        self._host = host
        self._port = port
        self._api_url = f"http://{self._host}:{self._port}"

    async def healthcheck(self) -> None:
        url = f"{self._api_url}/healthcheck"
        async with self._session_factory() as session:
            async with session.get(url) as resp:
                assert resp.status == 200, resp

    async def reset(self) -> None:
        url = f"{self._api_url}/"
        async with self._session_factory() as session:
            async with session.delete(url) as resp:
                assert resp.status == 200, resp

    async def get_exchange_messages(self, exchange_name: str) -> List[Message]:
        url = f"{self._api_url}/exchanges/{exchange_name}/messages"
        async with self._session_factory() as session:
            async with session.get(url) as resp:
                assert resp.status == 200, resp
                body = await resp.json()
                return [Message.from_dict(x) for x in body]

    async def delete_exchange_messages(self, exchange_name: str) -> None:
        url = f"{self._api_url}/exchanges/{exchange_name}/messages"
        async with self._session_factory() as session:
            async with session.delete(url) as resp:
                assert resp.status == 200, resp

    async def publish_message(self, queue_name: str, message: Message) -> None:
        assert isinstance(message, Message)
        url = f"{self._api_url}/queues/{queue_name}/messages"
        async with self._session_factory() as session:
            async with session.post(url, json=message.to_dict()) as resp:
                assert resp.status == 200, resp

    async def get_queue_message_history(self, queue_name: str) -> List[QueuedMessage]:
        url = f"{self._api_url}/queues/{queue_name}/messages/history"
        async with self._session_factory() as session:
            async with session.get(url) as resp:
                assert resp.status == 200, resp
                body = await resp.json()
                return [QueuedMessage.from_dict(x) for x in body]

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name} host={self._host!r} port={self._port!r}>"
