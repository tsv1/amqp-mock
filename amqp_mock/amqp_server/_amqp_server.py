import json
from asyncio.streams import StreamReader, StreamWriter
from typing import Any, AsyncGenerator, Dict, List, Optional

from .._message import Message, MessageStatus
from .._storage import Storage
from ._amqp_connection import AmqpConnection

__all__ = ("AmqpServer",)


class AmqpServer:
    def __init__(self, storage: Storage, host: str = "0.0.0.0", port: Optional[int] = None,
                 server_properties: Optional[Dict[str, Any]] = None) -> None:
        self._storage = storage
        self._host = host
        self._port = port
        self._server_properties = server_properties if server_properties else {
            "capabilities": {
                "publisher_confirms": True,
                "exchange_exchange_bindings": True,
                "basic.nack": True,
                "consumer_cancel_notify": True,
                "connection.blocked": True,
                "consumer_priorities": True,
                "authentication_failure_close": True,
                "per_consumer_qos": True,
                "direct_reply_to": True
            },
            "cluster_name": "<cluster_name>",
            "copyright": "<copyright>",
            "information": "<information>",
            "platform": "<platform>",
            "product": "<product>",
            "version": "<version>",
        }
        self._connections: List[AmqpConnection] = []

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> Optional[int]:
        return self._port

    @port.setter
    def port(self, value: int) -> None:
        self._port = value

    async def _on_bind(self, queue: str, exchange: str, routing_key: str) -> None:
        await self._storage.bind_queue_to_exchange(queue, exchange, routing_key)

    async def _on_declare_exchange(self, exchange: str, exchange_type: str) -> None:
        await self._storage.declare_exchange(exchange, exchange_type)

    async def _on_declare_queue(self, queue: str) -> None:
        await self._storage.declare_queue(queue)

    async def _on_publish(self, message: Message) -> None:
        try:
            message.value = json.loads(message.value.decode())
        except (TypeError, ValueError):
            message.value = str(message.value)
        await self._storage.add_message_to_exchange(message.exchange, message)

    async def _on_consume(self, queue_name: str) -> AsyncGenerator[Message, None]:
        async for message in self._storage.get_next_message(queue_name):
            await self._storage.change_message_status(message.id, MessageStatus.CONSUMING)
            yield message

    async def _on_ack(self, message_id: str) -> None:
        await self._storage.change_message_status(message_id, MessageStatus.ACKED)

    async def _on_nack(self, message_id: str) -> None:
        await self._storage.change_message_status(message_id, MessageStatus.NACKED)

    async def _on_close(self, connection: AmqpConnection) -> None:
        self._connections.remove(connection)

    def __call__(self, reader: StreamReader, writer: StreamWriter) -> AmqpConnection:
        connection = AmqpConnection(reader, writer, self._on_consume, self._server_properties)
        connection.on_publish(self._on_publish) \
                  .on_bind(self._on_bind) \
                  .on_declare_exchange(self._on_declare_exchange) \
                  .on_declare_queue(self._on_declare_queue) \
                  .on_ack(self._on_ack) \
                  .on_nack(self._on_nack) \
                  .on_close(self._on_close)
        self._connections += [connection]
        return connection

    def pre_shutdown(self) -> None:
        pass

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        for connection in self._connections:
            await connection.close()

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name} host={self._host!r} port={self._port!r}>"
