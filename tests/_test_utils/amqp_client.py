import asyncio
from types import TracebackType
from typing import Dict, List, Optional, Type, Union

import aiormq
from aiormq.abc import DeliveredMessage
from pamqp import commands
from rtry import CancelledError, retry


class AmqpClient:
    def __init__(self, host: str = "localhost", port: int = 5672, vhost: str = "/") -> None:
        self._host = host
        self._port = port
        self._vhost = vhost
        self._connection: Union[aiormq.Connection, None] = None
        self._channel: Union[aiormq.Channel, None] = None
        self._messages: List[DeliveredMessage] = []
        self._consumer_tags: Dict[str, str] = {}

    async def connect(self) -> None:
        self._connection = await aiormq.connect(f"amqp://{self._host}:{self._port}/{self._vhost}")
        self._channel = await self._connection.channel()

    async def close(self) -> None:
        await self._channel.close()
        await self._connection.close()

    async def basic_qos(self, prefetch_count: int) -> None:
        res = await self._channel.basic_qos(prefetch_count=prefetch_count)
        assert isinstance(res, commands.Basic.QosOk)

    async def declare_exchange(self, exchange_name: str, exchange_type: str = "direct") -> None:
        res = await self._channel.exchange_declare(exchange_name, exchange_type=exchange_type)
        assert isinstance(res, commands.Exchange.DeclareOk)

    async def declare_queue(self, queue_name: str) -> None:
        res = await self._channel.queue_declare(queue_name)
        assert isinstance(res, commands.Queue.DeclareOk)

    async def queue_bind(self, queue_name: str, exchange_name: str) -> None:
        res = await self._channel.queue_bind(queue_name, exchange_name, routing_key="")
        assert isinstance(res, commands.Queue.BindOk)

    async def publish(self, message: bytes, exchange_name: str, routing_key: str = "") -> None:
        res = await self._channel.basic_publish(message, exchange=exchange_name,
                                                routing_key=routing_key)
        assert isinstance(res, commands.Basic.Ack)

    async def _on_message(self, message: DeliveredMessage) -> None:
        self._messages.append(message)

    async def _on_message_do_ack(self, message: DeliveredMessage) -> None:
        self._messages.append(message)
        res = await self._channel.basic_ack(message.delivery.delivery_tag)
        assert res is None

    async def _on_message_do_nack(self, message: DeliveredMessage) -> None:
        self._messages.append(message)
        res = await self._channel.basic_nack(message.delivery.delivery_tag)
        assert res is None

    async def consume(self, queue_name: str) -> None:
        res = await self._channel.basic_consume(queue_name, self._on_message, no_ack=True)
        assert isinstance(res, commands.Basic.ConsumeOk)
        self._consumer_tags[queue_name] = res.consumer_tag

    async def consume_ack(self, queue_name: str) -> None:
        res = await self._channel.basic_consume(queue_name, self._on_message_do_ack, no_ack=True)
        assert isinstance(res, commands.Basic.ConsumeOk)
        self._consumer_tags[queue_name] = res.consumer_tag

    async def consume_nack(self, queue_name: str) -> None:
        res = await self._channel.basic_consume(queue_name, self._on_message_do_nack, no_ack=True)
        assert isinstance(res, commands.Basic.ConsumeOk)
        self._consumer_tags[queue_name] = res.consumer_tag

    async def consume_cancel(self, queue_name: str) -> None:
        consumer_tag = self._consumer_tags[queue_name]
        res = await self._channel.basic_cancel(consumer_tag)
        assert isinstance(res, commands.Basic.CancelOk)
        del self._consumer_tags[queue_name]

    async def wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def wait_for(self, message_count: int, attempts: Optional[int] = None,
                       timeout: float = 1.0) -> List[DeliveredMessage]:
        async def _wait_for() -> bool:
            return len(self._messages) >= message_count

        try:
            await retry(until=lambda r: not r, attempts=attempts, timeout=timeout)(_wait_for)()
        except CancelledError:
            pass

        return self.get_consumed_messages()

    def get_consumed_messages(self) -> List[DeliveredMessage]:
        return self._messages

    def __enter__(self) -> None:
        raise TypeError("Use async with instead")

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        pass

    async def __aenter__(self) -> 'AmqpClient':
        await self.connect()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        await self.close()
