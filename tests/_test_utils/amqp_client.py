import asyncio
from types import TracebackType
from typing import List, Optional, Type, Union

import aiormq
import pamqp.specification as spec
from aiormq.types import DeliveredMessage
from rtry import CancelledError, retry


class AmqpClient:
    def __init__(self, host: str = "localhost", port: int = 5672, vhost: str = "/") -> None:
        self._host = host
        self._port = port
        self._vhost = vhost
        self._connection: Union[aiormq.Connection, None] = None
        self._channel: Union[aiormq.Channel, None] = None
        self._messages: List[DeliveredMessage] = []

    async def connect(self) -> None:
        self._connection = await aiormq.connect(f"amqp://{self._host}:{self._port}/{self._vhost}")
        self._channel = await self._connection.channel()

    async def close(self) -> None:
        await self._channel.close()
        await self._connection.close()

    async def basic_qos(self, prefetch_count: int) -> None:
        res = await self._channel.basic_qos(prefetch_count=prefetch_count)
        assert isinstance(res, spec.Basic.QosOk)

    async def declare_exchange(self, exchange_name: str, exchange_type: str = "direct") -> None:
        res = await self._channel.exchange_declare(exchange_name, exchange_type=exchange_type)
        assert isinstance(res, spec.Exchange.DeclareOk)

    async def declare_queue(self, queue_name: str) -> None:
        res = await self._channel.queue_declare(queue_name)
        assert isinstance(res, spec.Queue.DeclareOk)

    async def queue_bind(self, queue_name: str, exchange_name: str) -> None:
        res = await self._channel.queue_bind(queue_name, exchange_name, routing_key="")
        assert isinstance(res, spec.Queue.BindOk)

    async def publish(self, message: bytes, exchange_name: str) -> None:
        res = await self._channel.basic_publish(message, exchange=exchange_name, routing_key="")
        assert isinstance(res, spec.Basic.Ack)

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
        assert isinstance(res, spec.Basic.ConsumeOk)

    async def consume_ack(self, queue_name: str) -> None:
        res = await self._channel.basic_consume(queue_name, self._on_message_do_ack, no_ack=True)
        assert isinstance(res, spec.Basic.ConsumeOk)

    async def consume_nack(self, queue_name: str) -> None:
        res = await self._channel.basic_consume(queue_name, self._on_message_do_nack, no_ack=True)
        assert isinstance(res, spec.Basic.ConsumeOk)

    async def wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def wait_for(self, message_count: int, timeout: float = 1.0) -> List[DeliveredMessage]:
        async def _wait_for() -> bool:
            return len(self._messages) >= message_count

        try:
            await retry(until=lambda r: not r, timeout=timeout)(_wait_for)()
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
