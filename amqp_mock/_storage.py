from asyncio import Queue
from collections import defaultdict
from typing import AsyncGenerator, DefaultDict, Dict, List, Tuple

from ._message import Message, MessageStatus, QueuedMessage


class Storage:
    def __init__(self) -> None:
        self._exchanges: Dict[str, List[Message]] = {}
        self._exchange_types: Dict[str, str] = {}
        self._queues: Dict[str, Queue[Message]] = {}
        self._history: List[Tuple[str, QueuedMessage]] = []
        self._binds: DefaultDict[str, Dict[str, str]] = defaultdict(dict)

    async def clear(self) -> None:
        self._exchanges = {}
        self._exchange_types = {}
        self._queues = {}
        self._history = []
        self._binds = defaultdict(dict)

    async def add_message_to_exchange(self, exchange: str, message: Message) -> None:
        await self.declare_exchange(exchange)
        self._exchanges[exchange].insert(0, message)

        exchange_type = self._exchange_types[exchange]
        binds = self._binds.get(exchange)

        if binds:
            if exchange_type == "direct":
                routing_key = message.routing_key

                if routing_key in binds:
                    await self.add_message_to_queue(binds[routing_key], message)
            elif exchange_type == "fanout":
                for queue in binds.values():
                    await self.add_message_to_queue(queue, message)
            else:
                raise RuntimeError(f"{exchange_type} exchanges not supported")

    async def bind_queue_to_exchange(self, queue: str, exchange: str,
                                     routing_key: str = "") -> None:
        await self.declare_queue(queue)
        self._binds[exchange][routing_key] = queue

    async def declare_exchange(self, exchange: str, exchange_type: str = "direct") -> None:
        if exchange not in self._exchanges:
            self._exchanges[exchange] = []
            self._exchange_types[exchange] = exchange_type

    async def declare_queue(self, queue: str) -> None:
        if queue not in self._queues:
            self._queues[queue] = Queue()
            await self.bind_queue_to_exchange(queue, exchange="", routing_key=queue)

    async def get_messages_from_exchange(self, exchange: str) -> List[Message]:
        if exchange not in self._exchanges:
            return []
        return self._exchanges[exchange]

    async def delete_messages_from_exchange(self, exchange: str) -> None:
        if exchange in self._exchanges:
            self._exchanges[exchange] = []

    async def add_message_to_queue(self, queue: str, message: Message) -> None:
        await self.declare_queue(queue)
        await self._queues[queue].put(message)
        self._history.append((message.id, QueuedMessage(message, queue)))

    async def get_history(self) -> List[QueuedMessage]:
        return [message[1] for message in self._history[::-1]]

    async def change_message_status(self, message_id: str, status: MessageStatus) -> None:
        for msg_id, message in self._history:
            if msg_id == message_id:
                message.set_status(status)

    async def get_next_message(self, queue: str) -> AsyncGenerator[Message, None]:
        if queue not in self._queues:
            self._queues[queue] = Queue()

        while True:
            message = await self._queues[queue].get()
            yield message
            self._queues[queue].task_done()
