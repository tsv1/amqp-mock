from asyncio import Queue
from collections import OrderedDict, defaultdict
from typing import AsyncGenerator, DefaultDict, Dict, List

from ._message import Message, MessageStatus, QueuedMessage


class Storage:
    def __init__(self) -> None:
        self._exchanges: Dict[str, List[Message]] = {}
        self._queues: Dict[str, Queue[Message]] = {}
        self._history: Dict[str, QueuedMessage] = OrderedDict()
        self._binds: DefaultDict[str, Dict[str, str]] = defaultdict(dict)

    async def clear(self) -> None:
        self._exchanges = {}
        self._queues = {}
        self._history = OrderedDict()
        self._binds = defaultdict(dict)

    async def add_message_to_exchange(self, exchange: str, message: Message) -> None:
        if exchange not in self._exchanges:
            self._exchanges[exchange] = []
        self._exchanges[exchange].insert(0, message)

        binds = self._binds.get(exchange)
        routing_key = message.routing_key

        if binds and routing_key in binds:
            await self.add_message_to_queue(binds[routing_key], message)

    async def bind_queue_to_exchange(self, queue: str, exchange: str,
                                     routing_key: str = "") -> None:
        self._binds[exchange][routing_key] = queue

    async def declare_queue(self, queue: str) -> None:
        if queue not in self._queues:
            self._queues[queue] = Queue()

        await self.bind_queue_to_exchange(queue, "", routing_key=queue)

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
        self._history[message.id] = QueuedMessage(message, queue)

    async def get_history(self) -> List[QueuedMessage]:
        return [message for message in self._history.values()][::-1]

    async def change_message_status(self, message_id: str, status: MessageStatus) -> None:
        self._history[message_id].set_status(status)

    async def get_next_message(self, queue: str) -> AsyncGenerator[Message, None]:
        if queue not in self._queues:
            self._queues[queue] = Queue()

        while True:
            message = await self._queues[queue].get()
            yield message
            self._queues[queue].task_done()
