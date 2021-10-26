from asyncio import Queue
from collections import OrderedDict
from typing import AsyncGenerator, Dict, List

from ._message import Message, MessageStatus, QueuedMessage


class Storage:
    def __init__(self) -> None:
        self._exchanges: Dict[str, List[Message]] = {}
        self._queues: Dict[str, Queue[Message]] = {}
        self._history: Dict[str, QueuedMessage] = OrderedDict()

    async def clear(self) -> None:
        self._exchanges = {}
        self._queues = {}
        self._history = OrderedDict()

    async def add_message_to_exchange(self, exchange: str, message: Message) -> None:
        if exchange not in self._exchanges:
            self._exchanges[exchange] = []
        self._exchanges[exchange].insert(0, message)

    async def get_messages_from_exchange(self, exchange: str) -> List[Message]:
        if exchange not in self._exchanges:
            return []
        return self._exchanges[exchange]

    async def delete_messages_from_exchange(self, exchange: str) -> None:
        if exchange in self._exchanges:
            self._exchanges[exchange] = []

    async def add_message_to_queue(self, queue: str, message: Message) -> None:
        if queue not in self._queues:
            self._queues[queue] = Queue()
        await self._queues[queue].put(message)
        self._history[message.id] = QueuedMessage(message, queue)

    async def get_history(self) -> List[QueuedMessage]:
        return [message for message in self._history.values()][::-1]

    async def change_message_status(self, message_id: str, status: MessageStatus) -> None:
        self._history[message_id].set_status(status)

    async def get_next_message(self, queue: str) -> AsyncGenerator[Message, None]:
        if queue not in self._queues:
            return

        while True:
            yield await self._queues[queue].get()
