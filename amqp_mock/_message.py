from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

__all__ = ("MessageStatus", "Message", "QueuedMessage",)


class MessageStatus(str, Enum):
    INIT = "INIT"
    CONSUMING = "CONSUMING"
    ACKED = "ACKED"
    NACKED = "NACKED"


class Message:
    __slots__ = ("value", "id", "exchange", "routing_key", "properties",)

    def __init__(self, value: Any, *,
                 id: Optional[str] = None,
                 exchange: Optional[str] = None,
                 routing_key: Optional[str] = None,
                 properties: Optional[Dict[str, Any]] = None) -> None:
        self.value = value
        self.id = id or str(uuid4())
        self.exchange = exchange or ""
        self.routing_key = routing_key or ""
        self.properties = properties

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "value": self.value,
            "exchange": self.exchange,
            "routing_key": self.routing_key,
            "properties": self.properties,
        }

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> 'Message':
        return Message(
            value=payload.get("value"),
            id=payload.get("id"),
            exchange=payload.get("exchange"),
            routing_key=payload.get("routing_key"),
            properties=payload.get("properties"),
        )

    def __repr__(self) -> str:
        return (f"<Message value={self.value!r}, "
                f"exchange={self.exchange!r}, "
                f"routing_key={self.routing_key!r}>")


class QueuedMessage:
    __slots__ = ("_message", "_queue", "_status",)

    def __init__(self, message: Message,
                 queue: str,
                 status: MessageStatus = MessageStatus.INIT) -> None:
        self._message = message
        self._queue = queue
        self._status = status

    @property
    def status(self) -> MessageStatus:
        return self._status

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def message(self) -> Message:
        return self._message

    def set_status(self, status: MessageStatus) -> None:
        assert isinstance(status, MessageStatus)
        self._status = status

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": self._message.to_dict(),
            "queue": self._queue,
            "status": self._status.value,
        }

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> 'QueuedMessage':
        message = Message.from_dict(payload["message"])
        return QueuedMessage(message, payload["queue"], MessageStatus(payload["status"]))

    def __repr__(self) -> str:
        return (f"<QueuedMessage message={self._message!r}, "
                f"queue={self._queue!r}, "
                f"status={self._status!s}>")
