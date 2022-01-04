import json
from typing import Any, Dict, List, Union
from uuid import uuid4

from amqp_mock import Message, QueuedMessage


def to_binary(message: Any) -> bytes:
    return json.dumps(message).encode()


def random_uuid() -> str:
    return str(uuid4())


_MessageType = Union[QueuedMessage, Message]


def to_dict(smth: List[_MessageType]) -> List[Dict[str, Any]]:
    return [x.to_dict() for x in smth]
