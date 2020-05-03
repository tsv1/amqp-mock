import json
from typing import Any
from uuid import uuid4


def to_binary(message: Any) -> bytes:
    return json.dumps(message).encode()


def random_uuid() -> str:
    return str(uuid4())
