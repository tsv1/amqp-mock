from typing import Optional

from ._message import Message, MessageStatus, QueuedMessage
from ._mock_client import AmqpMockClient
from ._mock_server import AmqpMockServer
from ._storage import Storage
from ._version import version
from .amqp_server import AmqpServer
from .http_server import HttpServer

__version__ = version
__all__ = ("AmqpServer", "HttpServer", "Storage",
           "AmqpMockClient", "AmqpMockServer", "create_amqp_mock",
           "Message", "MessageStatus", "QueuedMessage",)


def create_amqp_mock(http_server: Optional[HttpServer] = None,
                     amqp_server: Optional[AmqpServer] = None,
                     storage: Optional[Storage] = None) -> AmqpMockServer:
    storage = storage or Storage()
    http_server = http_server or HttpServer(storage)
    amqp_server = amqp_server or AmqpServer(storage)
    server = AmqpMockServer(http_server, amqp_server)
    return server
