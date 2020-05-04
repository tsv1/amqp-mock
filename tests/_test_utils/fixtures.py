import pytest

from amqp_mock import AmqpMockClient, AmqpMockServer, AmqpServer, HttpServer, Storage

from .amqp_client import AmqpClient


@pytest.fixture()
def mock_client():
    return AmqpMockClient("localhost", 8080)


@pytest.fixture()
async def mock_server():
    storage = Storage()
    http_server = HttpServer(storage, "localhost", 8080)
    amqp_server = AmqpServer(storage, "localhost", 5674)
    mock = AmqpMockServer(http_server, amqp_server)

    await mock.start()
    yield mock
    await mock.stop()


@pytest.fixture()
async def amqp_client():
    client = AmqpClient("localhost", 5674)

    await client.connect()
    yield client
    await client.close()
