import pytest

from amqp_mock import AmqpMockClient, AmqpMockServer, AmqpServer, HttpServer, Storage

from .amqp_client import AMQPClient


@pytest.fixture()
def amqp_mock_client():
    return AmqpMockClient("localhost", 8080)


@pytest.fixture()
async def amqp_mock_server():
    storage = Storage()
    http_server = HttpServer(storage, "localhost", 8080)
    amqp_server = AmqpServer(storage, "localhost", 5674)
    mock = AmqpMockServer(http_server, amqp_server)

    yield await mock.start()

    await mock.stop()


@pytest.fixture()
async def amqp_client():
    client = AMQPClient("localhost", 5674)
    await client.connect()

    yield client

    await client.close()
