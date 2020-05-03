import pytest

from amqp_mock import AmqpMockClient, AmqpServer, HttpServer, Storage, create_amqp_mock

from .amqp_client import AMQPClient


@pytest.fixture()
def amqp_mock_client():
    return AmqpMockClient("localhost", 8080)


@pytest.fixture()
async def amqp_mock_server():
    storage = Storage()
    http_server = HttpServer(storage, host="localhost", port=8080)
    amqp_server = AmqpServer(storage, host="localhost", port=5674)
    mock = create_amqp_mock(http_server, amqp_server)

    yield await mock.start()

    await mock.stop()


@pytest.fixture()
async def amqp_client():
    client = AMQPClient("localhost", 5674)
    await client.connect()

    yield client

    await client.close()
