import pytest

from ._test_utils.fixtures import amqp_client, amqp_mock_client, amqp_mock_server
from ._test_utils.steps import given, then, when

__all__ = ("amqp_mock_client", "amqp_mock_server", "amqp_client")


@pytest.mark.asyncio
async def test_declare_queue(*, amqp_mock_server, amqp_mock_client, amqp_client):
    with given():
        queue = "test_queue"

    with when():
        result = await amqp_client.declare_queue(queue)

    with then():
        assert result is None


@pytest.mark.asyncio
async def test_declare_exchange(*, amqp_mock_server, amqp_mock_client, amqp_client):
    with given():
        exchange = "test_exchange"

    with when():
        result = await amqp_client.declare_exchange(exchange)

    with then():
        assert result is None


@pytest.mark.asyncio
async def test_basic_qos(*, amqp_mock_server, amqp_mock_client, amqp_client):
    with given():
        prefetch_count = 1

    with when():
        result = await amqp_client.basic_qos(prefetch_count=prefetch_count)

    with then():
        assert result is None


@pytest.mark.asyncio
async def test_queue_bind(*, amqp_mock_server, amqp_mock_client, amqp_client):
    with given():
        queue = "test_queue"
        exchange = "test_exchange"

    with when():
        result = await amqp_client.queue_bind(queue, exchange)

    with then():
        assert result is None
