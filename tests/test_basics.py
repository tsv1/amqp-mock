import pytest

from ._test_utils.fixtures import amqp_client, mock_server
from ._test_utils.steps import given, then, when

__all__ = ("mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_declare_queue(*, mock_server, amqp_client):
    with given:
        queue = "test_queue"

    with when:
        result = await amqp_client.declare_queue(queue)

    with then:
        assert result is None


@pytest.mark.asyncio
async def test_declare_exchange(*, mock_server, amqp_client):
    with given:
        exchange = "test_exchange"

    with when:
        result = await amqp_client.declare_exchange(exchange)

    with then:
        assert result is None


@pytest.mark.asyncio
async def test_basic_qos(*, mock_server, amqp_client):
    with given:
        prefetch_count = 1

    with when:
        result = await amqp_client.basic_qos(prefetch_count=prefetch_count)

    with then:
        assert result is None


@pytest.mark.asyncio
async def test_queue_bind(*, mock_server, amqp_client):
    with given:
        queue = "test_queue"
        exchange = "test_exchange"

    with when:
        result = await amqp_client.queue_bind(queue, exchange)

    with then:
        assert result is None
