import pytest

from amqp_mock import Message

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_reset_exchanges(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        message = {"id": random_uuid()}
        await amqp_client.publish(to_binary(message), exchange)

    with when:
        result = await mock_client.reset()

    with then:
        assert result is None

        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 0


@pytest.mark.asyncio
async def test_reset_queues(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await mock_client.publish_message(queue, Message("text"))

    with when:
        result = await mock_client.reset()

    with then:
        assert result is None

        await amqp_client.consume(queue)
        await amqp_client.wait(seconds=0.1)
        assert len(amqp_client.get_consumed_messages()) == 0


@pytest.mark.asyncio
async def test_reset_history(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await mock_client.publish_message(queue, Message("text"))
        await amqp_client.consume(queue)

    with when:
        result = await mock_client.reset()

    with then:
        assert result is None

        history = await mock_client.get_queue_message_history(queue)
        assert len(history) == 0
