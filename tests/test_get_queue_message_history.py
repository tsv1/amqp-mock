import pytest

from amqp_mock import Message, MessageStatus, QueuedMessage

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_get_queue_message_history_init(*, mock_server, mock_client):
    with given:
        queue = "test_queue"
        message = "text"
        await mock_client.publish_message(queue, Message(message))

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert isinstance(history[0], QueuedMessage)
        assert history[0].status == MessageStatus.INIT
        assert history[0].queue == queue
        assert history[0].message.value == message


@pytest.mark.asyncio
async def test_get_queue_message_history_consuming(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await mock_client.publish_message(queue, Message("text"))
        await amqp_client.consume(queue)
        await amqp_client.wait_for(message_count=1)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert isinstance(history[0], QueuedMessage)
        assert history[0].status == MessageStatus.CONSUMING


@pytest.mark.asyncio
async def test_get_queue_message_history_acked(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await mock_client.publish_message(queue, Message("text"))
        await amqp_client.consume_ack(queue)
        await amqp_client.wait_for(message_count=1)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert isinstance(history[0], QueuedMessage)
        assert history[0].status == MessageStatus.ACKED


@pytest.mark.asyncio
async def test_get_queue_message_history_nacked(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await mock_client.publish_message(queue, Message("text"))
        await amqp_client.consume_nack(queue)
        await amqp_client.wait_for(message_count=1)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert isinstance(history[0], QueuedMessage)
        assert history[0].status == MessageStatus.NACKED
