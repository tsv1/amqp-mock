import pytest

from amqp_mock import Message, MessageStatus, QueuedMessage

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary
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


@pytest.mark.asyncio
async def test_get_queue_message_history(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message1, message2 = "text1", "text2"
        await mock_client.publish_message(queue, Message(message1))
        await mock_client.publish_message(queue, Message(message2))
        await amqp_client.consume(queue)
        await amqp_client.wait_for(message_count=2)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 2
        assert history[0].message.value == message2
        assert history[1].message.value == message1


@pytest.mark.asyncio
async def test_get_queue_message_history_with_declared_queue_no_routing_key(*,
                                                                            mock_server,
                                                                            mock_client,
                                                                            amqp_client):
    with given:
        exchange = ""
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)
        await amqp_client.publish(to_binary(message), exchange, routing_key="")

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 0


@pytest.mark.asyncio
async def test_get_queue_message_history_with_declared_queue_default_exchange(*,
                                                                              mock_server,
                                                                              mock_client,
                                                                              amqp_client):
    with given:
        exchange = ""
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)
        await amqp_client.publish(to_binary(message), exchange, routing_key=queue)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert history[0].message.value == message
        assert history[0].message.routing_key == queue
        assert history[0].status == MessageStatus.INIT


@pytest.mark.asyncio
async def test_get_queue_message_history_with_declared_queue_custom_exchange(*,
                                                                             mock_server,
                                                                             mock_client,
                                                                             amqp_client):
    with given:
        exchange = "test_exchange"
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)
        await amqp_client.publish(to_binary(message), exchange, routing_key=queue)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 0


@pytest.mark.asyncio
async def test_get_queue_message_history_with_bound_queue(*, mock_server,
                                                          mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.queue_bind(queue, exchange)
        await amqp_client.publish(to_binary(message), exchange)

    with when:
        history = await mock_client.get_queue_message_history(queue)

    with then:
        assert len(history) == 1
        assert history[0].message.value == message
        assert history[0].message.exchange == exchange
        assert history[0].queue == queue
        assert history[0].status == MessageStatus.INIT
