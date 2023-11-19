import pytest

from amqp_mock import Message

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_publish_single_message(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message = "text"

    with when:
        await mock_client.publish_message(queue, Message(message))

    with then:
        await amqp_client.consume(queue)
        messages = await amqp_client.wait_for(message_count=1)

        assert len(messages) == 1
        assert messages[0].body == to_binary(message)


@pytest.mark.asyncio
async def test_publish_message_with_properties(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message = "text"
        properties = {"content_type": "application/json"}

    with when:
        await mock_client.publish_message(queue, Message(message, properties=properties))

    with then:
        await amqp_client.consume(queue)
        messages = await amqp_client.wait_for(message_count=1)

        assert len(messages) == 1
        assert messages[0].body == to_binary(message)
        assert messages[0].header.properties.content_type == properties['content_type']


@pytest.mark.asyncio
async def test_publish_multiple_messages(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message1, message2 = "text1", "text2"
        await mock_client.publish_message(queue, Message(message1))

    with when:
        await mock_client.publish_message(queue, Message(message2))

    with then:
        await amqp_client.consume(queue)
        messages = await amqp_client.wait_for(message_count=2)

        assert len(messages) == 2
        assert messages[0].body == to_binary(message1)
        assert messages[1].body == to_binary(message2)


@pytest.mark.asyncio
async def test_publish_new_message_while_consuming(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        await amqp_client.consume(queue)
        message = "text"

    with when:
        await mock_client.publish_message(queue, Message(message))

    with then:
        messages = await amqp_client.wait_for(message_count=1)
        assert len(messages) == 1
        assert messages[0].body == to_binary(message)


@pytest.mark.asyncio
async def test_publish_new_messages_while_consuming(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message1, message2 = "text1", "text2"
        await mock_client.publish_message(queue, Message(message1))
        await amqp_client.consume(queue)
        await amqp_client.wait_for(message_count=1)

    with when:
        await mock_client.publish_message(queue, Message(message2))

    with then:
        messages = await amqp_client.wait_for(message_count=2)
        assert len(messages) == 2
        assert messages[0].body == to_binary(message1)
        assert messages[1].body == to_binary(message2)


@pytest.mark.asyncio
async def test_publish_message_specific_queue(*, mock_server, mock_client, amqp_client):
    with given:
        queue1, queue2 = "test_queue1", "test_queue2"
        message1, message2 = "text1", "text2"
        await mock_client.publish_message(queue1, Message(message1))

    with when:
        await mock_client.publish_message(queue2, Message(message2))

    with then:
        await amqp_client.consume(queue1)
        messages = await amqp_client.wait_for(message_count=1)

        assert len(messages) == 1
        assert messages[0].body == to_binary(message1)


@pytest.mark.asyncio
async def test_publish_to_fanout_exchange(*, mock_server, amqp_client):
    with given:
        exchange = "test_exchange"
        queue1, queue2 = "test_queue1", "test_queue2"
        message = {"value": "text"}

        await amqp_client.declare_exchange(exchange, "fanout")

        for queue in [queue1, queue2]:
            await amqp_client.queue_bind(queue, exchange, routing_key=queue)
            await amqp_client.consume(queue)

    with when:
        await amqp_client.publish(to_binary(message), exchange)

    with then:
        messages = await amqp_client.wait_for(message_count=2)
        assert len(messages) == 2
        for message_ in messages:
            assert message_.body == to_binary(message)


@pytest.mark.asyncio
async def test_publish_no_messages(*, mock_server, amqp_client):
    with given:
        queue = "test_queue1"

    with when:
        pass

    with then:
        await amqp_client.consume(queue)
        messages = amqp_client.get_consumed_messages()
        assert len(messages) == 0


@pytest.mark.asyncio
async def test_publish_cancelled_consumer(*, mock_server, mock_client, amqp_client):
    with given:
        queue = "test_queue"
        message1, message2 = "text1", "text2"
        await mock_client.publish_message(queue, Message(message1))
        await amqp_client.consume(queue)
        await amqp_client.wait_for(message_count=1)

        await amqp_client.consume_cancel(queue)

    with when:
        await mock_client.publish_message(queue, Message(message2))

    with then:
        await amqp_client.wait_for(message_count=1, attempts=1)
        messages = amqp_client.get_consumed_messages()
        assert len(messages) == 1
        assert messages[0].body == to_binary(message1)


@pytest.mark.asyncio
@pytest.mark.parametrize("routing_key", ["", "test_routing_key"])
async def test_get_exchange_message_with_routing_key(routing_key, *,
                                                     mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        message = {"id": random_uuid()}
        await amqp_client.publish(to_binary(message), exchange, routing_key=routing_key)

    with when:
        messages = await mock_client.get_exchange_messages(exchange)

    with then:
        assert len(messages) == 1
        assert messages[0].value == message
        assert messages[0].routing_key == routing_key
