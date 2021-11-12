import pytest
from rtry import timeout, CancelledError

from amqp_mock import Message

from ._test_utils.fixtures import amqp_client, amqp_client_factory, mock_client, mock_server
from ._test_utils.helpers import to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client", "amqp_client_factory",)


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

        h = await mock_client.get_queue_message_history(queue)
        print(h)

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
async def test_publish_to_default_exchange(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = ""
        queue = "test_queue"
        message = {"value": "text"}

    with when:
        await amqp_client.declare_queue(queue)
        await amqp_client.publish(to_binary(message), exchange, routing_key=queue)
        await amqp_client.consume(queue)

    with then:
        messages = await amqp_client.wait_for(message_count=1)
        assert len(messages) == 1
        assert messages[0].body == to_binary(message)
        messages = await mock_client.get_exchange_messages("")
        assert len(messages) == 1
        assert messages[0].value == message


@pytest.mark.asyncio
async def test_publish_to_exchange_with_bound_queue(*, mock_server, amqp_client):
    with given:
        exchange = "test_exchange"
        queue = "test_queue"
        message = b"text"

    with when:
        await amqp_client.queue_bind(queue, exchange)
        await amqp_client.publish(message, exchange)
        await amqp_client.consume(queue)

    with then:
        messages = await amqp_client.wait_for(message_count=1)
        assert len(messages) == 1


@pytest.mark.asyncio
async def test_publish_multiple_channels(*, mock_server, amqp_client_factory):
    with given:
        amqp_client1 = await amqp_client_factory()
        amqp_client2 = await amqp_client_factory(amqp_client1.connection)
        exchange = ""
        queue1, queue2 = "test_queue1", "test_queue2"
        message = {"value": "text"}

    with when:
        for client, queue in [(amqp_client1, queue1), (amqp_client2, queue2)]:
            await client.declare_queue(queue)
            await client.consume(queue)
            # The bug is reproduced if the call to amqp_client2.publish hangs,
            # because the client never confirms delivery of a message with the
            # correct delivery_tag
            try:
                async with timeout(1.0):
                    await client.publish(to_binary(message), exchange, queue)
            except CancelledError:
                pytest.fail('published message never delivered')

    with then:
        for client in [amqp_client1, amqp_client2]:
            messages = await client.wait_for(message_count=1)
            assert len(messages) == 1
            assert messages[0].body == to_binary(message)


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

    with when:
        await amqp_client.consume_cancel(queue)
        await mock_client.publish_message(queue, Message(message2))
        await amqp_client.wait_for(message_count=1, attempts=1)

    with then:
        messages = amqp_client.get_consumed_messages()
        assert len(messages) == 1
        assert messages[0].body == to_binary(message1)
