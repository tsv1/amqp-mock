import pytest
from d42 import schema

from amqp_mock import MessageStatus

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary, to_dict
from ._test_utils.schemas import MessageSchema, QueuedMessageSchema
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_routing(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = ""
        queue = "test_queue"
        routing_key = ""
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)

    with when:
        await amqp_client.publish(to_binary(message), exchange, routing_key=routing_key)

    with then:
        history = await mock_client.get_queue_message_history(queue)
        assert history == schema.list.len(0)

        messages = await mock_client.get_exchange_messages(exchange)
        assert to_dict(messages) == schema.list([
            MessageSchema % {
                "value": message,
                "exchange": exchange,
                "routing_key": routing_key,
            }
        ])


@pytest.mark.asyncio
async def test_routing_with_default_exchange(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = ""
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)

    with when:
        await amqp_client.publish(to_binary(message), exchange, routing_key=queue)

    with then:
        history = await mock_client.get_queue_message_history(queue)
        assert to_dict(history) == schema.list([
            QueuedMessageSchema % {
                "message": {
                    "value": message,
                    "exchange": exchange,
                    "routing_key": queue,
                },
                "queue": queue,
                "status": MessageStatus.INIT,
            }
        ])


@pytest.mark.asyncio
async def test_routing_with_custom_exchange(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.declare_queue(queue)

    with when:
        await amqp_client.publish(to_binary(message), exchange, routing_key=queue)

    with then:
        history = await mock_client.get_queue_message_history(queue)
        assert history == schema.list.len(0)


@pytest.mark.asyncio
async def test_routing_with_bound_queue(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        queue = "test_queue"
        message = {"id": random_uuid()}

        await amqp_client.queue_bind(queue, exchange)

    with when:
        await amqp_client.publish(to_binary(message), exchange)

    with then:
        history = await mock_client.get_queue_message_history(queue)
        assert to_dict(history) == schema.list([
            QueuedMessageSchema % {
                "message": {
                    "value": message,
                    "exchange": exchange,
                    "routing_key": "",
                },
                "queue": queue,
                "status": MessageStatus.INIT,
            }
        ])


@pytest.mark.asyncio
async def test_routing_fanout_exchange(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        queue1, queue2 = "test_queue1", "test_queue2"
        message = {"id": random_uuid()}
        routing_key = random_uuid()

        await amqp_client.declare_exchange(exchange, "fanout")
        for queue in [queue1, queue2]:
            await amqp_client.queue_bind(queue, exchange, routing_key=queue)

    with when:
        await amqp_client.publish(to_binary(message), exchange, routing_key=routing_key)

    with then:
        history_queue1 = await mock_client.get_queue_message_history(queue1)
        assert to_dict(history_queue1) == schema.list([
            QueuedMessageSchema % {
                "message": {
                    "value": message,
                    "exchange": exchange,
                    "routing_key": routing_key,
                },
                "queue": queue1,
                "status": MessageStatus.INIT,
            }
        ])

        history_queue2 = await mock_client.get_queue_message_history(queue2)
        assert to_dict(history_queue2) == schema.list([
            QueuedMessageSchema % {
                "message": {
                    "value": message,
                    "exchange": exchange,
                    "routing_key": routing_key,
                },
                "queue": queue2,
                "status": MessageStatus.INIT,
            }
        ])
