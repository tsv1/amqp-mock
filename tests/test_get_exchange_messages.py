import pytest

from amqp_mock import Message

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_get_exchange_message(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        message = {"id": random_uuid()}
        await amqp_client.publish(to_binary(message), exchange)

    with when:
        messages = await mock_client.get_exchange_messages(exchange)

    with then:
        assert len(messages) == 1
        assert isinstance(messages[0], Message)

        assert messages[0].value == message
        assert messages[0].exchange == exchange
        assert messages[0].routing_key == ""
        assert messages[0].properties is not None


@pytest.mark.asyncio
async def test_get_exchange_messages(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        message1, message2 = {"id": random_uuid()}, {"id": random_uuid()}
        await amqp_client.publish(to_binary(message1), exchange)
        await amqp_client.publish(to_binary(message2), exchange)

    with when:
        messages = await mock_client.get_exchange_messages(exchange)

    with then:
        assert len(messages) == 2

        assert messages[0].value == message2
        assert messages[1].value == message1


@pytest.mark.asyncio
async def test_get_exchange_message_specific_exchange(*, mock_server, mock_client, amqp_client):
    with given:
        exchange1, exchange2 = "test_exchange1", "test_exchange2"
        message1, message2 = {"id": random_uuid()}, {"id": random_uuid()}
        await amqp_client.publish(to_binary(message1), exchange1)
        await amqp_client.publish(to_binary(message2), exchange2)

    with when:
        messages = await mock_client.get_exchange_messages(exchange1)

    with then:
        assert len(messages) == 1
        assert messages[0].value == message1


@pytest.mark.asyncio
async def test_get_no_exchange_messages(*, mock_server, mock_client):
    with given:
        exchange = "test_exchange"

    with when:
        messages = await mock_client.get_exchange_messages(exchange)

    with then:
        assert len(messages) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("routing_key", ["", "test_routing_key"])
async def test_get_exchange_message_published_with_routing_key(routing_key, *, mock_server,
                                                               mock_client, amqp_client):
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
