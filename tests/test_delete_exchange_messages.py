import pytest

from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import random_uuid, to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_delete_exchange_message(*, mock_server, mock_client, amqp_client):
    with given:
        exchange = "test_exchange"
        message = {"id": random_uuid()}
        await amqp_client.publish(to_binary(message), exchange)

    with when:
        result = await mock_client.delete_exchange_messages(exchange)

    with then:
        assert result is None
        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 0


@pytest.mark.asyncio
async def test_delete_no_exchange_messages(*, mock_server, mock_client):
    with given:
        exchange = "test_exchange"

    with when:
        result = await mock_client.delete_exchange_messages(exchange)

    with then:
        assert result is None
        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 0
