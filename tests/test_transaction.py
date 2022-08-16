import pytest

from amqp_mock import AmqpMockClient

from ._test_utils.amqp_client import AmqpClient
from ._test_utils.fixtures import amqp_client, mock_client, mock_server
from ._test_utils.helpers import to_binary
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server", "amqp_client",)


@pytest.mark.asyncio
async def test_pending_transaction_messages_are_not_in_storage(
        *, mock_server, mock_client: AmqpMockClient, amqp_client: AmqpClient):
    with given:
        exchange = "test_exchange"
        message1, message2 = "text1", "text2"

    with when:
        await amqp_client.transaction_select()
        await amqp_client.publish(to_binary(message1), exchange)
        await amqp_client.publish(to_binary(message2), exchange)
        
    with then:
        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 0


@pytest.mark.asyncio
async def test_rolled_back_transaction_messages_are_not_in_storage(
        *, mock_server, mock_client: AmqpMockClient, amqp_client: AmqpClient):
    with given:
        exchange = "test_exchange"
        message1, message2 = "text1", "text2"

    with when:
        await amqp_client.transaction_select()
        await amqp_client.publish(to_binary(message1), exchange)
        await amqp_client.publish(to_binary(message2), exchange)
        await amqp_client.transaction_rollback()
        
    with then:
        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 0



@pytest.mark.asyncio
async def test_committed_transaction_messages_are_in_storage(
        *, mock_server, mock_client: AmqpMockClient, amqp_client: AmqpClient):
    with given:
        exchange = "test_exchange"
        message1, message2 = "text1", "text2"

    with when:
        await amqp_client.transaction_select()
        await amqp_client.publish(to_binary(message1), exchange)
        await amqp_client.publish(to_binary(message2), exchange)
        await amqp_client.transaction_commit()
        
    with then:
        messages = await mock_client.get_exchange_messages(exchange)
        assert len(messages) == 2
        assert messages[0].value == message2
        assert messages[1].value == message1
