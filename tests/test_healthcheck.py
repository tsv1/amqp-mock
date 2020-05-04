import pytest
from aiohttp import ClientError
from pytest import raises

from ._test_utils.fixtures import amqp_mock_client, amqp_mock_server
from ._test_utils.steps import given, then, when

__all__ = ("amqp_mock_client", "amqp_mock_server")


@pytest.mark.asyncio
async def test_healthcheck_ok(*, amqp_mock_server, amqp_mock_client):
    with given():
        pass

    with when():
        result = await amqp_mock_client.healthcheck()

    with then():
        assert result is None


@pytest.mark.asyncio
async def test_healthcheck_not_ok(*, amqp_mock_client):
    with given():
        pass

    with when(), raises(Exception) as exception:
        await amqp_mock_client.healthcheck()

    with then():
        assert isinstance(exception.value, ClientError)
