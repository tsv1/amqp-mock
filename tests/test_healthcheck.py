import pytest
from aiohttp import ClientError
from pytest import raises

from ._test_utils.fixtures import mock_client, mock_server
from ._test_utils.steps import given, then, when

__all__ = ("mock_client", "mock_server",)


@pytest.mark.asyncio
async def test_healthcheck_ok(*, mock_server, mock_client):
    with given:
        pass

    with when:
        result = await mock_client.healthcheck()

    with then:
        assert result is None


@pytest.mark.asyncio
async def test_healthcheck_not_ok(*, mock_client):
    with given:
        pass

    with when(), raises(Exception) as exception:
        await mock_client.healthcheck()

    with then:
        assert isinstance(exception.value, ClientError)
