from amqp_mock import AmqpMockClient, AmqpMockServer, AmqpServer, HttpServer, Storage

from ._test_utils.steps import given, then, when


def test_http_server_repr():
    with given:
        storage = Storage()
        host, port = "localhost", 8080
        http_server = HttpServer(storage, host, port)

    with when:
        result = repr(http_server)

    with then:
        assert result == f"<HttpServer host={host!r} port={port!r}>"


def test_amqp_server_repr():
    with given:
        storage = Storage()
        host, port = "localhost", 5674
        amqp_server = AmqpServer(storage, host, port)

    with when:
        result = repr(amqp_server)

    with then:
        assert result == f"<AmqpServer host={host!r} port={port!r}>"


def test_mock_server_repr():
    with given:
        storage = Storage()
        http_server = HttpServer(storage, "localhost", 8080)
        amqp_server = AmqpServer(storage, "localhost", 5674)
        mock_server = AmqpMockServer(http_server, amqp_server)

    with when:
        result = repr(mock_server)

    with then:
        assert result == f"<AmqpMockServer http_server={http_server} amqp_server={amqp_server}>"


def test_mock_client_repr():
    with given:
        host, port = "localhost", 8080
        mock_client = AmqpMockClient(host, port)

    with when:
        result = repr(mock_client)

    with then:
        assert result == f"<AmqpMockClient host={host!r} port={port!r}>"
