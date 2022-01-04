# AMQP Mock

[![Codecov](https://img.shields.io/codecov/c/github/nikitanovosibirsk/amqp-mock/master.svg?style=flat-square)](https://codecov.io/gh/nikitanovosibirsk/amqp-mock)
[![PyPI](https://img.shields.io/pypi/v/amqp-mock.svg?style=flat-square)](https://pypi.python.org/pypi/amqp-mock)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/amqp-mock?style=flat-square)](https://pypi.python.org/pypi/amqp-mock)
[![Python Version](https://img.shields.io/pypi/pyversions/amqp-mock.svg?style=flat-square)](https://pypi.python.org/pypi/amqp-mock)

* [Installation](#installation)
* [Overview](#overview)
  * [Test Publishing](#test-publishing)
  * [Test Consuming](#test-consuming)
* [Mock Server](#mock-server)
  * [Start server](#start-server)
  * [Publish message](#publish-message)
  * [Get queue message history](#get-queue-message-history)
  * [Get exchange messages](#get-exchange-messages)
  * [Delete exchange messages](#delete-exchange-messages)
  * [Reset](#reset)

## Installation

```sh
pip3 install amqp-mock
```

## Overview

### Test Publishing

```python
from amqp_mock import create_amqp_mock

# 1. Start AMQP mock server
async with create_amqp_mock() as mock:
    # 2. Publish message via "system under test"
    publish_message([1, 2, 3], "exchange")

    # 3. Test message has been published
    messages = await mock.client.get_exchange_messages("exchange")
    assert messages[0].value == [1, 2, 3]
```

Full code available here: [`./examples/publish_example.py`](https://github.com/nikitanovosibirsk/amqp-mock/blob/master/examples/publish_example.py)

### Test Consuming

```python
from amqp_mock import create_amqp_mock, Message, MessageStatus

# 1. Start AMQP mock server
async with create_amqp_mock() as mock:
    # 2. Mock next message
    await mock.client.publish_message("queue", Message([1, 2, 3]))

    # 3. Consume message via "system under test"
    consume_message("queue")

    # 4. Test message has been consumed
    history = await mock.client.get_queue_message_history("queue")
    assert history[0].status == MessageStatus.ACKED
```

Full code available here: [`./examples/consume_example.py`](https://github.com/nikitanovosibirsk/amqp-mock/blob/master/examples/consume_example.py)

## Mock Server

### Start server

```python
import asyncio

from amqp_mock import AmqpServer, HttpServer, Storage, create_amqp_mock


async def run() -> None:
    storage = Storage()
    http_server = HttpServer(storage, port=8080)
    amqp_server = AmqpServer(storage, port=5672)
    async with create_amqp_mock(http_server, amqp_server):
        await asyncio.Future()

asyncio.run(run())
```

or via docker

```shell
docker run -p 8080:80 -p 5672:5672 nikitanovosibirsk/amqp-mock
```

### Publish message

`POST /queues/{queue}/messages`

```js
{
    "id": "9e342ac1-eef6-40b1-9eaf-053ee7887968",
    "value": [1, 2, 3],
    "exchange": "",
    "routing_key": "",
    "properties": null
}
```

<details><summary>HTTP</summary>
<p>

```sh
$ http POST localhost/queues/test_queue/messages \
    value:='[1, 2, 3]' \
    exchange=test_exchange

HTTP/1.1 200 OK
Content-Length: 0
Content-Type: application/json
```

</p>
</details>

<details><summary>Python</summary>
<p>

```python
from amqp_mock import AmqpMockClient, Message

mock_client = AmqpMockClient()
message = Message([1, 2, 3], exchange="test_exchange")
await mock_client.publish_message("test_queue", message)
```

</p>
</details>

### Get queue message history

`GET /queues/{queue}/messages/history`

<details><summary>HTTP</summary>
<p>

```sh
$ http GET localhost/queues/test_queue/messages/history

HTTP/1.1 200 OK
Content-Length: 190
Content-Type: application/json; charset=utf-8

[
    {
        "message": {
            "exchange": "test_exchange",
            "id": "94459a41-9119-479a-98c9-80bc9dabb719",
            "properties": null,
            "routing_key": "",
            "value": [1, 2, 3]
        },
        "queue": "test_queue",
        "status": "ACKED"
    }
]
```

</p>
</details>

<details><summary>Python</summary>
<p>

```python
from amqp_mock import AmqpMockClient

mock_client = AmqpMockClient()
await mock_client.get_queue_message_history("test_queue")
# [
#   <QueuedMessage message=<Message value=[1, 2, 3], exchange='test_exchange', routing_key=''>,
#                  queue='test_queue',
#                  status=MessageStatus.ACKED>
# ]
```

</p>
</details>

### Get exchange messages

`GET /exchanges/{exchange}/messages`

<details><summary>HTTP</summary>
<p>

```sh
$ http GET localhost/exchanges/test_exchange/messages

HTTP/1.1 200 OK
Content-Length: 423
Content-Type: application/json; charset=utf-8

[
    {
        "exchange": "test_exchange",
        "id": "63fd1646-bdc1-4baa-9780-e337a9ab109c",
        "properties": {
            "app_id": "",
            "cluster_id": "",
            "content_encoding": "",
            "content_type": "",
            "correlation_id": "",
            "delivery_mode": 1,
            "expiration": "",
            "headers": null,
            "message_id": "5ec9024c74eca2e419fd7e29f7be846c",
            "message_type": "",
            "priority": null,
            "reply_to": "",
            "timestamp": null,
            "user_id": ""
        },
        "routing_key": "",
        "value": [1, 2, 3]
    }
]
```

</p>
</details>

<details><summary>Python</summary>
<p>

```python
from amqp_mock import AmqpMockClient

mock_client = AmqpMockClient()
messages = await mock_client.get_exchange_messages("test_exchange")
# [
#   <Message value=[1, 2, 3], exchange='test_exchange', routing_key=''>
# ]
```

</p>
</details>

### Delete exchange messages

`DELETE /exchanges/{exchange}/messages`

<details><summary>HTTP</summary>
<p>

```sh
$ http DELETE localhost/exchanges/test_exchange/messages

HTTP/1.1 200 OK
Content-Length: 0
Content-Type: application/json
```

</p>
</details>

<details><summary>Python</summary>
<p>

```python
from amqp_mock import AmqpMockClient

mock_client = AmqpMockClient()
await mock_client.delete_exchange_messages("test_exchange")
```

</p>
</details>

### Reset

`DELETE /`

<details><summary>HTTP</summary>
<p>

```sh
$ http DELETE localhost/

HTTP/1.1 200 OK
Content-Length: 0
Content-Type: application/json
```

</p>
</details>

<details><summary>Python</summary>
<p>

```python
from amqp_mock import AmqpMockClient

mock_client = AmqpMockClient()
await mock_client.reset()
```

</p>
</details>
