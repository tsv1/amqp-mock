# AMQP Mock

[![License](https://img.shields.io/github/license/nikitanovosibirsk/amqp-mock.svg)](https://github.com/nikitanovosibirsk/amqp-mock)
[![Codecov](https://img.shields.io/codecov/c/github/nikitanovosibirsk/amqp-mock/master.svg)](https://codecov.io/gh/nikitanovosibirsk/amqp-mock)
[![PyPI](https://img.shields.io/pypi/v/amqp-mock.svg)](https://pypi.python.org/pypi/amqp-mock/)
[![Python Version](https://img.shields.io/pypi/pyversions/amqp-mock.svg)](https://pypi.python.org/pypi/amqp-mock/)

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
    history = await mock.client.get_exchange_messages("exchange")
    assert history[0].value == [1, 2, 3]
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
