import asyncio
import logging
import signal
from os import environ

from amqp_mock import AmqpServer, HttpServer, Storage, create_amqp_mock


async def run() -> None:
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, future.set_result, None)

    storage = Storage()
    http_server = HttpServer(storage, port=80)
    amqp_server = AmqpServer(storage, port=5672)
    async with create_amqp_mock(http_server, amqp_server):
        await future

if __name__ == "__main__":
    LOG_LEVEL = environ.get("LOG_LEVEL", "ERROR").upper()
    logging.basicConfig(level=LOG_LEVEL)
    asyncio.run(run())
