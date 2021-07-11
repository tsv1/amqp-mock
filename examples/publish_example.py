import asyncio

# aiormq==3
import aiormq

from amqp_mock import create_amqp_mock


async def main():
    async with create_amqp_mock() as mock:
        connection = await aiormq.connect(f"amqp://{mock.amqp_server.host}:{mock.amqp_server.port}")
        channel = await connection.channel()
        await channel.basic_publish(b"[1, 2, 3]", exchange="exchange")

        history = await mock.client.get_exchange_messages("exchange")
        assert history[0].value == [1, 2, 3]

asyncio.run(main())
