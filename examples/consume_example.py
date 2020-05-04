import asyncio

import aiormq

from amqp_mock import create_amqp_mock, Message, MessageStatus


async def main():
    async with create_amqp_mock() as mock:
        await mock.client.publish_message("queue", Message([1, 2, 3]))

        connection = await aiormq.connect(f"amqp://{mock.amqp_server.host}:{mock.amqp_server.port}")
        channel = await connection.channel()

        async def on_message(message):
            await channel.basic_ack(message.delivery.delivery_tag)

        await channel.basic_consume("queue", on_message)

        history = await mock.client.get_queue_message_history("queue")
        assert history[0].message.value == [1, 2, 3]
        assert history[0].status == MessageStatus.ACKED


asyncio.run(main())
