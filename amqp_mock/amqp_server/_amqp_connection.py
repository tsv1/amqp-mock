import json
import logging
from asyncio import Task, create_task
from asyncio.streams import StreamReader, StreamWriter
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Union

from pamqp import specification as spec
from pamqp.body import ContentBody
from pamqp.exceptions import UnmarshalingException
from pamqp.frame import marshal, unmarshal
from pamqp.header import ContentHeader, ProtocolHeader
from pamqp.heartbeat import Heartbeat

from .._message import Message

__all__ = ("AmqpConnection",)

_logger = logging.getLogger("amqp_mock")

AnyFrame = Union[spec.Frame, ContentHeader, ContentBody]


class AmqpConnection:
    def __init__(self, reader: StreamReader, writer: StreamWriter,
                 on_consume: Callable[[str], AsyncGenerator[Message, None]],
                 server_properties: Dict[str, Any]) -> None:
        self._stream_reader = reader
        self._stream_writer = writer
        self._server_properties = server_properties
        self._reader = create_task(self._reader_task(reader))
        self._consumers: List[Task[Any]] = []
        self._delivered_messages: Dict[int, str] = {}
        self._incoming_message: Union[Message, None] = None
        self._delivery_tag = 0
        self._on_consume = on_consume
        self._on_publish: Optional[Callable[[Message], Awaitable[None]]] = None
        self._on_ack: Optional[Callable[[str], Awaitable[None]]] = None
        self._on_nack: Optional[Callable[[str], Awaitable[None]]] = None
        self._on_close: Optional[Callable[['AmqpConnection'], Awaitable[None]]] = None

    def on_publish(self, callback: Callable[[Message], Awaitable[None]]) -> 'AmqpConnection':
        self._on_publish = callback
        return self

    def on_ack(self, callback: Callable[[str], Awaitable[None]]) -> 'AmqpConnection':
        self._on_ack = callback
        return self

    def on_nack(self, callback: Callable[[str], Awaitable[None]]) -> 'AmqpConnection':
        self._on_nack = callback
        return self

    def on_close(self,
                 callback: Callable[['AmqpConnection'], Awaitable[None]]) -> 'AmqpConnection':
        self._on_close = callback
        return self

    def _get_delivery_tag(self) -> int:
        self._delivery_tag += 1
        return self._delivery_tag

    async def close(self) -> None:
        self._stream_writer.close()
        await self._stream_writer.wait_closed()

        self._stream_reader.feed_eof()
        await self._reader

        if self._on_close:
            await self._on_close(self)

    async def _reader_task(self, reader: StreamReader) -> None:
        buffer = b""
        while not reader.at_eof():
            chunk = await reader.read(1)
            if not chunk:
                break
            buffer += chunk
            try:
                byte_count, channel_id, frame = unmarshal(buffer)
            except UnmarshalingException:
                continue
            else:
                buffer = b""

            _logger.debug(f"<- {frame.name} {channel_id}")
            await self.dispatch_frame(frame, channel_id)

    async def _consumer_task(self, queue_name: str, consumer_tag: str, channel_id: int) -> None:
        _logger.debug(f"* New consumer {consumer_tag}")

        async for message in self._on_consume(queue_name):
            _logger.debug(f"--> Message {message}")

            delivery_tag = self._get_delivery_tag()
            self._delivered_messages[delivery_tag] = message.id

            frame_out = spec.Basic.Deliver(
                consumer_tag=consumer_tag,
                delivery_tag=delivery_tag,
                exchange=message.exchange,
                routing_key=message.routing_key,
            )
            await self._send_frame(channel_id, frame_out)

            encoded = json.dumps(message.value).encode()
            header, body = ContentHeader(body_size=len(encoded)), ContentBody(encoded)
            await self._send_frame(channel_id, header)
            await self._send_frame(channel_id, body)

    async def dispatch_frame(self, frame: AnyFrame, channel_id: int) -> None:
        handlers = {
            Heartbeat.name: self._do_nothing,
            ProtocolHeader.name: self._send_connection_start,
            ContentHeader.name: self._handle_content_header,
            ContentBody.name: self._handle_content_body,
            spec.Connection.StartOk.name: self._send_connection_tune,
            spec.Connection.TuneOk.name: self._do_nothing,
            spec.Connection.Open.name: self._send_connection_open_ok,
            spec.Connection.Close.name: self._send_connection_close_ok,
            spec.Channel.Open.name: self._send_channel_open_ok,
            spec.Channel.Close.name: self._send_channel_close_ok,
            spec.Confirm.Select.name: self._send_confirm_select_ok,
            spec.Queue.Declare.name: self._send_queue_declare_ok,
            spec.Exchange.Declare.name: self._send_exchange_declare_ok,
            spec.Queue.Bind.name: self._send_queue_bind_ok,
            spec.Basic.Qos.name: self._send_basic_qos_ok,
            spec.Basic.Publish.name: self._handle_publish,
            spec.Basic.Consume.name: self._handle_consume,
            spec.Basic.Ack.name: self._handle_ack,
            spec.Basic.Nack.name: self._handle_nack,
        }
        if frame.name in handlers:
            return await handlers[frame.name](channel_id, frame)
        return await self._do_nothing(channel_id, frame)

    async def _send_frame(self, channel_id: int, frame: AnyFrame) -> None:
        _logger.debug(f"-> {frame.name}")
        self._stream_writer.write(bytes(marshal(frame, channel_id)))
        await self._stream_writer.drain()

    async def _do_nothing(self, channel_id: int, frame_in: AnyFrame) -> None:
        _logger.debug("-> DoNothing")

    async def _send_connection_start(self, channel_id: int, frame_in: spec.Frame) -> None:
        frame_out = spec.Connection.Start(
            version_major=0,
            version_minor=9,
            server_properties=self._server_properties,
            mechanisms="PLAIN",
            locales="en_US",
        )
        return await self._send_frame(channel_id, frame_out)

    async def _send_connection_tune(self, channel_id: int,
                                    frame_in: spec.Connection.StartOk) -> None:
        frame_out = spec.Connection.Tune(channel_max=0, frame_max=0, heartbeat=0)
        return await self._send_frame(channel_id, frame_out)

    async def _send_connection_open_ok(self, channel_id: int,
                                       frame_in: spec.Connection.Open) -> None:
        frame_out = spec.Connection.OpenOk()
        await self._send_frame(channel_id, frame_out)

    async def _send_channel_open_ok(self, channel_id: int, frame_in: spec.Channel.Open) -> None:
        frame_out = spec.Channel.OpenOk()
        await self._send_frame(channel_id, frame_out)

    async def _send_queue_declare_ok(self, channel_id: int, frame_in: spec.Queue.Declare) -> None:
        frame_out = spec.Queue.DeclareOk(queue=frame_in.queue, message_count=0, consumer_count=0)
        return await self._send_frame(channel_id, frame_out)

    async def _send_exchange_declare_ok(self, channel_id: int,
                                        frame_in: spec.Exchange.Declare) -> None:
        frame_out = spec.Exchange.DeclareOk()
        return await self._send_frame(channel_id, frame_out)

    async def _send_queue_bind_ok(self, channel_id: int, frame_in: spec.Queue.Bind) -> None:
        frame_out = spec.Queue.BindOk()
        return await self._send_frame(channel_id, frame_out)

    async def _send_confirm_select_ok(self, channel_id: int,
                                      frame_in: spec.Confirm.Select) -> None:
        frame_out = spec.Confirm.SelectOk()
        return await self._send_frame(channel_id, frame_out)

    async def _send_connection_close_ok(self, channel_id: int,
                                        frame_in: spec.Connection.Close) -> None:
        frame_out = spec.Connection.CloseOk()
        await self._send_frame(channel_id, frame_out)

        self._stream_writer.close()
        await self._stream_writer.wait_closed()

    async def _send_channel_close_ok(self, channel_id: int, frame_in: spec.Channel.Close) -> None:
        frame_out = spec.Channel.CloseOk()
        await self._send_frame(channel_id, frame_out)

    async def _send_basic_qos_ok(self, channel_id: int, frame_in: spec.Basic.Qos) -> None:
        frame_out = spec.Basic.QosOk()
        return await self._send_frame(channel_id, frame_out)

    async def _handle_publish(self, channel_id: int, frame_in: spec.Basic.Publish) -> None:
        self._incoming_message = Message(None,
                                         exchange=frame_in.exchange,
                                         routing_key=frame_in.routing_key)
        return await self._do_nothing(channel_id, frame_in)

    async def _handle_content_header(self, channel_id: int, frame_in: ContentHeader) -> None:
        if self._incoming_message:
            self._incoming_message.properties = frame_in.properties.to_dict()
        return await self._do_nothing(channel_id, frame_in)

    async def _handle_content_body(self, channel_id: int, frame_in: ContentBody) -> None:
        if self._incoming_message:
            self._incoming_message.value = frame_in.value
            if self._on_publish:
                await self._on_publish(self._incoming_message)
            self._incoming_message = None

        frame_out = spec.Basic.Ack(delivery_tag=self._get_delivery_tag(), multiple=False)
        await self._send_frame(channel_id, frame_out)

    async def _handle_consume(self, channel_id: int, frame_in: spec.Basic.Consume) -> None:
        frame_out = spec.Basic.ConsumeOk(consumer_tag=frame_in.consumer_tag)
        await self._send_frame(channel_id, frame_out)

        consumer = create_task(
            self._consumer_task(frame_in.queue, frame_in.consumer_tag, channel_id))
        self._consumers.append(consumer)

    async def _handle_ack(self, channel_id: int, frame_in: spec.Basic.Ack) -> None:
        if self._on_ack:
            message_id = self._delivered_messages[frame_in.delivery_tag]
            await self._on_ack(message_id)

    async def _handle_nack(self, channel_id: int, frame_in: spec.Basic.Nack) -> None:
        if self._on_nack:
            message_id = self._delivered_messages[frame_in.delivery_tag]
            await self._on_nack(message_id)
