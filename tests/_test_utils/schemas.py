from d42 import schema

from amqp_mock import MessageStatus

MessageSchema = schema.dict({
    "id": schema.str.len(1, ...),
    "value": schema.any,
    "exchange": schema.str,
    "routing_key": schema.str,
    "properties": schema.dict,
})

QueuedMessageSchema = schema.dict({
    "message": MessageSchema,
    "queue": schema.str,
    "status": schema.any(
        schema.str(MessageStatus.INIT),
        schema.str(MessageStatus.CONSUMING),
        schema.str(MessageStatus.ACKED),
        schema.str(MessageStatus.NACKED),
    ),
})
