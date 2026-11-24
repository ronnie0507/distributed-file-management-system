import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties


_LOGGER = logging.getLogger(__name__)
_JOB_EXCHANGE = "dfms.jobs"
_JOB_QUEUE = "dfms.jobs"
_JOB_ROUTING_KEY = "background_job"
_DEAD_EXCHANGE = "dfms.jobs.dlx"
_DEAD_QUEUE = "dfms.jobs.dead"
_DEAD_ROUTING_KEY = "dead"


class InvalidJobMessageError(ValueError):
    pass


class PermanentJobError(Exception):
    """A message must be dead-lettered because retrying it cannot help."""


@dataclass(frozen=True)
class JobMessage:
    event_id: UUID
    job_id: UUID
    event_key: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class RabbitMQConfig:
    host: str
    port: int
    username: str
    password: str
    virtual_host: str
    connection_timeout_seconds: float = 5

    @classmethod
    def from_env(cls) -> "RabbitMQConfig":
        password = os.getenv("RABBITMQ_PASSWORD", "")
        if not password:
            raise ValueError("RABBITMQ_PASSWORD must be configured")
        return cls(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            username=os.getenv("RABBITMQ_USER", "dfms-worker"),
            password=password,
            virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
            connection_timeout_seconds=float(
                os.getenv("RABBITMQ_CONNECTION_TIMEOUT_SECONDS", "5")
            ),
        )

    def connection_parameters(self) -> pika.ConnectionParameters:
        credentials = pika.PlainCredentials(self.username, self.password)
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            socket_timeout=self.connection_timeout_seconds,
            connection_attempts=1,
            blocked_connection_timeout=self.connection_timeout_seconds,
            heartbeat=30,
        )


def decode_job_message(body: bytes) -> JobMessage:
    try:
        message = json.loads(body)
        event_id = UUID(message["event_id"])
        job_id = UUID(message["payload"]["job_id"])
        event_key = message["event_key"]
        payload = message["payload"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise InvalidJobMessageError("Malformed background-job message") from exc
    if not isinstance(event_key, str) or not event_key:
        raise InvalidJobMessageError("Message event_key must be a non-empty string")
    if not isinstance(payload, dict):
        raise InvalidJobMessageError("Message payload must be a JSON object")
    return JobMessage(
        event_id=event_id,
        job_id=job_id,
        event_key=event_key,
        payload=payload,
    )


class RabbitMQTransport:
    def __init__(
        self,
        config: RabbitMQConfig,
        *,
        connection_factory: Callable[
            [pika.ConnectionParameters], pika.BlockingConnection
        ] = pika.BlockingConnection,
        prefetch_count: int = 1,
    ) -> None:
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be positive")
        self._config = config
        self._connection_factory = connection_factory
        self._prefetch_count = prefetch_count

    def publish(
        self,
        *,
        event_id: UUID,
        event_key: str,
        payload: dict[str, Any],
    ) -> None:
        connection = self._connection_factory(self._config.connection_parameters())
        try:
            channel = connection.channel()
            self._declare_topology(channel)
            channel.confirm_delivery()
            channel.basic_publish(
                exchange=_JOB_EXCHANGE,
                routing_key=_JOB_ROUTING_KEY,
                body=json.dumps(
                    {
                        "event_id": str(event_id),
                        "event_key": event_key,
                        "payload": payload,
                    },
                    separators=(",", ":"),
                ).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=str(event_id),
                    headers={"event_key": event_key},
                ),
                mandatory=True,
            )
        finally:
            if connection.is_open:
                connection.close()

    def consume_forever(
        self,
        handler: Callable[[JobMessage], None],
    ) -> None:
        connection = self._connection_factory(self._config.connection_parameters())
        channel = connection.channel()
        self._declare_topology(channel)
        channel.basic_qos(prefetch_count=self._prefetch_count)

        def on_message(
            active_channel: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ) -> None:
            try:
                message = decode_job_message(body)
                if (
                    properties.message_id is not None
                    and properties.message_id != str(message.event_id)
                ):
                    raise InvalidJobMessageError(
                        "Message event_id does not match its AMQP message_id"
                    )
                handler(message)
            except (InvalidJobMessageError, PermanentJobError) as exc:
                _LOGGER.error("Dead-lettering job message: %s", exc)
                active_channel.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False,
                )
            except Exception:
                _LOGGER.exception("Background job failed before safe completion")
                active_channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True,
                )
            else:
                active_channel.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(
            queue=_JOB_QUEUE,
            on_message_callback=on_message,
            auto_ack=False,
        )
        try:
            channel.start_consuming()
        finally:
            if connection.is_open:
                connection.close()

    @staticmethod
    def _declare_topology(channel: BlockingChannel) -> None:
        channel.exchange_declare(
            exchange=_JOB_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.exchange_declare(
            exchange=_DEAD_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue=_JOB_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": _DEAD_EXCHANGE,
                "x-dead-letter-routing-key": _DEAD_ROUTING_KEY,
            },
        )
        channel.queue_bind(
            queue=_JOB_QUEUE,
            exchange=_JOB_EXCHANGE,
            routing_key=_JOB_ROUTING_KEY,
        )
        channel.queue_declare(queue=_DEAD_QUEUE, durable=True)
        channel.queue_bind(
            queue=_DEAD_QUEUE,
            exchange=_DEAD_EXCHANGE,
            routing_key=_DEAD_ROUTING_KEY,
        )
