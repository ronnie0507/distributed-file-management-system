import json
from types import SimpleNamespace
from uuid import uuid4

import pika
import pytest

from backend.messaging.rabbitmq import (
    InvalidJobMessageError,
    JobMessage,
    PermanentJobError,
    RabbitMQConfig,
    RabbitMQTransport,
    decode_job_message,
)


class FakeChannel:
    def __init__(self) -> None:
        self.exchanges: list[dict] = []
        self.queues: list[dict] = []
        self.bindings: list[dict] = []
        self.published: list[dict] = []
        self.confirmed = False
        self.callback = None
        self.acknowledgements: list[int] = []
        self.rejections: list[tuple[int, bool]] = []
        self.negative_acknowledgements: list[tuple[int, bool]] = []
        self.prefetch_count = 0

    def exchange_declare(self, **kwargs) -> None:
        self.exchanges.append(kwargs)

    def queue_declare(self, **kwargs) -> None:
        self.queues.append(kwargs)

    def queue_bind(self, **kwargs) -> None:
        self.bindings.append(kwargs)

    def confirm_delivery(self) -> None:
        self.confirmed = True

    def basic_publish(self, **kwargs) -> None:
        self.published.append(kwargs)

    def basic_qos(self, *, prefetch_count: int) -> None:
        self.prefetch_count = prefetch_count

    def basic_consume(self, **kwargs) -> None:
        self.callback = kwargs["on_message_callback"]
        assert kwargs["auto_ack"] is False

    def start_consuming(self) -> None:
        assert self.callback is not None
        self.callback(
            self,
            SimpleNamespace(delivery_tag=7),
            pika.BasicProperties(message_id=str(EVENT_ID)),
            MESSAGE_BODY,
        )

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acknowledgements.append(delivery_tag)

    def basic_reject(self, *, delivery_tag: int, requeue: bool) -> None:
        self.rejections.append((delivery_tag, requeue))

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.negative_acknowledgements.append((delivery_tag, requeue))


class FakeConnection:
    def __init__(self, _parameters) -> None:
        self.channel_instance = FakeChannel()
        self.is_open = True

    def channel(self) -> FakeChannel:
        return self.channel_instance

    def close(self) -> None:
        self.is_open = False


EVENT_ID = uuid4()
JOB_ID = uuid4()
MESSAGE_BODY = json.dumps(
    {
        "event_id": str(EVENT_ID),
        "event_key": "test-event",
        "payload": {"job_id": str(JOB_ID), "job_type": "REPLICATE"},
    }
).encode()


def _transport() -> tuple[RabbitMQTransport, list[FakeConnection]]:
    connections: list[FakeConnection] = []

    def create_connection(parameters):
        connection = FakeConnection(parameters)
        connections.append(connection)
        return connection

    config = RabbitMQConfig(
        host="rabbitmq",
        port=5672,
        username="worker",
        password="test-password",
        virtual_host="/",
    )
    return (
        RabbitMQTransport(config, connection_factory=create_connection),
        connections,
    )


def test_job_message_decoder_validates_durable_identity() -> None:
    message = decode_job_message(MESSAGE_BODY)

    assert message == JobMessage(
        event_id=EVENT_ID,
        job_id=JOB_ID,
        event_key="test-event",
        payload={"job_id": str(JOB_ID), "job_type": "REPLICATE"},
    )
    with pytest.raises(InvalidJobMessageError):
        decode_job_message(b"{not-json")


def test_publisher_uses_confirmed_persistent_routed_message() -> None:
    transport, connections = _transport()

    transport.publish(
        event_id=EVENT_ID,
        event_key="test-event",
        payload={"job_id": str(JOB_ID)},
    )

    channel = connections[0].channel_instance
    assert channel.confirmed
    assert channel.published[0]["mandatory"] is True
    assert channel.published[0]["properties"].delivery_mode == 2
    assert channel.published[0]["properties"].message_id == str(EVENT_ID)
    assert connections[0].is_open is False


def test_consumer_acknowledges_only_after_handler_returns() -> None:
    transport, connections = _transport()
    handled: list[JobMessage] = []

    transport.consume_forever(handled.append)

    channel = connections[0].channel_instance
    assert handled[0].job_id == JOB_ID
    assert channel.prefetch_count == 1
    assert channel.acknowledgements == [7]
    assert channel.rejections == []


def test_consumer_dead_letters_permanent_failures_and_requeues_transient_ones() -> None:
    transport, connections = _transport()
    transport.consume_forever(
        lambda _message: (_ for _ in ()).throw(PermanentJobError("dead"))
    )
    channel = connections[0].channel_instance
    assert channel.rejections == [(7, False)]
    assert channel.acknowledgements == []

    transport, connections = _transport()
    transport.consume_forever(
        lambda _message: (_ for _ in ()).throw(RuntimeError("database down"))
    )
    channel = connections[0].channel_instance
    assert channel.negative_acknowledgements == [(7, True)]
    assert channel.acknowledgements == []


def test_topology_uses_durable_queue_and_dead_letter_exchange() -> None:
    transport, connections = _transport()
    transport.publish(
        event_id=EVENT_ID,
        event_key="test-event",
        payload={"job_id": str(JOB_ID)},
    )

    channel = connections[0].channel_instance
    job_queue = next(queue for queue in channel.queues if queue["queue"] == "dfms.jobs")
    assert job_queue["durable"] is True
    assert job_queue["arguments"]["x-dead-letter-exchange"] == "dfms.jobs.dlx"
    assert any(
        queue["queue"] == "dfms.jobs.dead" and queue["durable"]
        for queue in channel.queues
    )
