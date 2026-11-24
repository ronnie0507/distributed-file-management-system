import logging
import os
import time
from typing import Protocol
from uuid import UUID

from pika.exceptions import AMQPError
from sqlalchemy.orm import Session, sessionmaker

if __package__ == "backend.workers":
    from ..database.session import get_session_factory
    from ..engines.outbox import OutboxClaim, OutboxEngine, outbox_engine
    from ..messaging.rabbitmq import RabbitMQConfig, RabbitMQTransport
else:
    from database.session import get_session_factory
    from engines.outbox import OutboxClaim, OutboxEngine, outbox_engine
    from messaging.rabbitmq import RabbitMQConfig, RabbitMQTransport


_LOGGER = logging.getLogger(__name__)
_POLL_INTERVAL_SECONDS = float(os.getenv("OUTBOX_POLL_SECONDS", "1"))


class JobPublisher(Protocol):
    def publish(
        self,
        *,
        event_id: UUID,
        event_key: str,
        payload: dict,
    ) -> None: ...


class OutboxDispatcher:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        publisher: JobPublisher,
        *,
        engine: OutboxEngine = outbox_engine,
        poll_interval_seconds: float = _POLL_INTERVAL_SECONDS,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        self._session_factory = session_factory
        self._publisher = publisher
        self._engine = engine
        self._poll_interval_seconds = poll_interval_seconds

    def run_once(self) -> bool:
        with self._session_factory() as db:
            claim = self._engine.claim_next(db)
            db.commit()
        if claim is None:
            return False

        try:
            self._publisher.publish(
                event_id=claim.event_id,
                event_key=claim.event_key,
                payload=claim.payload,
            )
        except (AMQPError, OSError, TimeoutError) as exc:
            self._release_for_retry(claim, exc)
            return True
        except Exception as exc:
            self._release_for_retry(claim, exc)
            raise

        with self._session_factory() as db:
            published = self._engine.mark_published(db, claim=claim)
            db.commit()
        if not published:
            _LOGGER.warning(
                "Outbox publish confirmation arrived after event lease expired: %s",
                claim.event_id,
            )
        return True

    def run_forever(self) -> None:
        while True:
            try:
                self.run_once()
            except Exception:
                _LOGGER.exception("Outbox dispatcher failed unexpectedly")
            time.sleep(self._poll_interval_seconds)

    def _release_for_retry(self, claim: OutboxClaim, error: Exception) -> None:
        with self._session_factory() as db:
            released = self._engine.release_for_retry(
                db,
                claim=claim,
                error=str(error),
            )
            db.commit()
        if released:
            _LOGGER.warning(
                "Could not publish outbox event %s; it will be retried: %s",
                claim.event_id,
                error,
            )
        else:
            _LOGGER.warning(
                "Outbox event %s was claimed by another publisher during failure",
                claim.event_id,
            )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    dispatcher = OutboxDispatcher(
        get_session_factory(),
        RabbitMQTransport(RabbitMQConfig.from_env()),
    )
    dispatcher.run_forever()


if __name__ == "__main__":
    main()
