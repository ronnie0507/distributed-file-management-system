from collections.abc import Generator
from datetime import datetime, timezone
from io import BytesIO
from typing import BinaryIO
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database.base import Base
from backend.database.models import (
    AuditLog,
    FileReplica,
    FileVersion,
    OutboxEvent,
    RefreshSession,
    ReplicationJob,
    StorageNode,
    User,
)
from backend.database.session import get_db
from backend.api.dependencies import get_storage_registry
from backend.engines.storage import ConfiguredStorageNode, StorageRegistry
from backend.database.models import StorageNode
from backend.engines.metadata import metadata_engine
import backend.main as main_module
from backend.main import app


class AllowAllRateLimiter:
    async def allow(self, _key: str, *, limit: int, window_seconds: int) -> bool:
        return True

    async def aclose(self) -> None:
        return None


class MemoryObjectReader(BytesIO):
    def read(self, size: int | None = -1, /) -> bytes:
        return super().read(-1 if size is None else size)

    def release_conn(self) -> None:
        return None


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        data: BinaryIO,
        size_bytes: int,
        content_type: str,
        checksum: str,
    ) -> None:
        content = data.read()
        assert len(content) == size_bytes
        self.objects[f"{bucket}/{object_key}"] = content
        assert content_type
        assert checksum

    def get_object(self, *, bucket: str, object_key: str) -> MemoryObjectReader:
        return MemoryObjectReader(self.objects[f"{bucket}/{object_key}"])

    def remove_object(self, *, bucket: str, object_key: str) -> None:
        self.objects.pop(f"{bucket}/{object_key}", None)

    def health_check(self) -> None:
        return None

    def used_bytes(self, bucket: str) -> int:
        prefix = f"{bucket}/"
        return sum(
            len(content)
            for key, content in self.objects.items()
            if key.startswith(prefix)
        )


@pytest.fixture
def api_client(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[
    tuple[TestClient, dict[str, str], sessionmaker, dict[str, MemoryObjectStore]],
    None,
    None,
]:
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-that-is-at-least-32-chars")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setattr(
        main_module,
        "create_rate_limiter",
        lambda: AllowAllRateLimiter(),
    )
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False)
    object_stores = {
        f"node-{index}": MemoryObjectStore()
        for index in range(1, 4)
    }
    configured_nodes = [
        ConfiguredStorageNode(
            node_name=f"node-{index}",
            node_address=f"node-{index}:9000",
            capacity_bytes=1024 * 1024,
            bucket="distributed-files",
            object_store=object_stores[f"node-{index}"],
        )
        for index in range(1, 4)
    ]
    storage_registry = StorageRegistry(configured_nodes)
    with test_session() as db:
        for configured in configured_nodes:
            db.add(
                StorageNode(
                    node_name=configured.node_name,
                    node_address=configured.node_address,
                    status="healthy",
                    capacity_bytes=configured.capacity_bytes,
                    used_bytes=0,
                    last_heartbeat_at=datetime.now(timezone.utc),
                )
            )
        db.commit()

    def override_get_db():
        with test_session() as db:
            yield db

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_storage_registry] = lambda: storage_registry
    with TestClient(app) as client:
        registration = client.post(
            "/api/auth/register",
            json={
                "username": "api-user",
                "email": "api-user@example.com",
                "password": "test-password-123",
            },
        )
        assert registration.status_code == 201
        login = client.post(
            "/api/auth/login",
            json={
                "email": "api-user@example.com",
                "password": "test-password-123",
            },
        )
        assert login.status_code == 200
        headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
        with test_session() as db:
            metadata_engine.register_file(
                db,
                owner_id=UUID(registration.json()["user_id"]),
                filename="owned.txt",
                checksum="d" * 64,
                file_size=4,
            )
            db.commit()
        yield client, headers, test_session, object_stores

    app.dependency_overrides.clear()
    engine.dispose()


def test_health_is_public(api_client) -> None:
    client, _, _, _ = api_client
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_register_login_and_current_user(api_client) -> None:
    client, headers, _, _ = api_client
    response = client.get("/api/auth/me", headers=headers)

    assert response.status_code == 200
    assert response.json()["email"] == "api-user@example.com"
    assert response.json()["role"] == "viewer"


def test_refresh_rotation_reuse_detection_and_session_revocation(api_client) -> None:
    client, headers, session_factory, _ = api_client
    old_refresh_token = client.cookies.get("dfms_refresh")
    assert old_refresh_token

    refreshed = client.post("/api/auth/refresh")
    assert refreshed.status_code == 200
    new_refresh_token = client.cookies.get("dfms_refresh")
    assert new_refresh_token and new_refresh_token != old_refresh_token
    new_headers = {
        "Authorization": f"Bearer {refreshed.json()['access_token']}"
    }

    assert client.get("/api/auth/me", headers=headers).status_code == 401
    reuse = client.post(
        "/api/auth/refresh",
        cookies={"dfms_refresh": old_refresh_token},
    )
    assert reuse.status_code == 401
    assert client.get("/api/auth/me", headers=new_headers).status_code == 401

    with session_factory() as db:
        sessions = list(db.query(RefreshSession).all())
        assert len(sessions) == 2
        assert all(session.revoked_at is not None for session in sessions)
        assert db.query(AuditLog).filter_by(
            action="refresh_token_reuse_detected"
        ).count() == 1


def test_logout_revokes_the_access_session_and_audits_event(api_client) -> None:
    client, headers, session_factory, _ = api_client

    response = client.post("/api/auth/logout", headers=headers)

    assert response.status_code == 204
    assert client.get("/api/auth/me", headers=headers).status_code == 401
    with session_factory() as db:
        assert db.query(AuditLog).filter_by(action="logout").count() == 1


def test_file_api_requires_a_valid_bearer_token(api_client) -> None:
    client, _, _, _ = api_client

    assert client.get("/api/files").status_code == 401
    assert (
        client.get(
            "/api/files",
            headers={"Authorization": "Bearer invalid"},
        ).status_code
        == 401
    )


def test_list_files_is_authenticated_and_validates_pagination(api_client) -> None:
    client, headers, _, _ = api_client
    response = client.get("/api/files?limit=10&offset=5", headers=headers)

    assert response.status_code == 200
    assert response.json()["limit"] == 10
    assert response.json()["offset"] == 5
    assert response.json()["total"] == 1
    assert response.json()["items"] == []

    first_page = client.get("/api/files?limit=10&offset=0", headers=headers)
    assert first_page.json()["items"][0]["filename"] == "owned.txt"

    invalid = client.get("/api/files?limit=0", headers=headers)
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"


def test_authenticated_upload_persists_metadata_and_minio_content(api_client) -> None:
    client, headers, _, object_stores = api_client
    content = b"phase three content"
    response = client.post(
        "/api/files/upload",
        files={"file": ("report.pdf", content, "application/pdf")},
        headers=headers,
    )

    assert response.status_code == 201
    assert response.json()["filename"] == "report.pdf"
    assert response.json()["file_size"] == len(content)
    assert response.json()["upload_state"] == "UPLOADED"
    assert response.json()["availability_state"] == "DEGRADED"
    file_id = response.json()["file_id"]

    download = client.get(f"/api/files/{file_id}/download", headers=headers)
    assert download.status_code == 200
    assert download.content == content
    assert sum(len(store.objects) for store in object_stores.values()) == 1


def test_upload_rejects_traversal_filename(api_client) -> None:
    client, headers, _, object_stores = api_client

    response = client.post(
        "/api/files/upload",
        files={"file": ("../secret.txt", b"private", "text/plain")},
        headers=headers,
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_FILENAME"
    assert all(not store.objects for store in object_stores.values())


def test_download_uses_another_verified_replica_after_checksum_failure(
    api_client,
) -> None:
    client, headers, test_session, object_stores = api_client
    content = b"replica survives"
    uploaded = client.post(
        "/api/files/upload",
        files={"file": ("surviving.txt", content, "text/plain")},
        headers=headers,
    )
    file_id = UUID(uploaded.json()["file_id"])

    with test_session() as db:
        replicas = db.query(FileReplica).filter_by(file_id=file_id).all()
        assert len(replicas) == 1
        primary_replica = replicas[0]
        primary_node = db.get(StorageNode, primary_replica.node_id)
        assert primary_node is not None
        replacement_node = (
            db.query(StorageNode)
            .filter(StorageNode.node_id != primary_node.node_id)
            .first()
        )
        assert replacement_node is not None
        replacement_store = object_stores[replacement_node.node_name]
        object_id = f"distributed-files/{primary_replica.object_key}"
        replacement_store.objects[object_id] = content
        db.add(
            FileReplica(
                file_id=primary_replica.file_id,
                version_id=primary_replica.version_id,
                node_id=replacement_node.node_id,
                object_key=primary_replica.object_key,
                checksum=primary_replica.checksum,
                status="VERIFIED",
                checksum_verified_at=datetime.now(timezone.utc),
            )
        )
        object_stores[primary_node.node_name].objects[object_id] = b"corrupted"
        db.commit()

    download = client.get(
        f"/api/files/{file_id}/download",
        headers=headers,
    )

    assert download.status_code == 200
    assert download.content == content
    with test_session() as db:
        assert db.get(FileReplica, primary_replica.replica_id).status == "CORRUPT"


def test_invalid_credentials_do_not_issue_a_token(api_client) -> None:
    client, _, _, _ = api_client
    response = client.post(
        "/api/auth/login",
        json={
            "email": "api-user@example.com",
            "password": "wrong-password",
        },
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_CREDENTIALS"


def test_password_is_not_reflected_in_validation_errors(api_client) -> None:
    client, _, _, _ = api_client
    password = "Sup3r!x"
    response = client.post(
        "/api/auth/register",
        json={
            "username": "other-user",
            "email": "other@example.com",
            "password": password,
        },
    )

    assert response.status_code == 422
    assert password not in response.text


def test_rate_limit_has_request_id_and_security_headers(api_client) -> None:
    client, _, _, _ = api_client

    class DenyRateLimiter:
        async def allow(self, _key: str, *, limit: int, window_seconds: int) -> bool:
            return False

        async def aclose(self) -> None:
            return None

    client.app.state.rate_limiter = DenyRateLimiter()
    response = client.post(
        "/api/auth/login",
        json={"email": "anything@example.com", "password": "no-password"},
        headers={"X-Request-ID": "00000000-0000-0000-0000-000000000123"},
    )

    assert response.status_code == 429
    assert response.headers["x-request-id"] == "00000000-0000-0000-0000-000000000123"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["retry-after"] == "60"


def test_file_metadata_and_delete_are_owner_scoped(api_client) -> None:
    client, headers, session_factory, _ = api_client
    listing = client.get("/api/files", headers=headers).json()
    file_id = listing["items"][0]["file_id"]

    metadata = client.get(f"/api/files/{file_id}", headers=headers)
    assert metadata.status_code == 200
    assert metadata.json()["filename"] == "owned.txt"

    deleted = client.delete(f"/api/files/{file_id}", headers=headers)
    assert deleted.status_code == 204
    assert client.get(f"/api/files/{file_id}", headers=headers).status_code == 404
    with session_factory() as db:
        cleanup_job = db.scalar(
            select(ReplicationJob).where(ReplicationJob.job_type == "CLEANUP")
        )
        assert cleanup_job is not None
        assert db.scalar(
            select(OutboxEvent).where(OutboxEvent.job_id == cleanup_job.job_id)
        ) is not None


def test_other_account_cannot_read_or_delete_a_file(api_client) -> None:
    client, owner_headers, _, _ = api_client
    file_id = client.get("/api/files", headers=owner_headers).json()["items"][0][
        "file_id"
    ]

    other = client.post(
        "/api/auth/register",
        json={
            "username": "other-user",
            "email": "other-user@example.com",
            "password": "other-password-123",
        },
    )
    assert other.status_code == 201
    token = client.post(
        "/api/auth/login",
        json={
            "email": "other-user@example.com",
            "password": "other-password-123",
        },
    ).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get(f"/api/files/{file_id}", headers=headers).status_code == 404
    assert client.get(f"/api/files/{file_id}/download", headers=headers).status_code == 404
    assert client.delete(f"/api/files/{file_id}", headers=headers).status_code == 404


def test_node_health_and_registration_require_admin(
    api_client,
) -> None:
    client, headers, _, _ = api_client

    assert client.get("/api/nodes/health").status_code == 401
    assert client.get("/api/nodes/health", headers=headers).status_code == 403
    response = client.post(
        "/api/nodes",
        headers=headers,
        json={
            "node_name": "node-a",
            "node_address": "http://node-a",
            "capacity_bytes": 1000,
        },
    )
    assert response.status_code == 403


def test_admin_can_register_node_and_record_health(api_client) -> None:
    client, headers, test_session, _ = api_client
    current_user = client.get("/api/auth/me", headers=headers).json()
    with test_session() as db:
        user = db.get(User, UUID(current_user["user_id"]))
        assert user is not None
        user.role = "admin"
        db.commit()

    registration = client.post(
        "/api/nodes",
        headers=headers,
        json={
            "node_name": "node-a",
            "node_address": "http://node-a",
            "capacity_bytes": 1000,
        },
    )
    assert registration.status_code == 201
    node_id = registration.json()["node_id"]
    heartbeat = client.post(
        f"/api/nodes/{node_id}/heartbeat",
        headers=headers,
        json={"status": "healthy", "latency_ms": 3, "used_bytes": 20},
    )
    health = client.get("/api/nodes/health", headers=headers)

    assert heartbeat.status_code == 202
    nodes = health.json()["storage_nodes"]
    assert nodes[0]["status"] == "healthy"


def test_only_admin_can_retry_dead_replication_job(api_client) -> None:
    client, headers, test_session, _ = api_client
    with test_session() as db:
        owner = db.query(User).filter_by(username="api-user").one()
        file_version = db.query(FileVersion).filter_by(created_by=owner.user_id).one()
        job = ReplicationJob(
            file_id=file_version.file_id,
            version_id=file_version.version_id,
            target_count=3,
            idempotency_key="api-test-retry-job",
            status="DEAD",
            attempt_count=5,
            max_attempts=5,
            last_error="temporary node failure",
        )
        db.add(job)
        db.commit()
        job_id = job.job_id

    denied = client.post(
        f"/api/nodes/replication-jobs/{job_id}/retry",
        headers=headers,
    )
    assert denied.status_code == 403

    with test_session() as db:
        owner = db.query(User).filter_by(username="api-user").one()
        owner.role = "admin"
        db.commit()

    response = client.post(
        f"/api/nodes/replication-jobs/{job_id}/retry",
        headers=headers,
    )

    assert response.status_code == 202
    assert response.json()["status"] == "PENDING"
    assert response.json()["attempt_count"] == 0
