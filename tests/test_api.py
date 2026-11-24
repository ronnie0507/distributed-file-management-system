from collections.abc import Generator
from io import BytesIO
from typing import BinaryIO
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database.base import Base
from backend.database.models import User
from backend.database.session import get_db
from backend.api.dependencies import get_file_engine
from backend.engines.files import FileEngine
from backend.engines.metadata import metadata_engine
from backend.main import app


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


@pytest.fixture
def api_client(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[
    tuple[TestClient, dict[str, str], sessionmaker, MemoryObjectStore],
    None,
    None,
]:
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-that-is-at-least-32-chars")
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False)
    object_store = MemoryObjectStore()
    file_engine = FileEngine(object_store)

    def override_get_db():
        with test_session() as db:
            yield db

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_file_engine] = lambda: file_engine
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
        yield client, headers, test_session, object_store

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
    client, headers, _, object_store = api_client
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
    file_id = response.json()["file_id"]

    download = client.get(f"/api/files/{file_id}/download", headers=headers)
    assert download.status_code == 200
    assert download.content == content
    assert len(object_store.objects) == 1


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


def test_file_metadata_and_delete_are_owner_scoped(api_client) -> None:
    client, headers, _, _ = api_client
    listing = client.get("/api/files", headers=headers).json()
    file_id = listing["items"][0]["file_id"]

    metadata = client.get(f"/api/files/{file_id}", headers=headers)
    assert metadata.status_code == 200
    assert metadata.json()["filename"] == "owned.txt"

    deleted = client.delete(f"/api/files/{file_id}", headers=headers)
    assert deleted.status_code == 204
    assert client.get(f"/api/files/{file_id}", headers=headers).status_code == 404


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
    assert client.delete(f"/api/files/{file_id}", headers=headers).status_code == 404


def test_node_health_is_public_but_node_registration_requires_admin(
    api_client,
) -> None:
    client, headers, _, _ = api_client

    assert client.get("/api/nodes/health").json()["storage_nodes"] == []
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

    assert heartbeat.status_code == 202
    nodes = client.get("/api/nodes/health").json()["storage_nodes"]
    assert nodes[0]["status"] == "healthy"
