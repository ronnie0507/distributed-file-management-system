from io import BytesIO

from fastapi.testclient import TestClient

from backend.main import app


client = TestClient(app)


def test_health_through_application() -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["phase"] == "networking-and-api"


def test_list_files_valid_pagination() -> None:
    response = client.get("/api/files?limit=10&offset=5")

    assert response.status_code == 200
    assert response.json()["limit"] == 10
    assert response.json()["offset"] == 5
    assert response.json()["items"] == []


def test_list_files_rejects_invalid_pagination() -> None:
    response = client.get("/api/files?limit=0")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_upload_accepts_a_non_empty_file() -> None:
    response = client.post(
        "/api/files/upload",
        files={"file": ("report.pdf", BytesIO(b"phase two"), "application/pdf")},
    )

    assert response.status_code == 202
    assert response.json()["filename"] == "report.pdf"
    assert response.json()["size_bytes"] == 9
    assert response.json()["upload_state"] == "PENDING"


def test_upload_rejects_an_empty_file() -> None:
    response = client.post(
        "/api/files/upload",
        files={"file": ("empty.txt", BytesIO(b""), "text/plain")},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "EMPTY_FILE"


def test_download_is_explicitly_not_implemented_in_phase_two() -> None:
    response = client.get("/api/files/00000000-0000-0000-0000-000000000000/download")

    assert response.status_code == 501
    assert response.json()["error"]["code"] == "NOT_IMPLEMENTED"
