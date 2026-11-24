import hashlib
from io import BytesIO
from uuid import uuid4

import pytest

from backend.engines.files import FileEngine, FileTooLargeError


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, tuple[str, str]] = {}

    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        data: BytesIO,
        size_bytes: int,
        content_type: str,
        checksum: str,
    ) -> None:
        content = data.read()
        assert len(content) == size_bytes
        self.objects[f"{bucket}/{object_key}"] = content
        self.metadata[object_key] = (content_type, checksum)


def test_store_content_streams_to_object_store_with_sha256() -> None:
    store = MemoryObjectStore()
    engine = FileEngine(store)
    file_id = uuid4()
    content = b"file-engine-content"

    stored = engine.store_content(
        file_id=file_id,
        version_number=2,
        source=BytesIO(content),
        content_type="application/octet-stream",
    )

    expected_checksum = hashlib.sha256(content).hexdigest()
    assert stored.checksum == expected_checksum
    assert stored.size_bytes == len(content)
    assert stored.object_key == f"files/{file_id}/versions/2/{expected_checksum}"
    assert store.objects[f"distributed-files/{stored.object_key}"] == content
    assert store.metadata[stored.object_key] == (
        "application/octet-stream",
        expected_checksum,
    )


def test_store_content_rejects_empty_file() -> None:
    engine = FileEngine(MemoryObjectStore())

    with pytest.raises(ValueError, match="must not be empty"):
        engine.store_content(
            file_id=uuid4(),
            version_number=1,
            source=BytesIO(b""),
            content_type=None,
        )


def test_store_content_enforces_size_limit_before_storage() -> None:
    store = MemoryObjectStore()
    engine = FileEngine(store, max_file_size_bytes=4)

    with pytest.raises(FileTooLargeError):
        engine.store_content(
            file_id=uuid4(),
            version_number=1,
            source=BytesIO(b"12345"),
            content_type="text/plain",
        )

    assert store.objects == {}


def test_store_content_requires_positive_version() -> None:
    engine = FileEngine(MemoryObjectStore())

    with pytest.raises(ValueError, match="version_number"):
        engine.store_content(
            file_id=uuid4(),
            version_number=0,
            source=BytesIO(b"data"),
            content_type="text/plain",
        )
