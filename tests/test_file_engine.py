import hashlib
from io import BytesIO
from typing import BinaryIO
from uuid import uuid4

import pytest

from backend.engines.files import ChecksumMismatchError, FileEngine, FileTooLargeError


class MemoryObjectReader(BytesIO):
    def read(self, size: int | None = -1, /) -> bytes:
        return super().read(-1 if size is None else size)

    def release_conn(self) -> None:
        return None


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, tuple[str, str]] = {}
        self.corrupt_writes = False

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
        if self.corrupt_writes:
            content += b"corruption"
        self.objects[f"{bucket}/{object_key}"] = content
        self.metadata[object_key] = (content_type, checksum)

    def get_object(self, *, bucket: str, object_key: str) -> MemoryObjectReader:
        return MemoryObjectReader(self.objects[f"{bucket}/{object_key}"])

    def remove_object(self, *, bucket: str, object_key: str) -> None:
        self.objects.pop(f"{bucket}/{object_key}", None)


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


def test_store_content_rejects_and_cleans_up_corrupted_write() -> None:
    store = MemoryObjectStore()
    store.corrupt_writes = True
    engine = FileEngine(store)

    with pytest.raises(ChecksumMismatchError):
        engine.store_content(
            file_id=uuid4(),
            version_number=1,
            source=BytesIO(b"valid input"),
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
