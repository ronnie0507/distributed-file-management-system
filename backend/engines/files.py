import hashlib
import os
import tempfile
import unicodedata
from dataclasses import dataclass
from typing import BinaryIO, IO, Protocol, cast
from uuid import UUID

from minio import Minio
from minio.error import S3Error
from urllib3 import PoolManager
from urllib3.util import Timeout
from urllib3.response import BaseHTTPResponse


DEFAULT_BUCKET = "distributed-files"
MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024
_CHUNK_SIZE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class StoredContent:
    object_key: str
    checksum: str
    size_bytes: int
    content_type: str


class ObjectReader(Protocol):
    def read(self, size: int | None = -1, /) -> bytes: ...

    def close(self) -> None: ...

    def release_conn(self) -> None: ...


class ObjectStore(Protocol):
    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        data: BinaryIO,
        size_bytes: int,
        content_type: str,
        checksum: str,
    ) -> None: ...

    def get_object(self, *, bucket: str, object_key: str) -> ObjectReader: ...

    def remove_object(self, *, bucket: str, object_key: str) -> None: ...

class FileTooLargeError(ValueError):
    pass


class ChecksumMismatchError(ValueError):
    pass


class FileEngine:
    def __init__(
        self,
        object_store: ObjectStore,
        *,
        bucket: str = DEFAULT_BUCKET,
        max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
    ) -> None:
        self._object_store = object_store
        self._bucket = bucket
        self._max_file_size_bytes = max_file_size_bytes

    def store_content(
        self,
        *,
        file_id: UUID,
        version_number: int,
        source: IO[bytes],
        content_type: str | None,
        quarantine: bool = False,
    ) -> StoredContent:
        if version_number < 1:
            raise ValueError("version_number must be positive")

        checksum = hashlib.sha256()
        size_bytes = 0
        with tempfile.TemporaryFile(mode="w+b") as spool:
            while True:
                chunk = source.read(_CHUNK_SIZE_BYTES)
                if not chunk:
                    break
                size_bytes += len(chunk)
                if size_bytes > self._max_file_size_bytes:
                    raise FileTooLargeError(
                        f"File exceeds the {self._max_file_size_bytes}-byte limit"
                    )
                checksum.update(chunk)
                spool.write(chunk)

            if size_bytes == 0:
                raise ValueError("file content must not be empty")

            digest = checksum.hexdigest()
            object_key = (
                self.quarantine_object_key(
                    file_id=file_id,
                    version_number=version_number,
                    checksum=digest,
                )
                if quarantine
                else self.object_key(
                    file_id=file_id,
                    version_number=version_number,
                    checksum=digest,
                )
            )
            resolved_content_type = "application/octet-stream"
            spool.seek(0)
            self._object_store.put_object(
                bucket=self._bucket,
                object_key=object_key,
                data=cast(BinaryIO, spool),
                size_bytes=size_bytes,
                content_type=resolved_content_type,
                checksum=digest,
            )
            try:
                if not self.verify_content(object_key, digest):
                    raise ChecksumMismatchError(
                        "Stored object does not match its SHA-256 checksum"
                    )
            except Exception as storage_error:
                try:
                    self._object_store.remove_object(
                        bucket=self._bucket,
                        object_key=object_key,
                    )
                except Exception as cleanup_error:
                    raise ExceptionGroup(
                        "Object verification failed and cleanup also failed",
                        [storage_error, cleanup_error],
                    ) from storage_error
                raise

        return StoredContent(
            object_key=object_key,
            checksum=digest,
            size_bytes=size_bytes,
            content_type=resolved_content_type,
        )

    def open_content(self, object_key: str) -> ObjectReader:
        return self._object_store.get_object(
            bucket=self._bucket,
            object_key=object_key,
        )

    def open_verified_content(
        self,
        object_key: str,
        expected_checksum: str,
    ) -> IO[bytes]:
        spool = tempfile.TemporaryFile(mode="w+b")
        digest = hashlib.sha256()
        try:
            reader = self.open_content(object_key)
            try:
                while chunk := reader.read(_CHUNK_SIZE_BYTES):
                    digest.update(chunk)
                    spool.write(chunk)
            finally:
                try:
                    reader.close()
                finally:
                    reader.release_conn()
        except Exception:
            spool.close()
            raise
        if digest.hexdigest() != expected_checksum:
            spool.close()
            raise ChecksumMismatchError(
                "Stored object does not match its SHA-256 checksum"
            )
        spool.seek(0)
        return spool

    def verify_content(self, object_key: str, expected_checksum: str) -> bool:
        reader = self.open_content(object_key)
        digest = hashlib.sha256()
        try:
            while chunk := reader.read(_CHUNK_SIZE_BYTES):
                digest.update(chunk)
        finally:
            try:
                reader.close()
            finally:
                reader.release_conn()
        return digest.hexdigest() == expected_checksum

    def delete_content(self, object_key: str) -> None:
        self._object_store.remove_object(
            bucket=self._bucket,
            object_key=object_key,
        )

    @staticmethod
    def object_key(*, file_id: UUID, version_number: int, checksum: str) -> str:
        if version_number < 1:
            raise ValueError("version_number must be positive")
        return f"files/{file_id}/versions/{version_number}/{checksum}"

    @staticmethod
    def quarantine_object_key(
        *,
        file_id: UUID,
        version_number: int,
        checksum: str,
    ) -> str:
        if version_number < 1:
            raise ValueError("version_number must be positive")
        return f"quarantine/{file_id}/versions/{version_number}/{checksum}"

    @staticmethod
    def sanitize_filename(filename: str | None) -> str:
        normalized = unicodedata.normalize("NFC", (filename or "").strip())
        if not normalized or any(
            unicodedata.category(char).startswith("C") for char in normalized
        ):
            raise ValueError("filename is empty or contains control characters")
        components = normalized.replace("\\", "/").split("/")
        if any(component in {".", ".."} for component in components):
            raise ValueError("filename must not contain traversal components")
        safe_name = components[-1].strip()
        if not safe_name or len(safe_name) > 255:
            raise ValueError("filename must contain between 1 and 255 characters")
        return safe_name


class MinioObjectStore:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        http_client = PoolManager(
            timeout=Timeout(connect=3.0, read=30.0),
            maxsize=10,
            block=True,
        )
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=http_client,
        )

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
        self._ensure_bucket(bucket)

        self._client.put_object(
            bucket,
            object_key,
            data,
            size_bytes,
            content_type=content_type,
            metadata={"sha256": checksum},
        )

    def get_object(self, *, bucket: str, object_key: str) -> ObjectReader:
        return MinioObjectReader(self._client.get_object(bucket, object_key))

    def remove_object(self, *, bucket: str, object_key: str) -> None:
        self._client.remove_object(bucket, object_key)

    def health_check(self) -> None:
        self._client.list_buckets()

    def used_bytes(self, bucket: str) -> int:
        self._ensure_bucket(bucket)
        return sum(
            item.size or 0
            for item in self._client.list_objects(bucket, recursive=True)
        )

    def _ensure_bucket(self, bucket: str) -> None:
        if self._client.bucket_exists(bucket):
            return
        try:
            self._client.make_bucket(bucket)
        except S3Error as exc:
            if exc.code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                raise
            if not self._client.bucket_exists(bucket):
                raise


class MinioObjectReader:
    def __init__(self, response: BaseHTTPResponse) -> None:
        self._response = response

    def read(self, size: int | None = -1, /) -> bytes:
        return self._response.read(amt=size)

    def close(self) -> None:
        self._response.close()

    def release_conn(self) -> None:
        self._response.release_conn()


def create_file_engine() -> FileEngine:
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not endpoint or not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY are required"
        )
    object_store = MinioObjectStore(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    return FileEngine(
        object_store,
        bucket=os.getenv("MINIO_BUCKET", DEFAULT_BUCKET),
    )
