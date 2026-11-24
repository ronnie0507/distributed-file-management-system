import os
from dataclasses import dataclass
from typing import Protocol

if __package__ == "backend.engines":
    from ..database.models import StorageNode
    from .files import DEFAULT_BUCKET, MinioObjectStore, ObjectStore
else:
    from database.models import StorageNode
    from engines.files import DEFAULT_BUCKET, MinioObjectStore, ObjectStore


class HealthCheckedObjectStore(ObjectStore, Protocol):
    def health_check(self) -> None: ...

    def used_bytes(self, bucket: str) -> int: ...


@dataclass(frozen=True)
class ConfiguredStorageNode:
    node_name: str
    node_address: str
    capacity_bytes: int | None
    bucket: str
    object_store: HealthCheckedObjectStore


class StorageRegistry:
    def __init__(self, nodes: list[ConfiguredStorageNode]) -> None:
        by_name = {node.node_name: node for node in nodes}
        if len(by_name) != len(nodes):
            raise ValueError("Configured storage-node names must be unique")
        if len({node.node_address for node in nodes}) != len(nodes):
            raise ValueError("Configured storage endpoints must be unique")
        self._nodes = by_name

    @property
    def configured_nodes(self) -> tuple[ConfiguredStorageNode, ...]:
        return tuple(self._nodes.values())

    def for_node(self, node: StorageNode) -> ConfiguredStorageNode:
        configured = self._nodes.get(node.node_name)
        if configured is None or configured.node_address != node.node_address:
            raise ValueError(
                f"Storage node {node.node_name!r} is not in the trusted configuration"
            )
        return configured


def create_storage_registry() -> StorageRegistry:
    node_count = int(os.getenv("STORAGE_NODE_COUNT", "3"))
    if node_count < 1:
        raise ValueError("STORAGE_NODE_COUNT must be positive")

    nodes: list[ConfiguredStorageNode] = []
    for index in range(1, node_count + 1):
        prefix = f"STORAGE_NODE_{index}_"
        node_name = os.getenv(f"{prefix}NAME", f"node-{index}")
        endpoint = os.getenv(
            f"{prefix}ENDPOINT",
            f"minio-node-{index}:9000",
        )
        access_key = os.getenv(
            f"{prefix}ACCESS_KEY",
        )
        secret_key = os.getenv(
            f"{prefix}SECRET_KEY",
        )
        if not access_key or not secret_key:
            raise RuntimeError(
                f"Credentials are required for configured node {node_name!r}"
            )
        capacity = os.getenv(f"{prefix}CAPACITY_BYTES")
        nodes.append(
            ConfiguredStorageNode(
                node_name=node_name,
                node_address=endpoint,
                capacity_bytes=int(capacity) if capacity else None,
                bucket=os.getenv("MINIO_BUCKET", DEFAULT_BUCKET),
                object_store=MinioObjectStore(
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=os.getenv(f"{prefix}SECURE", "false").lower() == "true",
                ),
            )
        )
    return StorageRegistry(nodes)
