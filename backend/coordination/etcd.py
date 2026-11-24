import asyncio
import json
import logging
import os
import threading
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import aetcd
import grpc


_LOGGER = logging.getLogger(__name__)
_MEMBERSHIP_PREFIX = b"/dfms/membership/storage"
_LEADER_KEY = b"/dfms/elections/recovery-coordinator"


class CoordinationUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class EtcdEndpoint:
    host: str
    port: int


@dataclass(frozen=True)
class EtcdConfig:
    endpoints: tuple[EtcdEndpoint, ...]
    username: str
    password: str
    timeout_seconds: float = 5
    membership_ttl_seconds: int = 30
    leader_ttl_seconds: int = 30

    @classmethod
    def from_env(cls) -> "EtcdConfig":
        raw_endpoints = os.getenv(
            "ETCD_ENDPOINTS",
            "etcd-1:2379,etcd-2:2379,etcd-3:2379",
        )
        endpoints: list[EtcdEndpoint] = []
        for value in raw_endpoints.split(","):
            host, separator, raw_port = value.strip().rpartition(":")
            if not separator or not host:
                raise ValueError(f"Invalid etcd endpoint: {value!r}")
            endpoints.append(EtcdEndpoint(host=host, port=int(raw_port)))
        username = os.getenv("ETCD_USERNAME", "")
        password = os.getenv("ETCD_PASSWORD", "")
        if not username or not password:
            raise ValueError("ETCD_USERNAME and ETCD_PASSWORD must be configured")
        timeout = float(os.getenv("ETCD_TIMEOUT_SECONDS", "5"))
        member_ttl = int(os.getenv("ETCD_MEMBERSHIP_TTL_SECONDS", "30"))
        leader_ttl = int(os.getenv("ETCD_LEADER_TTL_SECONDS", "30"))
        if timeout <= 0 or member_ttl < 3 or leader_ttl < 3:
            raise ValueError("etcd timeout and lease durations are invalid")
        return cls(
            endpoints=tuple(endpoints),
            username=username,
            password=password,
            timeout_seconds=timeout,
            membership_ttl_seconds=member_ttl,
            leader_ttl_seconds=leader_ttl,
        )


class EtcdCoordinator:
    def __init__(
        self,
        config: EtcdConfig,
        *,
        worker_id: UUID | None = None,
        client_factory: Callable[..., Any] = aetcd.Client,
    ) -> None:
        if not config.endpoints:
            raise ValueError("At least one etcd endpoint is required")
        self._config = config
        self._worker_id = worker_id or uuid4()
        self._client_factory = client_factory
        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"etcd-coordinator-{self._worker_id}",
            daemon=True,
        )
        self._clients: list[Any] = []
        self._membership_leases: dict[bytes, int] = {}
        self._leader_lease_id: int | None = None
        self._leader_token: bytes | None = None
        self._closed = False
        self._thread.start()
        if not self._loop_ready.wait(timeout=config.timeout_seconds):
            raise CoordinationUnavailableError("etcd event loop did not start")
        self._run(self._initialize_clients())

    def register_membership(
        self,
        *,
        node_id: UUID,
        node_name: str,
        endpoint: str,
        status: str,
        capacity_bytes: int | None,
    ) -> int:
        key = (
            _MEMBERSHIP_PREFIX
            + b"/"
            + str(node_id).encode("ascii")
            + b"/"
            + str(self._worker_id).encode("ascii")
        )
        value = json.dumps(
            {
                "node_id": str(node_id),
                "node_name": node_name,
                "endpoint": endpoint,
                "status": status,
                "capacity_bytes": capacity_bytes,
                "worker_id": str(self._worker_id),
            },
            separators=(",", ":"),
        ).encode("utf-8")

        async def register(client: Any) -> int:
            lease_id = self._membership_leases.get(key)
            if lease_id is not None:
                response = await client.refresh_lease(lease_id)
                if response.TTL > 0:
                    await client.put(key, value, lease=lease_id)
                    return lease_id
                self._membership_leases.pop(key, None)
            lease = await client.lease(self._config.membership_ttl_seconds)
            await client.put(key, value, lease=lease)
            self._membership_leases[key] = lease.id
            return lease.id

        return self._run(self._with_client(register))

    def try_acquire_recovery_leadership(self) -> bool:
        async def acquire(client: Any) -> bool:
            if self._leader_lease_id is not None and self._leader_token is not None:
                try:
                    response = await client.refresh_lease(self._leader_lease_id)
                except (aetcd.ClientError, grpc.RpcError, OSError, TimeoutError):
                    raise
                if response.TTL > 0:
                    return True
                self._leader_lease_id = None
                self._leader_token = None

            lease = await client.lease(self._config.leader_ttl_seconds)
            token = str(self._worker_id).encode("ascii")
            acquired, _ = await client.transaction(
                compare=[client.transactions.create(_LEADER_KEY) == 0],
                success=[
                    client.transactions.put(
                        _LEADER_KEY,
                        token,
                        lease=lease,
                    )
                ],
                failure=[],
            )
            if acquired:
                self._leader_lease_id = lease.id
                self._leader_token = token
                return True
            await lease.revoke()
            return False

        try:
            return self._run(self._with_client(acquire))
        except CoordinationUnavailableError:
            self._leader_lease_id = None
            self._leader_token = None
            raise

    def release_recovery_leadership(self) -> bool:
        async def release(client: Any) -> bool:
            if self._leader_lease_id is None or self._leader_token is None:
                return False
            lease_id = self._leader_lease_id
            success, _ = await client.transaction(
                compare=[
                    client.transactions.value(_LEADER_KEY)
                    == self._leader_token
                ],
                success=[client.transactions.delete(_LEADER_KEY)],
                failure=[],
            )
            await client.revoke_lease(lease_id)
            self._leader_lease_id = None
            self._leader_token = None
            return success

        return self._run(self._with_client(release))

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.release_recovery_leadership()
        except CoordinationUnavailableError:
            _LOGGER.warning("Could not release etcd leadership before shutdown")
        self._run(self._close_clients())
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=self._config.timeout_seconds)
        self._loop.close()

    async def _initialize_clients(self) -> None:
        self._clients = [
            self._client_factory(
                host=endpoint.host,
                port=endpoint.port,
                username=self._config.username,
                password=self._config.password,
                timeout=self._config.timeout_seconds,
            )
            for endpoint in self._config.endpoints
        ]

    async def _close_clients(self) -> None:
        for client in self._clients:
            await client.close()

    async def _with_client(
        self,
        operation: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        failures: list[Exception] = []
        for client in self._clients:
            try:
                await client.connect()
                return await operation(client)
            except (aetcd.ClientError, grpc.RpcError, OSError, TimeoutError) as exc:
                failures.append(exc)
        error = CoordinationUnavailableError(
            "No etcd endpoint is available; coordination is currently unavailable"
        )
        if failures:
            raise error from failures[-1]
        raise error

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        self._loop.run_forever()

    def _run(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        if self._closed:
            raise RuntimeError("EtcdCoordinator is closed")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        timeout = self._config.timeout_seconds * max(
            len(self._config.endpoints),
            1,
        ) + 1
        return future.result(timeout=timeout)


def create_etcd_coordinator() -> EtcdCoordinator:
    return EtcdCoordinator(EtcdConfig.from_env())
