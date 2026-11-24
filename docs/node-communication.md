# Node Communication and Failure Handling

## Metadata nodes

Three metadata API instances share PostgreSQL in Phase 1/2. Later, etcd membership elects
a leader for metadata writes. Followers may serve eligible reads from PostgreSQL.

## Storage nodes

Storage node API calls must use bounded retries, timeouts, and idempotency keys. Placement
uses the file UUID and a virtual-node consistent-hash ring. Replicas are selected from
distinct ring positions where possible.

The current Distribution Engine records administrator-submitted heartbeats and computes
capacity-aware target nodes. It does not yet send content to independent storage nodes or
repair replicas. MinIO is the active content backend.

## Planned failure policies

| Scenario | Response |
|---|---|
| One metadata node fails | Continue reads/writes through healthy nodes |
| Metadata leader fails | etcd elects another healthy leader |
| Two etcd members fail | Lose write quorum; report degraded service honestly |
| PostgreSQL unavailable | Stop metadata writes and return service degradation |
| One storage node fails | Serve or replicate from another available replica |
| Node restarts | Reconcile replica inventory and repair missing replicas |
| Redis fails | Bypass cache and read PostgreSQL |
| Object checksum mismatch | Mark replica corrupt and schedule restoration |

## Upload state machine

`PENDING -> UPLOADED -> REPLICATING -> AVAILABLE`

The current upload path commits `PENDING -> UPLOADED` after MinIO and PostgreSQL succeed.
Transitions to `REPLICATING` and `AVAILABLE`, retry scheduling, and orphan reconciliation
require the future replication workers. Failed object storage leaves metadata unchanged;
a metadata commit failure triggers a best-effort object cleanup and surfaces cleanup errors.
