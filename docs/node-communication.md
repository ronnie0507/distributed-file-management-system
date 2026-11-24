# Node Communication and Failure Handling

## Metadata nodes

Three metadata API instances share PostgreSQL in Phase 1/2. Later, etcd membership elects
a leader for metadata writes. Followers may serve eligible reads from PostgreSQL.

## Storage nodes

The Distribution Engine selects distinct, healthy, capacity-eligible nodes using the
file UUID and a virtual-node consistent-hash ring. Heartbeats older than the configured
TTL are excluded. The storage registry resolves database node records only to trusted
environment-configured endpoints; administrative node addresses are never used as
arbitrary worker URLs.

The local Compose topology uses three private MinIO endpoints, each with a separate
volume and credentials. Storage calls have bounded connect/read timeouts. Uploads and
replications are read back and SHA-256 verified before a replica is marked `VERIFIED`.
Downloads verify the complete object before streaming it and can fall through to another
healthy verified replica.

The database-backed worker queue is durable and idempotent. Jobs use `PENDING`,
`PROCESSING`, `RETRY`, `COMPLETED`, and `DEAD` states; PostgreSQL row locks prevent
concurrent claims and expiring leases make interrupted work reclaimable. Retries are
bounded with backoff. Dead jobs retain their last error and can be retried by an
administrator. RabbitMQ and a separate transactional outbox are deferred because the
database job row is currently created in the same transaction as the initial replica.

The worker probes each configured object store, updates health/capacity, reconciles
under-replicated current versions, and rebuilds them from a verified source. Existing
Phase 3 objects are backfilled only after read-back checksum validation on configured
node 1.

Three containers on one Docker host are not independent physical fault domains. With
only three nodes, a failed node leaves a file readable but degraded; a fourth healthy
node (or a recovered original) is required to restore replication factor three.

## Planned failure policies

| Scenario | Response |
|---|---|
| One metadata node fails | Continue reads/writes through healthy nodes |
| Metadata leader fails | etcd elects another healthy leader |
| Two etcd members fail | Lose write quorum; report degraded service honestly |
| PostgreSQL unavailable | Stop metadata writes and return service degradation |
| One storage node fails | Exclude it from placement and serve from verified survivors |
| Node restarts | Reconcile inventory and repair on a healthy replacement node |
| Redis fails | Bypass cache and read PostgreSQL |
| Object checksum mismatch | Mark replica corrupt and retry from another verified source |

## Upload state machine

Upload lifecycle and availability are separate:

- Upload lifecycle: `PENDING -> UPLOADED` after initial content verification and atomic
  metadata/replica/job persistence.
- Availability: `AVAILABLE` when the required count is verified on fresh healthy nodes,
  `DEGRADED` when at least one such replica remains, and `UNAVAILABLE` when none do.
- Replica state: `PENDING -> COPYING -> VERIFIED`, or `FAILED`/`CORRUPT`; only `VERIFIED`
  replicas are download candidates.

A failed metadata transaction triggers cleanup of the initial object. If cleanup also
fails, both failures are surfaced. A worker/database failure after a copy leaves a
reclaimable job lease and an idempotent object key; a retry must re-verify the object
before marking metadata verified.
