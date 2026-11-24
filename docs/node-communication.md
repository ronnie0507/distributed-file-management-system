# Node Communication and Failure Handling

## PostgreSQL, outbox, and RabbitMQ

PostgreSQL is authoritative for file metadata, replica state, replication jobs, and
worker fencing. A transaction that creates or retries a job also inserts its outbox
event. The outbox relay claims pending events with an expiring lease, publishes a
persistent message, waits for the RabbitMQ publisher confirm, and only then marks the
event published in PostgreSQL.

The broker provides at-least-once delivery, not exactly-once execution. A relay crash
after a confirm but before the published-state commit can produce a duplicate message.
Messages carry event and job IDs; consumers reload the PostgreSQL job, and completed
jobs are no-ops. PostgreSQL row locks, expiring job leases, fencing tokens, and a
per-file lease prevent concurrent or stale workers from committing conflicting work.
Consumers manually acknowledge only after the database-backed operation reaches a
safe terminal or retry state. Transient failures are requeued; permanent or exhausted
jobs are dead-lettered and retain their failure in PostgreSQL.

The outbox relay runs independently of the consumer loop. If RabbitMQ is unavailable,
the relay leaves events pending and retries after reconnect; committed jobs are not
silently lost. Periodic reconciliation re-enqueues any eligible job that lacks a
current dispatch event.

## etcd coordination

The three-member etcd cluster is used for leased storage-node membership and election
of one recovery coordinator. It does not store authoritative file metadata and does
not replace PostgreSQL job locks. Membership registrations expire unless renewed and
are recreated after restart. Recovery coordination uses an etcd lease and
compare-and-set election key, allowing another worker to take over after expiry.

Three etcd members require a quorum of two. If quorum is lost, workers fail closed for
leader-only health refresh and reconciliation and report coordination as unavailable.
RabbitMQ consumers continue processing already-published jobs under PostgreSQL
fencing. This development topology is still one Docker host and does not provide
independent physical failure domains.

## Storage nodes and replicas

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

The elected coordinator refreshes configured-node health and periodically schedules
reconciliation, integrity verification, replica repair, and cleanup jobs. Existing
objects are backfilled only after read-back checksum validation. Three containers on
one Docker host are not independent physical fault domains. With only three storage
nodes, a failed node leaves a file readable but degraded; a fourth healthy node (or a
recovered original) is required to restore replication factor three.

## Failure policies

| Scenario | Response |
|---|---|
| One application worker fails | RabbitMQ redelivers unacknowledged jobs; PostgreSQL leases expire for safe takeover |
| RabbitMQ is unavailable | Outbox events remain pending and the relay retries |
| etcd quorum is lost | Pause leader-only maintenance; continue PostgreSQL-fenced job consumption |
| Recovery coordinator fails | Its etcd lease expires and another worker may acquire leadership |
| PostgreSQL is unavailable | Do not acknowledge successful job completion; RabbitMQ redelivers |
| One storage node fails | Exclude it from placement and serve from verified survivors |
| Node restarts | Renew membership and reconcile/repair from a verified source |
| Object checksum mismatch | Do not mark the copy verified; mark the bad replica and retry from another verified source |

## Upload and job state

Upload lifecycle and availability are separate:

- Upload lifecycle: `PENDING -> UPLOADED` after initial content verification and atomic
  metadata/replica/job/outbox persistence.
- Availability: `AVAILABLE` when the required count is verified on fresh healthy nodes,
  `DEGRADED` when at least one such replica remains, and `UNAVAILABLE` when none do.
- Replica state: `PENDING -> COPYING -> VERIFIED`, or `FAILED`/`CORRUPT`; only `VERIFIED`
  replicas are download candidates.
- Job state: `PENDING -> PROCESSING -> COMPLETED`, or `RETRY` with bounded backoff and
  eventually `DEAD`. Expired leases allow takeover; fencing prevents a stale owner from
  updating a reclaimed job.

A failed metadata transaction triggers cleanup of the initial object. If cleanup also
fails, both failures are surfaced. A worker/database failure after a copy leaves a
reclaimable job lease and an idempotent object key; a retry must re-verify the object
before marking metadata verified.
