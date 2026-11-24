# Distributed File Management System

This repository implements an industry-oriented distributed file management
system using FastAPI, PostgreSQL, MinIO, Docker Compose, and a React frontend
planned for a later phase.

## Phase 1 - Foundation

- [x] Architecture and engine responsibilities
- [x] PostgreSQL metadata schema
- [x] Docker Compose infrastructure
- [x] API specification and failure-handling design
- [x] FastAPI project skeleton

## Phase 2 - Networking and API

- [x] Nginx reverse proxy and request limits
- [x] Modular FastAPI route structure
- [x] Structured validation and HTTP errors
- [x] Planned file and node API endpoints
- [x] Focused API tests
- [ ] Container verification (Docker engine unavailable on the development machine)

## Phase 3 - Engine Development

- [x] Metadata ORM models and database session factory
- [x] Owner-scoped metadata operations and upload-state transitions
- [x] Initial immutable file version recorded with metadata
- [x] Argon2 password hashing, signed JWTs, and file-role authorization
- [x] Authenticated file metadata, upload, download, and soft-delete API
- [x] MinIO streaming content with SHA-256 and a 25 MiB limit
- [x] Storage-node registration, health reporting, and deterministic replica targets
- [x] SHA-256-verified replication across independent storage endpoints
- [x] Durable replication jobs, bounded retries, and restart-safe leases
- [x] Availability states and verified-replica download failover
- [x] Health-based recovery and replacement-node repair
- [ ] Live PostgreSQL/MinIO integration verification (Docker engine unavailable)

## Phase 5 - Coordination and Reliable Processing

- [x] Transactional outbox for durable job publication
- [x] RabbitMQ durable queues, publisher confirms, manual acknowledgements, and dead-letter routing
- [x] Fenced PostgreSQL job and per-file work leases for multi-worker execution
- [x] Leased etcd storage membership and recovery-coordinator election
- [x] Reconciliation, integrity verification, replica repair, and soft-delete cleanup jobs
- [x] Unit coverage for duplicate delivery, retry, lease takeover, broker failure, and quorum loss
- [ ] Live PostgreSQL/RabbitMQ/etcd integration verification (Docker engine unavailable)

## Quick start

Prerequisites: Docker Desktop, Docker Compose, Python 3.11+, and Git.

```powershell
Copy-Item .env.example .env
```

Set `JWT_SECRET_KEY` and the database, storage, RabbitMQ, and etcd credentials in
`.env` to unique values before starting the services. Values in `.env.example`
are local-only placeholders, not production credentials. The etcd root password
and application service-account password must be different.

```powershell
docker compose up --build
```

Compose initializes the legacy schema for a new PostgreSQL volume, then runs the
Alembic upgrades before starting the API and worker. The worker publishes
transactional outbox events to RabbitMQ and consumes durable jobs. A leased etcd
election selects one worker for health refresh and periodic reconciliation;
PostgreSQL remains authoritative for files, replicas, jobs, and worker fencing.
The worker provisions configured node records and checksum-verifies legacy
objects on node 1 for upgrade compatibility.

The local stack exposes:

- Nginx gateway: `http://localhost:8080`
- FastAPI documentation: `http://localhost:8000/docs`
- Gateway health endpoint: `http://localhost:8080/api/health`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`

RabbitMQ, etcd, and MinIO endpoints have no published host ports. RabbitMQ and
the three etcd members use a worker-only internal Compose network; the etcd
bootstrap creates a root account and a separate least-privilege worker account.
MinIO nodes have separate persistent volumes. Node 1 reuses the previous
`minio_data` volume; nodes 2 and 3 use new volumes. The three etcd members and
storage containers still share one Docker host/daemon, so this is a development
simulation, not physical fault tolerance. Three etcd members require two for
quorum; losing quorum pauses leader-only maintenance, while workers continue
processing already-published jobs using PostgreSQL fencing.

The default replication factor and active storage-node count are both three.
The optional fourth MinIO endpoint is a spare with its own volume. To activate
it for replacement-node recovery, set `STORAGE_NODE_COUNT=4` in `.env` and run
`docker compose --profile spare up --build`. With only three active nodes,
service remains readable from survivors but is `DEGRADED` until the failed node
recovers or a replacement is activated.

Replication failures are recorded in PostgreSQL `replication_jobs` and replica
state/error fields. Admins can retry a dead job with
`POST /api/nodes/replication-jobs/{job_id}/retry`. The worker claims jobs using
PostgreSQL row locks, expiring leases, and fencing tokens. Job delivery is
at-least-once: the outbox relay marks an event published only after a RabbitMQ
confirm, and a crash between broker confirmation and the database update can
publish a duplicate. Consumers deduplicate through the PostgreSQL job row and
per-file lease. Bounded job attempts end in the database `DEAD` state and
dead-letter queue; outbox publication retries during a broker outage.

Stop with `docker compose down`. Add `-v` only when intentionally deleting
development data.

Public registration creates viewer accounts only. To administer storage nodes,
promote a trusted account directly in PostgreSQL:

```sql
UPDATE users SET role = 'admin' WHERE email = 'trusted-admin@example.com';
```

Local validation uses SQLite and in-memory storage adapters:

```powershell
.\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m compileall -q backend tests
docker compose --env-file .env.example config --quiet
```

These checks do not replace live integration tests. If the Docker daemon is
unavailable, Compose startup, PostgreSQL migrations, RabbitMQ delivery, etcd
leases/election, and real multi-node transfers remain unverified.

## Repository layout

See [docs/folder-structure.md](docs/folder-structure.md) for the target structure
and ownership split.
