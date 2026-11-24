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

## Quick start

Prerequisites: Docker Desktop, Docker Compose, Python 3.11+, and Git.

```powershell
Copy-Item .env.example .env
```

Set `JWT_SECRET_KEY` and the database/storage credentials in `.env` to unique
values before starting the services. Values in `.env.example` are local-only
placeholders, not production credentials.

```powershell
docker compose up --build
```

Compose initializes the legacy schema for a new PostgreSQL volume, then runs the
Alembic Phase 4 upgrade before starting the API and replication worker. The
worker provisions configured node records, reports health, verifies legacy
objects on node 1 for upgrade compatibility, and processes durable replication
jobs.

The local stack exposes:

- Nginx gateway: `http://localhost:8080`
- FastAPI documentation: `http://localhost:8000/docs`
- Gateway health endpoint: `http://localhost:8080/api/health`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`

MinIO endpoints are private to the Compose network and have separate persistent
volumes. Node 1 reuses the previous `minio_data` volume; nodes 2 and 3 use new
volumes. The three containers still share one Docker host/daemon and therefore
are a development simulation, not physical fault domains. Production fault
tolerance requires independently managed hosts or storage services.

The default replication factor and active storage-node count are both three.
The optional fourth MinIO endpoint is a spare with its own volume. To activate
it for replacement-node recovery, set `STORAGE_NODE_COUNT=4` in `.env` and run
`docker compose --profile spare up --build`. With only three active nodes,
service remains readable from survivors but is `DEGRADED` until the failed node
recovers or a replacement is activated.

Replication failures are recorded in PostgreSQL `replication_jobs` and replica
state/error fields. Admins can retry a dead job with
`POST /api/nodes/replication-jobs/{job_id}/retry`. The worker claims jobs using
PostgreSQL row locks and expiring leases; RabbitMQ is intentionally deferred.

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

These checks do not replace live Postgres/MinIO integration tests. If the Docker
daemon is unavailable, Compose startup, migrations against PostgreSQL, and real
multi-node transfers remain unverified.

## Repository layout

See [docs/folder-structure.md](docs/folder-structure.md) for the target structure
and ownership split.
