# Distributed File Metadata Access and Management System

This repository implements the project blueprint in phases, beginning with
architecture, database, container infrastructure, and the API contract.

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
- [ ] Physical replication and repair across independent storage nodes

## Quick start

Prerequisites: Docker Desktop, Docker Compose, Python 3.11+, and Git.

```powershell
Copy-Item .env.example .env
```

Set `JWT_SECRET_KEY` in `.env` to a unique random value of at least 32 characters
before starting the services. The example database and MinIO passwords are for
local development only.

```powershell
docker compose up --build
```

The foundation exposes:

- Nginx gateway: `http://localhost:8080`
- FastAPI documentation: `http://localhost:8000/docs`
- Gateway health endpoint: `http://localhost:8080/api/health`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`
- MinIO console: `http://localhost:9001`

Stop with `docker compose down`. Add `-v` only when deleting development data.

Public registration creates viewer accounts only. To administer storage nodes,
promote a trusted account directly in PostgreSQL:

```sql
UPDATE users SET role = 'admin' WHERE email = 'trusted-admin@example.com';
```

The consistent-hash placement policy selects targets, but actual cross-node replica
transfers and repair workers are not implemented yet.

## Repository layout

See [docs/folder-structure.md](docs/folder-structure.md) for the target structure
and ownership split.
