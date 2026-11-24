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
- [ ] Authenticated API integration (Access Engine prerequisite)
- [x] MinIO streaming content adapter with SHA-256 and size bounds (File Engine)
- [ ] Storage-node placement and replication (Distribution Engine)

## Quick start

Prerequisites: Docker Desktop, Docker Compose, Python 3.11+, and Git.

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

## Repository layout

See [docs/folder-structure.md](docs/folder-structure.md) for the target structure
and ownership split.
