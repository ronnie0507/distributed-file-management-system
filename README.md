<<<<<<< HEAD
# Distributed File Metadata Access and Management System

This repository implements the blueprint as a phased project. It starts with Phase 1:
architecture, database, container infrastructure, API contract, and development task
breakdown.

## Phase 1 status

- [x] Architecture and engine responsibilities
- [x] PostgreSQL metadata schema
- [x] Docker Compose infrastructure
- [x] API specification
- [x] Node communication and failure design
- [x] Initial FastAPI project skeleton
- [ ] Application implementation (Phase 2)

## Phase 2 status

- [x] Nginx reverse proxy and request limits
- [x] Modular FastAPI route structure
- [x] Structured validation and HTTP errors
- [x] Planned file and node API endpoints
- [ ] Container and integration verification

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

Stop the environment with `docker compose down`. Add `-v` only when deleting development data.

## Repository layout

See `docs/folder-structure.md` for the target structure and ownership split.
=======
# distributed-file-management-system
A distributed file metadata access and management system featuring centralized API services, multiple metadata and storage nodes, intelligent file distribution, replication, fault tolerance, data integrity, and secure access control.
>>>>>>> 5b9f903fdf626524408b482f1313dc70f651449c
