# Development Task Breakdown

## Phase 1 - Foundation

| ID | Task | Exit condition |
|---|---|---|
| F1 | Architecture diagram | Reviewed and mapped to eight layers |
| F2 | Engine boundaries | Four engines have explicit responsibilities |
| F3 | Database schema | PostgreSQL initializes without errors |
| F4 | API contract | OpenAPI document covers planned endpoints |
| F5 | Docker foundation | Compose infrastructure starts |
| F6 | Node design | Health, placement, and failure policies documented |

## Phase 2 - Networking and API

| ID | Task | Exit condition |
|---|---|---|
| N1 | Configure Nginx gateway | Gateway proxies `/api` traffic to FastAPI |
| N2 | Build modular API routes | Health, file, and node routes are available |
| N3 | Add validation and errors | Invalid fields return structured 422 responses |
| N4 | Add API tests | Focused communication tests pass |
| N5 | Verify containers | Compose starts gateway and backend successfully |

## Phase 3 - Engine Development

| ID | Task | Exit condition |
|---|---|---|
| E1 | Metadata persistence models | ORM maps users, files, and immutable versions |
| E2 | Metadata operations | Owner-scoped create, list, lookup, soft delete, and state changes |
| E3 | File Engine | Content is checksummed and stored/retrieved through MinIO |
| E4 | Access Engine | API derives owner identity from verified credentials |
| E5 | Distribution Engine | Replicas are placed and tracked across healthy nodes |

E1-E3 have started. File API writes remain unwired until E4 provides a trusted
authenticated owner identity; accepting a client-supplied owner ID would be unsafe.

## Team ownership

| Member | Primary work | Shared area |
|---|---|---|
| Member 1 | FastAPI, engines, database | API and integration |
| Member 2 | Containers, metadata/storage nodes, etcd | Consistency and replication |
| Member 3 | React, dashboard, test automation | Monitoring and integration |
