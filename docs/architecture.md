# Phase 1 Architecture

## Logical layers

```mermaid
flowchart TB
    C[React Client / API Consumer] --> G[Nginx API Gateway]
    G --> A[FastAPI Application]

    subgraph E[Four Core Engines]
        F[File Engine]
        M[Metadata Engine]
        AC[Access Engine]
        D[Distribution Engine]
    end

    A --> F
    A --> M
    A --> AC
    A --> D

    M --> PG[(PostgreSQL)]
    D --> ETCD[(etcd Cluster)]
    AC --> PG
    D --> H[Node Health Registry]

    subgraph MC[Metadata Service Cluster]
        MN1[Metadata Node 1 / Leader]
        MN2[Metadata Node 2 / Follower]
        MN3[Metadata Node 3 / Follower]
    end

    M --> MC
    MC --> PG

    subgraph SC[Distributed Storage Cluster]
        SN1[Storage Node 1]
        SN2[Storage Node 2]
        SN3[Storage Node 3]
    end

    D --> SC

    F --> R[Redis Cache]
    R --> PG
```

## Engine boundaries

| Engine | Owns | Reads/Writes |
|---|---|---|
| File Engine | Upload validation, SHA-256, versions, integrity | Object storage and metadata operations |
| Metadata Engine | File IDs, metadata, versions, replica references | PostgreSQL |
| Access Engine | JWT validation, RBAC, ownership, audit events | PostgreSQL and audit log |
| Distribution Engine | Placement, health, replication, leader coordination | Node registry, storage nodes, etcd |

## Infrastructure boundaries

- PostgreSQL is the authoritative metadata store for the initial implementation.
- Redis is a cache and must never be treated as authoritative.
- MinIO or storage-node APIs hold file content only.
- etcd coordinates metadata leader election; it is not the metadata database.
- Nginx routes external traffic and later applies request limits and TLS termination.
