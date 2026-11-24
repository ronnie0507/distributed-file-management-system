# System Architecture

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
    D --> PG
    AC --> PG
    W[Replication Workers] --> PG
    W <--> MQ[RabbitMQ]
    W <--> ETCD[3-member etcd Cluster]
    PG --> O[Transactional Outbox]
    O --> MQ

    subgraph SC[Distributed Storage Cluster]
        SN1[Storage Node 1]
        SN2[Storage Node 2]
        SN3[Storage Node 3]
    end

    D --> SC
    W --> SC

    F --> R[Redis Cache]
    R --> PG
```

## Engine boundaries

| Engine | Owns | Reads/Writes |
|---|---|---|
| File Engine | Upload validation, SHA-256, versions, integrity | Object storage and metadata operations |
| Metadata Engine | File IDs, metadata, versions, replica references | PostgreSQL |
| Access Engine | JWT validation, RBAC, ownership, audit events | PostgreSQL and audit log |
| Distribution Engine | Placement, health, replication, availability | PostgreSQL node registry, storage nodes |
| Replication worker | Durable job execution, verification, repair, reconciliation | PostgreSQL, RabbitMQ, etcd, storage nodes |

## Infrastructure boundaries

- PostgreSQL is authoritative for file metadata, replica records, jobs, and outbox events.
- Redis is a cache and must never be treated as authoritative.
- MinIO or storage-node APIs hold file content only.
- RabbitMQ transports durable background-job notifications; PostgreSQL outbox events
  prevent committed work from being lost during broker outages.
- etcd coordinates leased storage membership and elects one recovery coordinator; it
  is not the metadata database. A three-member cluster needs two members for quorum.
- Nginx routes external traffic and later applies request limits and TLS termination.
