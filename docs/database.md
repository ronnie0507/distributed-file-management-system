# Database ER Diagram

```mermaid
erDiagram
    USERS ||--o{ FILES : owns
    USERS ||--o{ PERMISSIONS : receives
    USERS ||--o{ FILE_VERSIONS : creates
    USERS ||--o{ AUDIT_LOGS : generates
    USERS ||--o{ METADATA_OPERATIONS : initiates
    FILES ||--o{ FILE_VERSIONS : has
    FILES ||--o{ FILE_REPLICAS : has
    FILE_VERSIONS ||--o{ FILE_REPLICAS : stores
    FILE_VERSIONS ||--o{ REPLICATION_JOBS : schedules
    REPLICATION_JOBS ||--o{ OUTBOX_EVENTS : dispatches
    FILES ||--o{ PERMISSIONS : controlled_by
    STORAGE_NODES ||--o{ FILE_REPLICAS : stores
    STORAGE_NODES ||--o{ NODE_HEALTH : reports
    FILES ||--o{ REPLICATION_JOBS : schedules

    USERS {
        uuid user_id PK
        string username UK
        string email UK
        text password_hash
        string role
    }
    FILES {
        uuid file_id PK
        uuid owner_id FK
        string filename
        char checksum
        bigint file_size
        int current_version
        string upload_state
        string availability_state
        int replication_factor
    }
    FILE_VERSIONS {
        uuid version_id PK
        uuid file_id FK
        int version_number
        char checksum
        bigint file_size
    }
    STORAGE_NODES {
        uuid node_id PK
        string node_name UK
        text node_address UK
        string status
    }
    FILE_REPLICAS {
        uuid replica_id PK
        uuid file_id FK
        uuid version_id FK
        uuid node_id FK
        text object_key
        char checksum
        string status
    }
    REPLICATION_JOBS {
        uuid job_id PK
        uuid file_id FK
        uuid version_id FK
        string job_type
        string idempotency_key UK
        string status
        int attempt_count
        datetime locked_until
        uuid lock_token
        int dispatch_generation
    }
    OUTBOX_EVENTS {
        uuid event_id PK
        uuid job_id FK
        string event_key UK
        json payload
        string status
        int attempt_count
        datetime available_at
        datetime locked_until
        datetime published_at
    }
    PERMISSIONS {
        uuid permission_id PK
        uuid user_id FK
        uuid file_id FK
        string role
    }
```

## Invariants

- File IDs are globally unique UUIDs.
- Version numbers are unique within a file and immutable after commit.
- `checksum` is a lowercase SHA-256 hexadecimal string.
- Replica records identify content independently of user ownership.
- A replica is downloadable only in `VERIFIED` state with a fresh healthy node heartbeat.
- Replica records are unique per file version and storage node.
- Replication jobs are durable, idempotent, leased, fenced, and bounded by a retry limit.
- Job state and its outbox event are committed in the same PostgreSQL transaction.
- Outbox events are published at least once; consumers deduplicate by the PostgreSQL job.
- File-level work leases serialize different job types for the same file.
- Deduplicated content is deleted only after every owner reference is removed.
- Permissions are checked against PostgreSQL even when Redis metadata is present.

Phase 4 and 5 schema changes are applied by Alembic after the baseline `schema.sql`
has initialized a clean PostgreSQL volume. The Phase 5 migration adds typed and
fenced jobs, per-file work leases, and the transactional outbox. Legacy replica
rows are migrated to an unverified state; the worker only backfills legacy objects
after checksum validation.
