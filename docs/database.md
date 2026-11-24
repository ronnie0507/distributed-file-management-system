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
    FILES ||--o{ PERMISSIONS : controlled_by
    STORAGE_NODES ||--o{ FILE_REPLICAS : stores
    STORAGE_NODES ||--o{ NODE_HEALTH : reports

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
        uuid node_id FK
        text object_key
        string status
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
- Deduplicated content is deleted only after every owner reference is removed.
- Permissions are checked against PostgreSQL even when Redis metadata is present.
