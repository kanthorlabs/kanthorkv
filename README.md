# KanthorKV
> Key-Value database implementation based on database research at KanthorLabs

## Guideline

1. Introduction & Requirements

    - Define the scope of the key-value database.
    - Identify functional and non-functional requirements.
    - Decide on supported data types and API structure (CRUD operations).

2. Data Structures for Storage

    - Choose in-memory storage (e.g., Go maps, slices, or skip lists).
    - Implement a basic in-memory key-value store.
    - Consider data serialization for persistence.

3. Persistence Mechanism

    - Design a log-structured storage (write-ahead log or append-only file).
    - Implement a basic file-based storage system.
    - Use binary encoding (e.g., Gob, JSON, or Protocol Buffers).

4. Indexing Strategies

    - Implement basic key lookup using in-memory indexes.
    - Introduce B-Trees, LSM trees, or Hash indexing.
    - Discuss trade-offs between read and write performance.

5. Concurrency Control

    - Use mutexes, RWLocks, or channels for thread-safe access.
    - Implement multi-version concurrency control (MVCC) if necessary.

6. Transactions & ACID Properties

    - Implement atomic operations.
    - Ensure durability with Write-Ahead Logging (WAL).
    - Consider implementing snapshot isolation.

7. Compaction & Garbage Collection

    - Handle log compaction for efficient storage.
    - Implement a merge process for LSM trees or B-Trees.

8. Networking & Client API

    - Design a simple TCP or HTTP server.
    - Implement gRPC or REST API for external clients.
    - Handle connection pooling and request handling.

9. Replication & Distribution (Optional)

    - Implement master-slave or peer-to-peer replication.
    - Use Raft or Paxos for consensus in a distributed system.

10. Testing & Benchmarking

    - Write unit tests for key components.
    - Benchmark reads, writes, and concurrency performance.
    - Compare performance with existing key-value stores.

## Diary

- 2025-03-01: [Implement page and block_id](docs/diary/2025-03-01.md)
- 2025-03-09: [Implement file manager](docs/diary/2025-03-09.md)
- 2025-05-03: [Database memory management](docs/diary/2025-05-03.md)
