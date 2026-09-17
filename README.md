# Plexus-SQLite: Distributed SQLite with PostgreSQL Wire Protocol

[![Go Version](https://img.shields.io/badge/go-1.27%2B-blue)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Plexus-SQLite** turns SQLite into a distributed, fault-tolerant relational database server powered by the [Plexus](https://github.com/avivklas/plexus) consensus framework.

It exposes a **PostgreSQL wire protocol (pgwire)** interface, enabling standard PostgreSQL tools (`psql`, `pgcli`, DBeaver, SQLAlchemy, Prisma, etc.) to connect transparently to a distributed SQLite cluster.

---

### Architecture

```
                               +-----------------------------+
                               |     PostgreSQL Client       |
                               |  (psql / pgx / DBeaver)     |
                               +--------------+--------------+
                                              |
                                   TCP :5432 (pgwire)
                                              v
+------------------------------------------------------------------------------------+
| Plexus-SQLite Server                                                               |
|                                                                                    |
|  +-------------------------------------------------------------------------------+ |
|  |                     PostgreSQL Wire Protocol Engine (pgwire)                  | |
|  |   - Handshake (SSL negotiation, Startup, AuthOK)                              | |
|  |   - Simple Query ('Q') & Extended Query ('P', 'B', 'E', 'S')                  | |
|  |   - SQL Classifier (Read vs. Write / DDL / Tx)                                | |
|  |   - Placeholder Rewriting ($1, $2 -> ?)                                       | |
|  +-----------------------------------+-------------------------------------------+ |
|                                      |                                             |
|                     [Read: SELECT]   |   [Write: INSERT/UPDATE/DDL/Tx]             |
|                                      v                                             |
|                       +-------------------------------+                            |
|                       |       sqlstore (Store)        |                            |
|                       +---------------+---------------+                            |
|                                       |                                            |
|             +-------------------------+-------------------------+                  |
|             | (Local In-Memory Read)                            | (Mutator.Apply)  |
|             v                                                   v                  |
|     +---------------+                                   +---------------+          |
|     | In-Memory DB  | <================================ | Plexus Cluster|          |
|     | (Shared Cache)|   (FSM Apply after Quorum Commit) | & Raft Engine |          |
|     +---------------+                                   +---------------+          |
|                                                                 |                  |
|                                                                 v                  |
|                                                         +---------------+          |
|                                                         | Raft LogStore |          |
|                                                         |  (Direct-I/O) |          |
|                                                         +---------------+          |
+------------------------------------------------------------------------------------+
```

### Key Design Principles
- **In-Memory SQLite Engine (`in mem`)**: By default, SQLite is opened in memory using URI shared cache (`file:plexus_mem?mode=memory&cache=shared`). Reads and queries run entirely in memory with microsecond latencies.
- **Raft Log Durability**: State durability is guaranteed by the underlying Plexus Raft cluster and its Direct-I/O SegmentLogStore. Upon node bootstrap or rejoin, the in-memory SQLite instance is hydrated automatically from the Raft snapshot and log replay.
- **Consistency Model**:
  - **Mutations & DDL (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`)**: Replicated across the Raft cluster using Plexus. Writes are only acknowledged (`CommandComplete`) after being committed by quorum **and** applied to the local in-memory SQLite database.
  - **Reads (`SELECT`, `PRAGMA`, `EXPLAIN`)**: Executed directly against the local in-memory SQLite engine with zero network overhead ($O(1)$ latency) and guaranteed **Read-Your-Own-Writes / Monotonic Read consistency**.
  - **Snapshots & Restore**: Cluster snapshot synchronization uses SQLite's native `VACUUM INTO` and online backup engine (`sqlite3_backup`).

---

## Quickstart

### 1. Build the Daemon

```bash
cd /Users/avivk/dev/github.com/avivklas/plexus-sqlite
go build -o dsqlite ./cmd/dsqlite
```

### 2. Start a Multi-Node Cluster

**Node 1 (Bootstrap Leader):**
```bash
./dsqlite \
  --node-id node-1 \
  --bind-addr 127.0.0.1:9000 \
  --pg-addr 127.0.0.1:5432 \
  --data-dir ./data/node-1 \
  --bootstrap
```

**Node 2 (Follower Auto-Join):**
```bash
./dsqlite \
  --node-id node-2 \
  --bind-addr 127.0.0.1:9001 \
  --pg-addr 127.0.0.1:5433 \
  --data-dir ./data/node-2 \
  --join 127.0.0.1:9000
```

### 3. Connect via `psql`

Connect to any node using the standard PostgreSQL CLI:

```bash
psql -h 127.0.0.1 -p 5432 -U postgres
```

Run standard SQL commands:

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL
);

INSERT INTO products (id, name, price) VALUES (1, 'Mechanical Keyboard', 129.99);
INSERT INTO products (id, name, price) VALUES (2, 'Ergonomic Mouse', 79.50);

SELECT * FROM products;
```

---

## Running Tests

Run the test suite with the Go race detector enabled:

```bash
go test -v -race ./...
```
