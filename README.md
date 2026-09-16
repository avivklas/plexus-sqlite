# Plexus-SQLite: Distributed SQLite with PostgreSQL Wire Protocol

[![Go Version](https://img.shields.io/badge/go-1.22%2B-blue)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Plexus-SQLite** turns SQLite into a distributed, fault-tolerant relational database server powered by the [Plexus](https://github.com/avivklas/plexus) consensus framework.

It exposes a **PostgreSQL wire protocol (pgwire)** interface, enabling standard PostgreSQL tools (`psql`, `pgcli`, DBeaver, SQLAlchemy, Prisma, etc.) to connect transparently to a distributed SQLite cluster.

---

## Architecture

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
|  |   - Handshake (SSL negotiation, Startup, AuthOK)                             | |
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
|             | (Local Read)                                      | (Mutator.Apply)  |
|             v                                                   v                  |
|     +---------------+                                   +---------------+          |
|     | Local SQLite3 | <================================ | Plexus Cluster|          |
|     |  Engine (WAL) |   (FSM Apply after Quorum Commit) | & Raft Engine |          |
|     +---------------+                                   +---------------+          |
+------------------------------------------------------------------------------------+
```

### Consistency Model
- **Mutations & DDL (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`)**: Replicated across the Raft cluster using Plexus. Thanks to Plexus's **Quorum + Local Apply guarantee**, a write is only acknowledged (`CommandComplete`) after being committed by quorum **and** applied to the local SQLite database.
- **Reads (`SELECT`, `PRAGMA`, `EXPLAIN`)**: Executed directly on the local SQLite engine with zero network overhead ($O(1)$ latency). Because writes guarantee local application before returning, subsequent local reads are guaranteed to have **Read-Your-Own-Writes / Monotonic Read consistency**.
- **Snapshots & Restore**: State synchronization and log compaction use SQLite's native `VACUUM INTO` and WAL checkpoints.

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
