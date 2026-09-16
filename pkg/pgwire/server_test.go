package pgwire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/avivklas/plexus"
	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
	"github.com/hashicorp/raft"
	"github.com/jackc/pgx/v5"
)

func TestPGWireWithPostgresClient(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-pgwire-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Setup in-memory sqlstore
	store, err := sqlstore.NewInMemory()
	if err != nil {
		t.Fatalf("NewInMemory sqlstore failed: %v", err)
	}
	defer store.Close()

	// 2. Setup Plexus cluster
	clusterCfg := plexus.DefaultClusterConfig("pg-node-1", "127.0.0.1:9393", filepath.Join(tmpDir, "cluster"))
	clusterCfg.Bootstrap = true
	clusterCfg.ApplyTimeout = 5 * time.Second

	c, err := plexus.NewCluster(clusterCfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	_, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:9393"))
	c.DefaultRaftMachine().WithCustomStores(
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		raft.NewInmemSnapshotStore(),
		trans,
	)

	mutator := c.UseState(store)
	store.AttachMutator(mutator)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Cluster Start failed: %v", err)
	}
	defer c.Stop()

	// Wait for leadership
	deadline := time.Now().Add(5 * time.Second)
	for !c.DefaultMachine().IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 3. Start pgwire server on random available port
	pgServer, err := NewServer("127.0.0.1:0", store)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer pgServer.Close()

	serverAddr := pgServer.Addr()
	t.Logf("pgwire listening on %s", serverAddr)

	ctx := context.Background()

	// 4. Connect using standard PostgreSQL client (pgx)
	connStr := fmt.Sprintf("postgres://postgres@%s/testdb?sslmode=disable", serverAddr)
	pgConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx.Connect to %s failed: %v", connStr, err)
	}
	defer pgConn.Close(ctx)

	// 5. Execute DDL via PostgreSQL protocol
	_, err = pgConn.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);`)
	if err != nil {
		t.Fatalf("CREATE TABLE via pgwire failed: %v", err)
	}

	// 6. Execute INSERT via PostgreSQL protocol (replicated via Plexus consensus)
	commandTag, err := pgConn.Exec(ctx, `INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');`)
	if err != nil {
		t.Fatalf("INSERT via pgwire failed: %v", err)
	}
	if commandTag.RowsAffected() != 1 {
		t.Errorf("expected 1 row affected, got %d", commandTag.RowsAffected())
	}

	// 7. Execute Query via PostgreSQL protocol (read locally with broken-CAP consistency)
	var id int
	var name, email string
	row := pgConn.QueryRow(ctx, `SELECT id, name, email FROM users WHERE id = 1;`)
	if err := row.Scan(&id, &name, &email); err != nil {
		t.Fatalf("SELECT Scan via pgwire failed: %v", err)
	}
	if id != 1 || name != "Alice" || email != "alice@example.com" {
		t.Errorf("unexpected row data: id=%d name=%s email=%s", id, name, email)
	}

	// 8. Parameterized Query (Extended Query Protocol)
	row2 := pgConn.QueryRow(ctx, `SELECT name FROM users WHERE id = $1;`, 1)
	var name2 string
	if err := row2.Scan(&name2); err != nil {
		t.Fatalf("parameterized query failed: %v", err)
	}
	if name2 != "Alice" {
		t.Errorf("expected Alice, got %s", name2)
	}
}
