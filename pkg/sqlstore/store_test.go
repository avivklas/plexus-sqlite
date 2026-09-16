package sqlstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/avivklas/plexus"
	"github.com/hashicorp/raft"
)

func TestSQLStoreWithPlexusCluster(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-sqlite-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New sqlstore failed: %v", err)
	}
	defer store.Close()

	// Setup single-process Plexus cluster
	clusterCfg := plexus.DefaultClusterConfig("sql-node-1", "127.0.0.1:9292", filepath.Join(tmpDir, "cluster"))
	clusterCfg.Bootstrap = true
	clusterCfg.ApplyTimeout = 5 * time.Second

	c, err := plexus.NewCluster(clusterCfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	// In-memory Raft backends for fast test execution
	_, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:9292"))
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

	ctx := context.Background()

	// 1. DDL: Create Table
	createTableSQL := `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);`
	res, err := store.Exec(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Exec CREATE TABLE failed: %v", err)
	}
	if res.Tag != "CREATE TABLE" {
		t.Errorf("expected tag CREATE TABLE, got %s", res.Tag)
	}

	// 2. DML: Insert
	insertSQL := `INSERT INTO items (id, name, price) VALUES (1, 'Widget', 19.99);`
	res, err = store.Exec(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Exec INSERT failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", res.RowsAffected)
	}

	// 3. Local Read: Query
	rows, err := store.Query(ctx, `SELECT id, name, price FROM items WHERE id = ?;`, 1)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected 1 row returned")
	}

	var id int
	var name string
	var price float64
	if err := rows.Scan(&id, &name, &price); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if id != 1 || name != "Widget" || price != 19.99 {
		t.Errorf("unexpected row data: id=%d name=%s price=%f", id, name, price)
	}

	// 4. Batch Execution
	batchStmts := []string{
		`INSERT INTO items (id, name, price) VALUES (2, 'Gadget', 49.95);`,
		`INSERT INTO items (id, name, price) VALUES (3, 'Doohickey', 9.99);`,
		`UPDATE items SET price = 24.99 WHERE id = 1;`,
	}
	batchRes, err := store.Batch(ctx, batchStmts)
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}
	if batchRes.TotalRowsAffected != 3 {
		t.Errorf("expected 3 total rows affected, got %d", batchRes.TotalRowsAffected)
	}

	// 5. Snapshot & Restore verification
	snap, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(snap) == 0 {
		t.Fatalf("expected non-empty snapshot")
	}

	restorePath := filepath.Join(tmpDir, "restore.db")
	restoredStore, err := New(restorePath)
	if err != nil {
		t.Fatalf("New restored store failed: %v", err)
	}
	defer restoredStore.Close()

	if err := restoredStore.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	var count int
	err = restoredStore.QueryRow(ctx, "SELECT COUNT(*) FROM items;").Scan(&count)
	if err != nil || count != 3 {
		t.Fatalf("expected 3 items in restored db, got count=%d err=%v", count, err)
	}
}
