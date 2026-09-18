package pgwire

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/plexus"
	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
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

type writeRecorderConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
}

func (c *writeRecorderConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	cp := make([]byte, len(b))
	copy(cp, b)
	c.writes = append(c.writes, cp)
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *writeRecorderConn) getWriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

func (c *writeRecorderConn) resetWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = nil
}

func TestPGWireBufferedOutputCoalescing(t *testing.T) {
	store, err := sqlstore.NewInMemory()
	if err != nil {
		t.Fatalf("NewInMemory failed: %v", err)
	}
	defer store.Close()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	recorder := &writeRecorderConn{Conn: serverConn}
	session := NewSession(recorder, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Serve(ctx)
	}()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)

	// 1. Handshake
	if err := frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "postgres", "database": "testdb"},
	}); err != nil {
		t.Fatalf("send startup message: %v", err)
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive during handshake failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// Handshake messages (AuthenticationOk, 5 ParameterStatus, BackendKeyData, ReadyForQuery)
	// should all be coalesced into 1 write!
	handshakeWrites := recorder.getWriteCount()
	if handshakeWrites != 1 {
		t.Errorf("expected 1 coalesced write for handshake, got %d", handshakeWrites)
	}

	// 2. Simple Query: SELECT 1
	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Query{String: "SELECT 1;"}); err != nil {
		t.Fatalf("send query: %v", err)
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive during query failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// RowDescription + DataRow + CommandComplete + ReadyForQuery must coalesce into 1 TCP write!
	queryWrites := recorder.getWriteCount()
	if queryWrites != 1 {
		t.Errorf("expected 1 coalesced write for simple query, got %d", queryWrites)
	}

	// 3. Error query: invalid SQL syntax or table
	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Query{String: "SELECT * FROM nonexistent_table;"}); err != nil {
		t.Fatalf("send invalid query: %v", err)
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive during error query failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// ErrorResponse + ReadyForQuery must coalesce into 1 TCP write!
	errWrites := recorder.getWriteCount()
	if errWrites != 1 {
		t.Errorf("expected 1 coalesced write for error query, got %d", errWrites)
	}

	// 4. Extended Query Protocol: Parse -> Bind -> Describe -> Execute -> Sync
	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Parse{Name: "stmt1", Query: "SELECT 1;"}); err != nil {
		t.Fatalf("send parse: %v", err)
	}
	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive parse response failed: %v", err)
	}
	if _, ok := msg.(*pgproto3.ParseComplete); !ok {
		t.Fatalf("expected ParseComplete, got %T", msg)
	}
	if parseWrites := recorder.getWriteCount(); parseWrites != 1 {
		t.Errorf("expected 1 write for ParseComplete, got %d", parseWrites)
	}

	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "stmt1"}); err != nil {
		t.Fatalf("send bind: %v", err)
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive bind response failed: %v", err)
	}
	if _, ok := msg.(*pgproto3.BindComplete); !ok {
		t.Fatalf("expected BindComplete, got %T", msg)
	}
	if bindWrites := recorder.getWriteCount(); bindWrites != 1 {
		t.Errorf("expected 1 write for BindComplete, got %d", bindWrites)
	}

	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Describe{ObjectType: 'P', Name: "p1"}); err != nil {
		t.Fatalf("send describe: %v", err)
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive describe response failed: %v", err)
	}
	if _, ok := msg.(*pgproto3.NoData); !ok {
		t.Fatalf("expected NoData, got %T", msg)
	}
	if descWrites := recorder.getWriteCount(); descWrites != 1 {
		t.Errorf("expected 1 write for Describe, got %d", descWrites)
	}

	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Execute{Portal: "p1"}); err != nil {
		t.Fatalf("send execute: %v", err)
	}
	for {
		msg, err = frontend.Receive()
		if err != nil {
			t.Fatalf("receive execute response failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.CommandComplete); ok {
			break
		}
	}
	if execWrites := recorder.getWriteCount(); execWrites != 1 {
		t.Errorf("expected 1 write for Execute, got %d", execWrites)
	}

	recorder.resetWrites()
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive sync response failed: %v", err)
	}
	if _, ok := msg.(*pgproto3.ReadyForQuery); !ok {
		t.Fatalf("expected ReadyForQuery, got %T", msg)
	}
	if syncWrites := recorder.getWriteCount(); syncWrites != 1 {
		t.Errorf("expected 1 write for Sync, got %d", syncWrites)
	}

	// 5. Terminate
	if err := frontend.Send(&pgproto3.Terminate{}); err != nil {
		t.Fatalf("send terminate: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected Serve error upon terminate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("session did not exit upon terminate")
	}
}
