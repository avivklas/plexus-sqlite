package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/avivklas/plexus"
)

// Store wraps an embedded SQLite database and replicates mutating statements via Plexus Raft.
type Store struct {
	plexus.BaseStore
	mu      sync.RWMutex
	db      *DB
	mutator plexus.Mutator
}

// New creates a new Store backed by the SQLite database at the specified path.
func New(dbPath string) (*Store, error) {
	sqliteDB, err := OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	s := &Store{
		BaseStore: plexus.NewBaseStore(),
		db:        sqliteDB,
	}

	// Register Raft mutating handlers
	plexus.Handle(s.Router(), CmdExec, s.handleExec)
	plexus.Handle(s.Router(), CmdBatch, s.handleBatch)

	return s, nil
}

// ID implements plexus.Store.
func (s *Store) ID() plexus.StoreID {
	return "sql"
}

// AttachMutator attaches the Plexus cluster mutator to this store.
func (s *Store) AttachMutator(m plexus.Mutator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mutator = m
}

// DB returns the underlying DB.
func (s *Store) DB() *DB {
	return s.db
}

func (s *Store) handleExec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	res, err := s.db.Exec(ctx, req.Query, req.Args...)
	if err != nil {
		return ExecResponse{}, err
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	tag := formatCommandTag(req.Query, rowsAffected)

	return ExecResponse{
		RowsAffected: rowsAffected,
		LastInsertID: lastInsertID,
		Tag:          tag,
	}, nil
}

func (s *Store) handleBatch(ctx context.Context, req BatchRequest) (BatchResponse, error) {
	tx, err := s.db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return BatchResponse{}, fmt.Errorf("begin batch tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	var total int64
	for _, stmt := range req.Statements {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		res, err := tx.ExecContext(ctx, trimmed)
		if err != nil {
			return BatchResponse{}, fmt.Errorf("batch stmt %q failed: %w", trimmed, err)
		}
		ra, _ := res.RowsAffected()
		total += ra
	}

	if err := tx.Commit(); err != nil {
		return BatchResponse{}, fmt.Errorf("commit batch: %w", err)
	}
	tx = nil

	return BatchResponse{
		TotalRowsAffected: total,
	}, nil
}

// Exec replicates a mutating SQL statement across the Plexus cluster.
// With Plexus's Quorum + Local Apply guarantee, this method only returns after commitment to quorum
// AND application to this node's local SQLite database.
func (s *Store) Exec(ctx context.Context, query string, args ...any) (*ExecResponse, error) {
	s.mu.RLock()
	m := s.mutator
	s.mu.RUnlock()

	if m == nil {
		return nil, fmt.Errorf("store not attached to cluster mutator")
	}

	res, err := m.Apply(ctx, CmdExec, ExecRequest{
		Query: query,
		Args:  args,
	})
	if err != nil {
		return nil, err
	}

	if resp, ok := res.(ExecResponse); ok {
		return &resp, nil
	}
	if respPtr, ok := res.(*ExecResponse); ok {
		return respPtr, nil
	}

	return nil, fmt.Errorf("unexpected exec response type: %T", res)
}

// Batch replicates multiple statements in an atomic transaction across the cluster.
func (s *Store) Batch(ctx context.Context, stmts []string) (*BatchResponse, error) {
	s.mu.RLock()
	m := s.mutator
	s.mu.RUnlock()

	if m == nil {
		return nil, fmt.Errorf("store not attached to cluster mutator")
	}

	res, err := m.Apply(ctx, CmdBatch, BatchRequest{Statements: stmts})
	if err != nil {
		return nil, err
	}

	if resp, ok := res.(BatchResponse); ok {
		return &resp, nil
	}
	if respPtr, ok := res.(*BatchResponse); ok {
		return respPtr, nil
	}

	return nil, fmt.Errorf("unexpected batch response type: %T", res)
}

// Query executes a read-only query directly against the local SQLite database.
// Because Plexus ensures all previously acknowledged mutations on this node have been applied locally,
// this read has immediate monotonic consistency without any network round-trip.
func (s *Store) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.Query(ctx, query, args...)
}

// QueryRow executes a single-row read-only query on the local SQLite database.
func (s *Store) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRow(ctx, query, args...)
}

// Snapshot implements plexus.Store by creating a WAL checkpoint and serializing the SQLite file.
func (s *Store) Snapshot() ([]byte, error) {
	return s.db.Backup(context.Background())
}

// Restore implements plexus.Store by restoring the SQLite database from snapshot bytes.
func (s *Store) Restore(data []byte) error {
	return s.db.Restore(context.Background(), data)
}

// Close closes the underlying SQLite database.
func (s *Store) Close() error {
	return s.db.Close()
}

func formatCommandTag(query string, rowsAffected int64) string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upper, "INSERT"):
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case strings.HasPrefix(upper, "UPDATE"):
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case strings.HasPrefix(upper, "DELETE"):
		return fmt.Sprintf("DELETE %d", rowsAffected)
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(upper, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return "ALTER TABLE"
	case strings.HasPrefix(upper, "CREATE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(upper, "DROP INDEX"):
		return "DROP INDEX"
	default:
		words := strings.Fields(upper)
		if len(words) > 0 {
			return words[0]
		}
		return "OK"
	}
}
