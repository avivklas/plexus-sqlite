package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DB wraps an embedded SQLite database in WAL mode.
type DB struct {
	mu       sync.RWMutex
	db       *sql.DB
	filePath string
}

// OpenDB opens or creates a SQLite database at the specified path with WAL mode configured.
func OpenDB(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	// Set connection pool limits: SQLite in WAL mode works best with 1 writer or bounded connections
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}

	return &DB{
		db:       sqlDB,
		filePath: path,
	}, nil
}

// SQL returns the underlying *sql.DB instance.
func (d *DB) SQL() *sql.DB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db
}

// Query executes a query on the local database.
func (d *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query expected to return a single row.
func (d *DB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a mutation statement on the database.
func (d *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db.ExecContext(ctx, query, args...)
}

// Checkpoint forces a WAL truncation checkpoint so the main database file has all current changes.
func (d *DB) Checkpoint(ctx context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, err := d.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")
	return err
}

// Backup creates a byte slice snapshot of the database file using VACUUM INTO.
func (d *DB) Backup(ctx context.Context) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tmpPath := fmt.Sprintf("%s.backup.%d.tmp", d.filePath, time.Now().UnixNano())
	defer os.Remove(tmpPath)

	escapedPath := strings.ReplaceAll(tmpPath, "'", "''")
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s';", escapedPath)); err != nil {
		return nil, fmt.Errorf("vacuum into failed: %w", err)
	}

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("read vacuum backup: %w", err)
	}
	return data, nil
}

// Restore resets the database from the provided snapshot bytes.
func (d *DB) Restore(ctx context.Context, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Close existing connection pool
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("close db during restore: %w", err)
	}

	// 2. Remove WAL and SHM files if they exist
	_ = os.Remove(d.filePath + "-wal")
	_ = os.Remove(d.filePath + "-shm")

	// 3. Overwrite main database file
	if err := os.WriteFile(d.filePath, data, 0644); err != nil {
		return fmt.Errorf("write restored db file: %w", err)
	}

	// 4. Reopen database connection
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", d.filePath)
	newDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("reopen restored sqlite db: %w", err)
	}
	newDB.SetMaxOpenConns(10)
	newDB.SetMaxIdleConns(5)

	if err := newDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping restored db: %w", err)
	}

	d.db = newDB
	return nil
}

// Close closes the underlying database.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}
