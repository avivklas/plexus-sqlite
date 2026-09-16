package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"modernc.org/sqlite"
)

var memCounter uint64

// backuper is implemented by modernc.org/sqlite connection.
type backuper interface {
	NewRestore(string) (*sqlite.Backup, error)
}

// DB wraps an embedded SQLite database (in-memory or on-disk).
type DB struct {
	mu       sync.RWMutex
	db       *sql.DB
	filePath string
	dsn      string
	inMemory bool
}

// OpenDB opens or creates a SQLite database.
// If path is empty, ":memory:", or has mode=memory, an in-memory database with shared cache is created.
// Otherwise, an on-disk database with WAL mode is opened.
func OpenDB(path string) (*DB, error) {
	var (
		dsn      string
		inMemory bool
	)

	if path == "" || path == ":memory:" {
		inMemory = true
		memName := fmt.Sprintf("plexus_mem_%d_%d", os.Getpid(), atomic.AddUint64(&memCounter, 1))
		dsn = fmt.Sprintf("file:%s?mode=memory&cache=shared&_pragma=foreign_keys(ON)&_pragma=busy_timeout(10000)", memName)
	} else if strings.HasPrefix(path, "file:") && strings.Contains(path, "mode=memory") {
		inMemory = true
		dsn = path
	} else {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
		dsn = fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	}

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	// Set connection pool limits:
	// For shared in-memory SQLite, keeping idle connections alive prevents database disposal.
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}

	return &DB{
		db:       sqlDB,
		filePath: path,
		dsn:      dsn,
		inMemory: inMemory,
	}, nil
}

// OpenInMemoryDB creates an in-memory SQLite database with shared cache.
func OpenInMemoryDB() (*DB, error) {
	return OpenDB(":memory:")
}

// SQL returns the underlying *sql.DB instance.
func (d *DB) SQL() *sql.DB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db
}

// IsInMemory returns true if the database is running in memory.
func (d *DB) IsInMemory() bool {
	return d.inMemory
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

// Checkpoint forces a WAL truncation checkpoint (or is a no-op on in-memory DB).
func (d *DB) Checkpoint(ctx context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, err := d.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")
	return err
}

// Backup creates a byte slice snapshot of the database using VACUUM INTO.
// Works identically for both on-disk and in-memory databases.
func (d *DB) Backup(ctx context.Context) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tmpDir, err := os.MkdirTemp("", "plexus-sqlite-backup-*")
	if err != nil {
		return nil, fmt.Errorf("create backup temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tmpPath := filepath.Join(tmpDir, "backup.db")
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
// For in-memory databases, it uses the SQLite online backup/restore API.
// For on-disk databases, it writes the file and reopens the connection pool.
func (d *DB) Restore(ctx context.Context, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tmpDir, err := os.MkdirTemp("", "plexus-sqlite-restore-*")
	if err != nil {
		return fmt.Errorf("create restore temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tmpPath := filepath.Join(tmpDir, "restore.db")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write restore temp file: %w", err)
	}

	if d.inMemory {
		conn, err := d.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("get db connection for restore: %w", err)
		}
		defer conn.Close()

		err = conn.Raw(func(driverConn any) error {
			b, ok := driverConn.(backuper)
			if !ok {
				return fmt.Errorf("driverConn does not implement backuper (%T)", driverConn)
			}
			bck, err := b.NewRestore(tmpPath)
			if err != nil {
				return fmt.Errorf("init online restore: %w", err)
			}
			for more := true; more; {
				more, err = bck.Step(-1)
				if err != nil {
					_ = bck.Finish()
					return fmt.Errorf("restore step: %w", err)
				}
			}
			return bck.Finish()
		})
		if err != nil {
			return fmt.Errorf("restore in-memory db: %w", err)
		}
		return nil
	}

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
	newDB, err := sql.Open("sqlite", d.dsn)
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
