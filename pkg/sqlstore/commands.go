package sqlstore

import (
	"github.com/avivklas/plexus"
)

const (
	// CmdExec executes a single mutating SQL statement.
	CmdExec plexus.CommandType = "sql.exec"

	// CmdBatch executes multiple SQL statements atomically.
	CmdBatch plexus.CommandType = "sql.batch"

	// CmdTxBegin begins an explicit transaction.
	CmdTxBegin plexus.CommandType = "sql.tx_begin"

	// CmdTxCommit commits an explicit transaction.
	CmdTxCommit plexus.CommandType = "sql.tx_commit"

	// CmdTxRollback rolls back an explicit transaction.
	CmdTxRollback plexus.CommandType = "sql.tx_rollback"
)

// ExecRequest represents a SQL statement and optional parameters to execute.
type ExecRequest struct {
	Query string `json:"query"`
	Args  []any  `json:"args,omitempty"`
}

// ExecResponse returns the result of an Exec operation.
type ExecResponse struct {
	RowsAffected int64  `json:"rows_affected"`
	LastInsertID int64  `json:"last_insert_id"`
	Tag          string `json:"tag,omitempty"` // e.g. "INSERT 0 1", "UPDATE 2"
}

// BatchRequest represents a series of SQL statements executed sequentially.
type BatchRequest struct {
	Statements []string `json:"statements"`
}

// BatchResponse returns results for a batch execution.
type BatchResponse struct {
	TotalRowsAffected int64 `json:"total_rows_affected"`
}

// TxRequest represents transaction lifecycle commands.
type TxRequest struct {
	TxID int64 `json:"tx_id"`
}
