package pgwire

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
	"github.com/jackc/pgproto3/v2"
)


const defaultBufferSize = 65536 // 64KB buffer for coalescing pgwire packets

type preparedStmt struct {
	Query          string
	ConvertedQuery string
	QueryType      QueryType
}

type boundPortal struct {
	Query          string
	ConvertedQuery string
	QueryType      QueryType
	Args           []any
}

// Session handles an individual PostgreSQL client connection.
type Session struct {
	conn       net.Conn
	bufWriter  *bufio.Writer
	backend    *pgproto3.Backend
	store      *sqlstore.Store
	readConn   *sql.Conn            // dedicated SQLite connection for this session's reads
	readStmts  map[string]*sql.Stmt // per-connection prepared statement cache
	statements map[string]preparedStmt
	portals    map[string]boundPortal
	txStatus   byte // 'I' = Idle, 'T' = In transaction
}

// NewSession initializes a client session over a net.Conn.
func NewSession(conn net.Conn, store *sqlstore.Store) *Session {
	bufWriter := bufio.NewWriterSize(conn, defaultBufferSize)
	// Acquire a dedicated read connection for this session to avoid per-query pool checkout overhead.
	// Falls back gracefully to the pool if unavailable (readConn stays nil).
	readConn, _ := store.AcquireReadConn(context.Background())
	return &Session{
		conn:       conn,
		bufWriter:  bufWriter,
		backend:    pgproto3.NewBackend(pgproto3.NewChunkReader(conn), bufWriter),
		store:      store,
		readConn:   readConn,
		readStmts:  make(map[string]*sql.Stmt),
		statements: make(map[string]preparedStmt),
		portals:    make(map[string]boundPortal),
		txStatus:   'I',
	}
}


// Serve runs the handshake and main command loop until client terminates or disconnects.
func (s *Session) Serve(ctx context.Context) error {
	defer s.conn.Close()
	defer func() {
		_ = s.bufWriter.Flush()
	}()
	defer s.closeReadConn()

	if err := s.handshake(); err != nil {
		return err
	}

	return s.queryLoop(ctx)
}

// closeReadConn closes all per-session prepared statements and returns the connection to the pool.
func (s *Session) closeReadConn() {
	for _, stmt := range s.readStmts {
		_ = stmt.Close()
	}
	s.readStmts = nil
	if s.readConn != nil {
		_ = s.readConn.Close()
		s.readConn = nil
	}
}

// getOrPrepareOnConn returns a prepared statement for query on the session's dedicated read connection.
// Falls back to the pool-level prepared statement if the connection is unavailable.
func (s *Session) getOrPrepareOnConn(ctx context.Context, query string) (*sql.Stmt, error) {
	if s.readConn == nil {
		return nil, fmt.Errorf("no dedicated read connection")
	}
	if stmt, ok := s.readStmts[query]; ok {
		return stmt, nil
	}
	stmt, err := s.readConn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(s.readStmts) < 64 {
		s.readStmts[query] = stmt
	}
	return stmt, nil
}


func (s *Session) handshake() error {
	for {
		startupMsg, err := s.backend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("receive startup msg: %w", err)
		}

		switch startupMsg.(type) {
		case *pgproto3.SSLRequest:
			// Respond with 'N' to decline SSL
			if _, err := s.conn.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("decline ssl: %w", err)
			}
			if err := s.bufWriter.Flush(); err != nil {
				return fmt.Errorf("flush ssl decline: %w", err)
			}
			continue
		case *pgproto3.StartupMessage:
			// Complete authentication and parameters
			s.backend.Send(&pgproto3.AuthenticationOk{})
			s.backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
			s.backend.Send(&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"})
			s.backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
			s.backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
			s.backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
			s.backend.Send(&pgproto3.BackendKeyData{ProcessID: 1000, SecretKey: 12345})
			if err := s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus}); err != nil {
				return fmt.Errorf("send ready for query: %w", err)
			}
			if err := s.bufWriter.Flush(); err != nil {
				return fmt.Errorf("flush startup messages: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("unexpected startup msg: %T", startupMsg)
		}
	}
}

func (s *Session) queryLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := s.backend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			s.handleSimpleQuery(ctx, v.String)
		case *pgproto3.Parse:
			s.handleParse(v)
		case *pgproto3.Bind:
			s.handleBind(v)
		case *pgproto3.Describe:
			s.handleDescribe(ctx, v)
		case *pgproto3.Execute:
			s.handleExecute(ctx, v)
		case *pgproto3.Sync:
			s.handleSync()
		case *pgproto3.Terminate:
			return nil
		default:
			// Unhandled message type, send error
			s.sendError(fmt.Sprintf("unsupported message type: %T", msg))
		}

		if err := s.bufWriter.Flush(); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("flush: %w", err)
		}
	}
}

func (s *Session) handleSync() {
	s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
}

func (s *Session) handleSimpleQuery(ctx context.Context, sqlText string) {
	qType := Classify(sqlText)

	switch qType {
	case QueryTypeEmpty:
		s.backend.Send(&pgproto3.EmptyQueryResponse{})
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})

	case QueryTypeSet:
		s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})

	case QueryTypeTxBegin:
		s.txStatus = 'T'
		s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})

	case QueryTypeTxCommit:
		s.txStatus = 'I'
		s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})

	case QueryTypeTxRollback:
		s.txStatus = 'I'
		s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})

	case QueryTypeRead:
		s.executeReadQuery(ctx, ConvertPlaceholders(sqlText), nil, true)

	case QueryTypeWrite:
		s.executeWriteQuery(ctx, ConvertPlaceholders(sqlText), nil, true)

	default:
		// Try as read query first, fallback to write
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "SELECT") {
			s.executeReadQuery(ctx, ConvertPlaceholders(sqlText), nil, true)
		} else {
			s.executeWriteQuery(ctx, ConvertPlaceholders(sqlText), nil, true)
		}
	}
}

func (s *Session) executeReadQuery(ctx context.Context, query string, args []any, sendReady bool) {
	// Fast path: use dedicated per-session connection + per-connection stmt cache
	// to avoid pool checkout and pool-level mutex on every read.
	var rows *sql.Rows
	var err error
	if stmt, perConnErr := s.getOrPrepareOnConn(ctx, query); perConnErr == nil {
		rows, err = stmt.QueryContext(ctx, args...)
	} else {
		rows, err = s.store.Query(ctx, query, args...)
	}
	if err != nil {
		s.sendError(err.Error())
		return
	}
	defer rows.Close()


	colTypes, err := rows.ColumnTypes()
	if err != nil {
		s.sendError(err.Error())
		return
	}

	// 1. Send RowDescription
	rowDesc := BuildRowDescription(colTypes)
	s.backend.Send(rowDesc)

	// 2. Scan and stream DataRows
	colCount := len(colTypes)
	// Pre-allocate scan buffers once — reused across all rows.
	vals := make([]any, colCount)
	scanDest := make([]any, colCount)
	rowValues := make([][]byte, colCount)
	dataRow := &pgproto3.DataRow{Values: rowValues}
	for i := range vals {
		scanDest[i] = &vals[i]
	}

	var rowCount int
	for rows.Next() {
		rowCount++
		if err := rows.Scan(scanDest...); err != nil {
			s.sendError(err.Error())
			return
		}
		for i, v := range vals {
			rowValues[i] = FormatValue(v)
		}
		s.backend.Send(dataRow)
	}

	if err := rows.Err(); err != nil {
		s.sendError(err.Error())
		return
	}

	// 3. CommandComplete and optional ReadyForQuery
	s.backend.Send(&pgproto3.CommandComplete{CommandTag: strconv.AppendInt([]byte("SELECT "), int64(rowCount), 10)})
	if sendReady {
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
	}
}

func (s *Session) executeWriteQuery(ctx context.Context, query string, args []any, sendReady bool) {
	res, err := s.store.Exec(ctx, query, args...)
	if err != nil {
		s.sendError(err.Error())
		return
	}

	tag := res.Tag
	if tag == "" {
		tag = "OK"
	}

	s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	if sendReady {
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
	}
}

func (s *Session) handleParse(msg *pgproto3.Parse) {
	s.statements[msg.Name] = preparedStmt{
		Query:          msg.Query,
		ConvertedQuery: ConvertPlaceholders(msg.Query),
		QueryType:      Classify(msg.Query),
	}
	s.backend.Send(&pgproto3.ParseComplete{})
}

func (s *Session) handleBind(msg *pgproto3.Bind) {
	stmt, ok := s.statements[msg.PreparedStatement]
	if !ok {
		s.sendError(fmt.Sprintf("prepared statement %q does not exist", msg.PreparedStatement))
		return
	}

	// Convert parameter values from string format to any
	args := make([]any, len(msg.Parameters))
	for i, param := range msg.Parameters {
		if param == nil {
			args[i] = nil
		} else {
			args[i] = string(param)
		}
	}

	s.portals[msg.DestinationPortal] = boundPortal{
		Query:          stmt.Query,
		ConvertedQuery: stmt.ConvertedQuery,
		QueryType:      stmt.QueryType,
		Args:           args,
	}
	s.backend.Send(&pgproto3.BindComplete{})
}

func (s *Session) handleDescribe(ctx context.Context, msg *pgproto3.Describe) {
	switch msg.ObjectType {
	case 'S': // Describe prepared statement
		stmt, ok := s.statements[msg.Name]
		if !ok {
			s.sendError(fmt.Sprintf("statement %q not found", msg.Name))
			return
		}
		paramCount := strings.Count(stmt.ConvertedQuery, "?")
		paramOIDs := make([]uint32, paramCount)
		s.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: paramOIDs})
		s.backend.Send(&pgproto3.NoData{})

	case 'P': // Describe portal
		s.backend.Send(&pgproto3.NoData{})
	}
}

func (s *Session) handleExecute(ctx context.Context, msg *pgproto3.Execute) {
	portal, ok := s.portals[msg.Portal]
	if !ok {
		s.sendError(fmt.Sprintf("portal %q not found", msg.Portal))
		return
	}
	if portal.QueryType == QueryTypeRead {
		s.executeReadQuery(ctx, portal.ConvertedQuery, portal.Args, false)
	} else {
		s.executeWriteQuery(ctx, portal.ConvertedQuery, portal.Args, false)
	}
}

func (s *Session) sendError(message string) {
	s.backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42601",
		Message:  message,
	})
	s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
}
