package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
	"github.com/jackc/pgproto3/v2"
)

type preparedStmt struct {
	Query string
}

type boundPortal struct {
	Query string
	Args  []any
}

// Session handles an individual PostgreSQL client connection.
type Session struct {
	conn       net.Conn
	backend    *pgproto3.Backend
	store      *sqlstore.Store
	statements map[string]preparedStmt
	portals    map[string]boundPortal
	txStatus   byte // 'I' = Idle, 'T' = In transaction
}

// NewSession initializes a client session over a net.Conn.
func NewSession(conn net.Conn, store *sqlstore.Store) *Session {
	return &Session{
		conn:       conn,
		backend:    pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn),
		store:      store,
		statements: make(map[string]preparedStmt),
		portals:    make(map[string]boundPortal),
		txStatus:   'I',
	}
}

// Serve runs the handshake and main command loop until client terminates or disconnects.
func (s *Session) Serve(ctx context.Context) error {
	defer s.conn.Close()

	if err := s.handshake(); err != nil {
		return err
	}

	return s.queryLoop(ctx)
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
			return s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
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
			s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
		case *pgproto3.Terminate:
			return nil
		default:
			// Unhandled message type, send error
			s.sendError(fmt.Sprintf("unsupported message type: %T", msg))
		}
	}
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
		s.executeReadQuery(ctx, sqlText, nil, true)

	case QueryTypeWrite:
		s.executeWriteQuery(ctx, sqlText, nil, true)

	default:
		// Try as read query first, fallback to write
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "SELECT") {
			s.executeReadQuery(ctx, sqlText, nil, true)
		} else {
			s.executeWriteQuery(ctx, sqlText, nil, true)
		}
	}
}

func (s *Session) executeReadQuery(ctx context.Context, query string, args []any, sendReady bool) {
	query = ConvertPlaceholders(query)
	rows, err := s.store.Query(ctx, query, args...)
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
	var rowCount int

	for rows.Next() {
		rowCount++
		scanDest := make([]any, colCount)
		for i := range scanDest {
			var val any
			scanDest[i] = &val
		}

		if err := rows.Scan(scanDest...); err != nil {
			s.sendError(err.Error())
			return
		}

		rowValues := make([][]byte, colCount)
		for i, v := range scanDest {
			actual := *(v.(*any))
			rowValues[i] = FormatValue(actual)
		}

		s.backend.Send(&pgproto3.DataRow{Values: rowValues})
	}

	if err := rows.Err(); err != nil {
		s.sendError(err.Error())
		return
	}

	// 3. CommandComplete and optional ReadyForQuery
	tag := fmt.Sprintf("SELECT %d", rowCount)
	s.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	if sendReady {
		s.backend.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus})
	}
}

func (s *Session) executeWriteQuery(ctx context.Context, query string, args []any, sendReady bool) {
	query = ConvertPlaceholders(query)
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
		Query: msg.Query,
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
		Query: stmt.Query,
		Args:  args,
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
		converted := ConvertPlaceholders(stmt.Query)
		paramCount := strings.Count(converted, "?")
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

	qType := Classify(portal.Query)
	if qType == QueryTypeRead {
		s.executeReadQuery(ctx, portal.Query, portal.Args, false)
	} else {
		s.executeWriteQuery(ctx, portal.Query, portal.Args, false)
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
