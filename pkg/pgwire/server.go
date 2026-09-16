package pgwire

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
)

// Server listens for incoming PostgreSQL client connections and serves them via pgwire.
type Server struct {
	ln        net.Listener
	store     *sqlstore.Store
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewServer starts a PostgreSQL wire protocol server on the given address.
func NewServer(addr string, store *sqlstore.Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	s := &Server{
		ln:      ln,
		store:   store,
		closeCh: make(chan struct{}),
	}

	s.wg.Add(1)
	go s.acceptLoop()

	return s, nil
}

// Addr returns the bound TCP address string.
func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
				continue
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			session := NewSession(c, s.store)
			_ = session.Serve(context.Background())
		}(conn)
	}
}

// Close gracefully terminates the PostgreSQL wire server.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
	err := s.ln.Close()
	s.wg.Wait()
	return err
}
