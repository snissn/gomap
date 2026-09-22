package redisserver

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tidwall/redcon"

	"github.com/snissn/gomap/kvstore"
	hashdbadapter "github.com/snissn/gomap/kvstore/adapters/hashdb"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

// Server implements a Redis-compatible server backed by TreeDB or HashDB.
type Server struct {
	cfg Config

	dbMu sync.RWMutex
	db   kvstore.DB

	checkpointer checkpointer
	compactor    compactor
	clearer      clearer

	epoch atomic.Uint64
	idSeq atomic.Uint64

	keyLocks *keyLocker

	closing atomic.Bool

	logf func(format string, args ...any)

	redcon *redcon.Server
}

type connState struct {
	id     uint64
	name   string
	authed bool

	epoch uint64

	pending          int
	batch            kvstore.Batch
	batchUnsupported bool

	replyOff  bool
	replySkip bool
	resp3     bool
}

// New creates a configured server and opens the backing engine.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()
	db, err := OpenEngine(cfg)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:      cfg,
		db:       db,
		keyLocks: newKeyLocker(256),
		logf:     cfg.Logf,
	}
	if s.logf == nil {
		s.logf = log.Printf
	}
	s.attachExtras(db)

	s.redcon = redcon.NewServer(cfg.Addr, s.handle, s.accept, s.closed)
	if cfg.IdleClose > 0 {
		s.redcon.SetIdleClose(cfg.IdleClose)
	}
	return s, nil
}

func (s *Server) attachExtras(db kvstore.DB) {
	s.checkpointer = nil
	s.compactor = nil
	s.clearer = nil

	if cp, ok := db.(checkpointer); ok {
		s.checkpointer = cp
	}
	if cl, ok := db.(clearer); ok {
		s.clearer = cl
	}
	if c, ok := db.(compactor); ok {
		s.compactor = c
		return
	}
	if td, ok := db.(*treedbadapter.DB); ok {
		if !treeDBCommandWALEnabled(td.DB) {
			s.compactor = &treeDBCompactor{db: td.DB}
		}
		return
	}
	if hd, ok := db.(*hashdbadapter.DB); ok {
		s.compactor = hd
	}
}

// ListenAndServe starts serving on the configured address.
func (s *Server) ListenAndServe() error {
	return s.redcon.ListenAndServe()
}

// Serve starts serving on the provided listener (useful for tests).
func (s *Server) Serve(ln net.Listener) error {
	return s.redcon.Serve(ln)
}

// Addr returns the bound address if the server is listening.
func (s *Server) Addr() net.Addr {
	return s.redcon.Addr()
}

// Close stops the server and closes the backing engine.
func (s *Server) Close() error {
	s.closing.Store(true)
	if s.redcon != nil {
		_ = s.redcon.Close()
	}
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Server) accept(conn redcon.Conn) bool {
	state := &connState{
		id:     s.idSeq.Add(1),
		authed: s.cfg.Auth == "",
		epoch:  s.epoch.Load(),
	}
	conn.SetContext(state)
	return true
}

func (s *Server) closed(conn redcon.Conn, _ error) {
	if s.closing.Load() {
		return
	}
	st, _ := conn.Context().(*connState)
	if st == nil {
		return
	}
	if st.pending > 0 {
		if err := s.flushBatch(conn, st); err != nil {
			s.logf("redisserver: flush on close failed: %v", err)
		}
	}
	if st.batch != nil {
		_ = st.batch.Close()
	}
}

func (s *Server) withDB(fn func(db kvstore.DB) error) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db == nil {
		return errors.New("db closed")
	}
	return fn(s.db)
}

func (s *Server) resetDB() error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
	if err := os.RemoveAll(s.cfg.Dir); err != nil {
		return err
	}
	if err := os.MkdirAll(s.cfg.Dir, 0o755); err != nil {
		return err
	}
	db, err := OpenEngine(s.cfg)
	if err != nil {
		return err
	}
	s.db = db
	s.attachExtras(db)
	s.epoch.Add(1)
	return nil
}

func (s *Server) flushBatch(conn redcon.Conn, st *connState) error {
	if st.pending == 0 {
		return nil
	}
	if st.batch == nil {
		st.pending = 0
		return nil
	}
	if err := st.batch.Commit(); err != nil {
		return err
	}
	for i := 0; i < st.pending; i++ {
		conn.WriteString("OK")
	}
	st.pending = 0
	// Reset batch buffer if supported.
	if rb, ok := st.batch.(interface{ Reset() }); ok {
		rb.Reset()
		return nil
	}
	_ = st.batch.Close()
	st.batch = nil
	return nil
}

func wrongArgs(conn redcon.Conn, cmd string) {
	conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s'", strings.ToLower(cmd)))
}

func notAuthed(conn redcon.Conn) {
	conn.WriteError("NOAUTH Authentication required.")
}

func unsupported(conn redcon.Conn) {
	conn.WriteError("ERR unsupported")
}

func prefixUpper(b []byte) string {
	return strings.ToUpper(string(b))
}
