package redisserver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/match"
	"github.com/tidwall/redcon"

	"github.com/snissn/gomap/kvstore"
)

func (s *Server) handle(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	st, _ := conn.Context().(*connState)
	if st == nil {
		st = &connState{
			id:     s.idSeq.Add(1),
			authed: s.cfg.Auth == "",
			epoch:  s.epoch.Load(),
		}
		conn.SetContext(st)
	}

	if st.epoch != s.epoch.Load() {
		if st.batch != nil {
			_ = st.batch.Close()
		}
		st.batch = nil
		st.pending = 0
		st.batchUnsupported = false
		st.epoch = s.epoch.Load()
	}

	name := prefixUpper(cmd.Args[0])

	if s.cfg.Auth != "" && !st.authed {
		switch name {
		case "AUTH", "PING", "QUIT", "HELLO", "CLIENT":
		default:
			notAuthed(conn)
			return
		}
	}

	if s.cfg.BatchSets && s.cfg.BatchFlushOnNonset && name != "SET" {
		if err := s.flushBatch(conn, st); err != nil {
			conn.WriteError(err.Error())
			return
		}
	}

	if name == "CLIENT" {
		s.handleClient(conn, st, cmd)
		return
	}

	quiet := st.replyOff || st.replySkip
	if st.replySkip {
		st.replySkip = false
	}
	if quiet {
		conn = &silentConn{Conn: conn}
	}

	switch name {
	case "PING":
		if len(cmd.Args) > 2 {
			wrongArgs(conn, name)
			return
		}
		if len(cmd.Args) == 2 {
			conn.WriteBulk(cmd.Args[1])
			return
		}
		conn.WriteString("PONG")

	case "ECHO":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		conn.WriteBulk(cmd.Args[1])

	case "QUIT":
		conn.WriteString("OK")
		_ = conn.Close()

	case "AUTH":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		if s.cfg.Auth == "" {
			st.authed = true
			conn.WriteString("OK")
			return
		}
		if string(cmd.Args[1]) == s.cfg.Auth {
			st.authed = true
			conn.WriteString("OK")
			return
		}
		conn.WriteError("ERR invalid password")

	case "SELECT":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		n, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil || n < 0 {
			conn.WriteError("ERR invalid DB index")
			return
		}
		if n != 0 {
			conn.WriteError("ERR DB index is out of range")
			return
		}
		conn.WriteString("OK")

	case "HELLO":
		s.handleHello(conn, st, cmd)

	case "COMMAND":
		s.handleCommand(conn, cmd)

	case "INFO":
		s.handleInfo(conn)

	case "SET":
		s.handleSet(conn, st, cmd)

	case "GET":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		var val []byte
		if err := s.withDB(func(db kvstore.DB) error {
			var err error
			val, err = db.Get(key)
			return err
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		if val == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(val)

	case "MGET":
		if len(cmd.Args) < 2 {
			wrongArgs(conn, name)
			return
		}
		keys := cmd.Args[1:]
		conn.WriteArray(len(keys))
		if err := s.withDB(func(db kvstore.DB) error {
			for _, key := range keys {
				val, err := db.Get(key)
				if err != nil || val == nil {
					conn.WriteNull()
					continue
				}
				conn.WriteBulk(val)
			}
			return nil
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}

	case "MSET":
		if len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
			wrongArgs(conn, name)
			return
		}
		items := cmd.Args[1:]
		err := s.withDB(func(db kvstore.DB) error {
			if b, ok := db.(kvstore.Batcher); ok {
				batch, err := b.NewBatch()
				if err != nil {
					return err
				}
				for i := 0; i < len(items); i += 2 {
					if err := batch.Set(items[i], items[i+1]); err != nil {
						_ = batch.Close()
						return err
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return err
				}
				return batch.Close()
			}
			for i := 0; i < len(items); i += 2 {
				if err := db.Set(items[i], items[i+1]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	case "DEL", "UNLINK":
		if len(cmd.Args) < 2 {
			wrongArgs(conn, name)
			return
		}
		count := 0
		keys := cmd.Args[1:]
		if err := s.withDB(func(db kvstore.DB) error {
			for _, key := range keys {
				exists := false
				if h, ok := db.(kvstore.Haser); ok {
					ok, err := h.Has(key)
					if err == nil && ok {
						exists = true
					}
				} else {
					val, err := db.Get(key)
					if err == nil && val != nil {
						exists = true
					}
				}
				if exists {
					if err := db.Delete(key); err == nil {
						count++
					}
				}
			}
			return nil
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(count)

	case "EXISTS":
		if len(cmd.Args) < 2 {
			wrongArgs(conn, name)
			return
		}
		count := 0
		keys := cmd.Args[1:]
		if err := s.withDB(func(db kvstore.DB) error {
			for _, key := range keys {
				if h, ok := db.(kvstore.Haser); ok {
					ok, err := h.Has(key)
					if err == nil && ok {
						count++
					}
				} else {
					val, err := db.Get(key)
					if err == nil && val != nil {
						count++
					}
				}
			}
			return nil
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(count)

	case "INCR":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		s.handleIncrBy(conn, cmd.Args[1], 1)

	case "INCRBY":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		delta, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		s.handleIncrBy(conn, cmd.Args[1], delta)

	case "DECR":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		s.handleIncrBy(conn, cmd.Args[1], -1)

	case "DECRBY":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		delta, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		s.handleIncrBy(conn, cmd.Args[1], -delta)

	case "GETSET":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		val := cmd.Args[2]
		unlock := s.keyLocks.lockKey(key)
		defer unlock()
		var old []byte
		var err error
		_ = s.withDB(func(db kvstore.DB) error {
			old, err = db.Get(key)
			if err != nil {
				return err
			}
			return db.Set(key, val)
		})
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if old == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(old)

	case "GETDEL":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		unlock := s.keyLocks.lockKey(key)
		defer unlock()
		var old []byte
		var err error
		_ = s.withDB(func(db kvstore.DB) error {
			old, err = db.Get(key)
			if err != nil {
				return err
			}
			if old != nil {
				return db.Delete(key)
			}
			return nil
		})
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if old == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(old)

	case "SETNX":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		val := cmd.Args[2]
		unlock := s.keyLocks.lockKey(key)
		defer unlock()
		var wrote bool
		var err error
		_ = s.withDB(func(db kvstore.DB) error {
			var exists bool
			if h, ok := db.(kvstore.Haser); ok {
				exists, err = h.Has(key)
				if err != nil {
					return err
				}
			} else {
				var cur []byte
				cur, err = db.Get(key)
				if err != nil {
					return err
				}
				exists = cur != nil
			}
			if exists {
				return nil
			}
			err = db.Set(key, val)
			if err == nil {
				wrote = true
			}
			return err
		})
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if wrote {
			conn.WriteInt(1)
			return
		}
		conn.WriteInt(0)

	case "APPEND":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		val := cmd.Args[2]
		unlock := s.keyLocks.lockKey(key)
		defer unlock()
		var newLen int
		if err := s.withDB(func(db kvstore.DB) error {
			cur, err := db.Get(key)
			if err != nil {
				return err
			}
			combined := append(cur, val...)
			newLen = len(combined)
			return db.Set(key, combined)
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(newLen)

	case "STRLEN":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		var length int
		if err := s.withDB(func(db kvstore.DB) error {
			val, err := db.Get(key)
			if err != nil {
				return err
			}
			if val != nil {
				length = len(val)
			}
			return nil
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(length)

	case "GETRANGE":
		if len(cmd.Args) != 4 {
			wrongArgs(conn, name)
			return
		}
		start, err1 := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		end, err2 := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
		if err1 != nil || err2 != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		key := cmd.Args[1]
		var out []byte
		if err := s.withDB(func(db kvstore.DB) error {
			val, err := db.Get(key)
			if err != nil || val == nil {
				return err
			}
			out = sliceRange(val, start, end)
			return nil
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		if out == nil {
			conn.WriteBulkString("")
			return
		}
		conn.WriteBulk(out)

	case "SETRANGE":
		if len(cmd.Args) != 4 {
			wrongArgs(conn, name)
			return
		}
		offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil || offset < 0 {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		key := cmd.Args[1]
		val := cmd.Args[3]
		unlock := s.keyLocks.lockKey(key)
		defer unlock()
		var newLen int
		var setErr error
		_ = s.withDB(func(db kvstore.DB) error {
			cur, err := db.Get(key)
			if err != nil {
				return err
			}
			needed := int(offset) + len(val)
			if len(cur) < needed {
				expanded := make([]byte, needed)
				copy(expanded, cur)
				cur = expanded
			}
			copy(cur[int(offset):], val)
			newLen = len(cur)
			setErr = db.Set(key, cur)
			return setErr
		})
		if setErr != nil {
			conn.WriteError(setErr.Error())
			return
		}
		conn.WriteInt(newLen)

	case "TYPE":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		key := cmd.Args[1]
		var exists bool
		if err := s.withDB(func(db kvstore.DB) error {
			if h, ok := db.(kvstore.Haser); ok {
				ok, err := h.Has(key)
				if err == nil {
					exists = ok
				}
				return err
			}
			val, err := db.Get(key)
			if err == nil && val != nil {
				exists = true
			}
			return err
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		if exists {
			conn.WriteString("string")
		} else {
			conn.WriteString("none")
		}

	case "RENAME":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		s.handleRename(conn, cmd.Args[1], cmd.Args[2], true)

	case "RENAMENX":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, name)
			return
		}
		s.handleRename(conn, cmd.Args[1], cmd.Args[2], false)

	case "DBSIZE":
		count, err := s.countKeys()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(count)

	case "KEYS":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, name)
			return
		}
		keys, err := s.collectKeys(string(cmd.Args[1]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}

	case "SCAN":
		s.handleScan(conn, cmd)

	case "SAVE":
		if s.checkpointer == nil {
			unsupported(conn)
			return
		}
		if err := s.checkpointer.Checkpoint(); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	case "BGSAVE":
		if s.checkpointer == nil {
			unsupported(conn)
			return
		}
		go func() { _ = s.checkpointer.Checkpoint() }()
		conn.WriteString("Background saving started")

	case "BGREWRITEAOF":
		if s.compactor == nil {
			unsupported(conn)
			return
		}
		go func() { _ = s.compactor.Compact() }()
		conn.WriteString("Background append only file rewriting started")

	case "COMPACT":
		if s.compactor == nil {
			unsupported(conn)
			return
		}
		if err := s.compactor.Compact(); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	case "FLUSHDB", "FLUSHALL":
		if err := s.resetDB(); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	default:
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(name)))
	}
}

func (s *Server) handleSet(conn redcon.Conn, st *connState, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		wrongArgs(conn, "SET")
		return
	}
	key := cmd.Args[1]
	val := cmd.Args[2]
	if s.cfg.BatchSets && !st.batchUnsupported {
		didBatch := true
		err := s.withDB(func(db kvstore.DB) error {
			b, ok := db.(kvstore.Batcher)
			if !ok {
				st.batchUnsupported = true
				didBatch = false
				return db.Set(key, val)
			}
			if st.batch == nil {
				batch, err := b.NewBatch()
				if err != nil {
					return err
				}
				st.batch = batch
			}
			return st.batch.Set(key, val)
		})
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if !didBatch {
			conn.WriteString("OK")
			return
		}
		st.pending++
		if st.pending >= s.cfg.BatchSize {
			if err := s.flushBatch(conn, st); err != nil {
				conn.WriteError(err.Error())
				return
			}
		}
		return
	}
	if err := s.withDB(func(db kvstore.DB) error { return db.Set(key, val) }); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (s *Server) handleIncrBy(conn redcon.Conn, key []byte, delta int64) {
	unlock := s.keyLocks.lockKey(key)
	defer unlock()
	var newVal int64
	if err := s.withDB(func(db kvstore.DB) error {
		val, err := db.Get(key)
		if err != nil {
			return err
		}
		var cur int64
		if val != nil {
			cur, err = strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				return err
			}
		}
		if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < (-1<<63)-delta) {
			return strconv.ErrRange
		}
		newVal = cur + delta
		return db.Set(key, []byte(strconv.FormatInt(newVal, 10)))
	}); err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}
	conn.WriteInt64(newVal)
}

func (s *Server) handleRename(conn redcon.Conn, src, dst []byte, overwrite bool) {
	unlock := s.keyLocks.lockPair(src, dst)
	defer unlock()
	var (
		srcVal  []byte
		err     error
		renamed bool
	)
	if err = s.withDB(func(db kvstore.DB) error {
		srcVal, err = db.Get(src)
		if err != nil {
			return err
		}
		if srcVal == nil {
			return fmt.Errorf("ERR no such key")
		}
		if !overwrite {
			var exists bool
			if h, ok := db.(kvstore.Haser); ok {
				exists, err = h.Has(dst)
				if err != nil {
					return err
				}
			} else {
				val, err := db.Get(dst)
				if err != nil {
					return err
				}
				exists = val != nil
			}
			if exists {
				renamed = false
				return nil
			}
		}
		renamed = true
		if err := db.Set(dst, srcVal); err != nil {
			return err
		}
		return db.Delete(src)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	if !overwrite {
		if renamed {
			conn.WriteInt(1)
			return
		}
		conn.WriteInt(0)
		return
	}
	conn.WriteString("OK")
}

func (s *Server) existsKey(key []byte) (bool, error) {
	var exists bool
	err := s.withDB(func(db kvstore.DB) error {
		if h, ok := db.(kvstore.Haser); ok {
			ok, err := h.Has(key)
			if err != nil {
				return err
			}
			exists = ok
			return nil
		}
		val, err := db.Get(key)
		if err != nil {
			return err
		}
		exists = val != nil
		return nil
	})
	return exists, err
}

func (s *Server) handleClient(conn redcon.Conn, st *connState, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		wrongArgs(conn, "CLIENT")
		return
	}
	sub := prefixUpper(cmd.Args[1])
	switch sub {
	case "ID":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, "CLIENT")
			return
		}
		conn.WriteInt64(int64(st.id))
	case "SETNAME":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, "CLIENT")
			return
		}
		st.name = string(cmd.Args[2])
		conn.WriteString("OK")
	case "GETNAME":
		if len(cmd.Args) != 2 {
			wrongArgs(conn, "CLIENT")
			return
		}
		if st.name == "" {
			conn.WriteNull()
			return
		}
		conn.WriteBulkString(st.name)
	case "INFO":
		info := fmt.Sprintf("id=%d addr=%s name=%s", st.id, conn.RemoteAddr(), st.name)
		conn.WriteBulkString(info)
	case "REPLY":
		if len(cmd.Args) != 3 {
			wrongArgs(conn, "CLIENT")
			return
		}
		mode := prefixUpper(cmd.Args[2])
		switch mode {
		case "ON":
			st.replyOff = false
			st.replySkip = false
			conn.WriteString("OK")
		case "OFF":
			st.replyOff = true
			st.replySkip = false
		case "SKIP":
			st.replySkip = true
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	default:
		unsupported(conn)
	}
}

func (s *Server) handleHello(conn redcon.Conn, st *connState, cmd redcon.Command) {
	if len(cmd.Args) > 1 {
		proto, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil || (proto != 2 && proto != 3) {
			conn.WriteError("ERR unsupported protocol version")
			return
		}
		st.resp3 = proto == 3
	}
	conn.WriteString("OK")
}

type commandInfo struct {
	name     string
	arity    int
	flags    []string
	firstKey int
	lastKey  int
	keyStep  int
}

var commandTable = []commandInfo{
	{"append", 3, []string{"write"}, 1, 1, 1},
	{"auth", 2, []string{"fast"}, 0, 0, 0},
	{"bgrewriteaof", 1, []string{"admin"}, 0, 0, 0},
	{"bgsave", 1, []string{"admin"}, 0, 0, 0},
	{"client", -2, []string{"fast"}, 0, 0, 0},
	{"command", -1, []string{"fast"}, 0, 0, 0},
	{"compact", 1, []string{"admin"}, 0, 0, 0},
	{"dbsize", 1, []string{"readonly"}, 0, 0, 0},
	{"decr", 2, []string{"write"}, 1, 1, 1},
	{"decrby", 3, []string{"write"}, 1, 1, 1},
	{"del", -2, []string{"write"}, 1, -1, 1},
	{"echo", 2, []string{"fast"}, 0, 0, 0},
	{"exists", -2, []string{"readonly"}, 1, -1, 1},
	{"flushall", 1, []string{"admin"}, 0, 0, 0},
	{"flushdb", 1, []string{"admin"}, 0, 0, 0},
	{"get", 2, []string{"readonly"}, 1, 1, 1},
	{"getdel", 2, []string{"write"}, 1, 1, 1},
	{"getrange", 4, []string{"readonly"}, 1, 1, 1},
	{"getset", 3, []string{"write"}, 1, 1, 1},
	{"hello", -1, []string{"fast"}, 0, 0, 0},
	{"incr", 2, []string{"write"}, 1, 1, 1},
	{"incrby", 3, []string{"write"}, 1, 1, 1},
	{"info", 1, []string{"fast"}, 0, 0, 0},
	{"keys", 2, []string{"readonly"}, 0, 0, 0},
	{"mget", -2, []string{"readonly"}, 1, -1, 1},
	{"mset", -3, []string{"write"}, 1, -1, 2},
	{"ping", -1, []string{"fast"}, 0, 0, 0},
	{"quit", 1, []string{"fast"}, 0, 0, 0},
	{"rename", 3, []string{"write"}, 1, 2, 1},
	{"renamenx", 3, []string{"write"}, 1, 2, 1},
	{"save", 1, []string{"admin"}, 0, 0, 0},
	{"scan", -2, []string{"readonly"}, 0, 0, 0},
	{"select", 2, []string{"fast"}, 0, 0, 0},
	{"set", -3, []string{"write"}, 1, 1, 1},
	{"setnx", 3, []string{"write"}, 1, 1, 1},
	{"setrange", 4, []string{"write"}, 1, 1, 1},
	{"strlen", 2, []string{"readonly"}, 1, 1, 1},
	{"type", 2, []string{"readonly"}, 1, 1, 1},
	{"unlink", -2, []string{"write"}, 1, -1, 1},
}

func (s *Server) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		conn.WriteArray(len(commandTable))
		for _, info := range commandTable {
			writeCommandInfo(conn, info)
		}
		return
	}
	sub := prefixUpper(cmd.Args[1])
	switch sub {
	case "COUNT":
		conn.WriteInt(len(commandTable))
	case "INFO":
		if len(cmd.Args) < 3 {
			wrongArgs(conn, "COMMAND")
			return
		}
		conn.WriteArray(len(cmd.Args) - 2)
		for _, arg := range cmd.Args[2:] {
			name := strings.ToLower(string(arg))
			info, ok := lookupCommand(name)
			if !ok {
				conn.WriteNull()
				continue
			}
			writeCommandInfo(conn, info)
		}
	default:
		wrongArgs(conn, "COMMAND")
	}
}

func lookupCommand(name string) (commandInfo, bool) {
	for _, info := range commandTable {
		if info.name == name {
			return info, true
		}
	}
	return commandInfo{}, false
}

func writeCommandInfo(conn redcon.Conn, info commandInfo) {
	conn.WriteArray(6)
	conn.WriteBulkString(info.name)
	conn.WriteInt(info.arity)
	conn.WriteArray(len(info.flags))
	for _, flag := range info.flags {
		conn.WriteBulkString(flag)
	}
	conn.WriteInt(info.firstKey)
	conn.WriteInt(info.lastKey)
	conn.WriteInt(info.keyStep)
}

func (s *Server) handleInfo(conn redcon.Conn) {
	var stats map[string]string
	_ = s.withDB(func(db kvstore.DB) error {
		if sp, ok := db.(kvstore.StatsProvider); ok {
			stats = sp.Stats()
		}
		return nil
	})
	var b strings.Builder
	b.WriteString("# Server\r\n")
	b.WriteString("redis_version:0.0.0\r\n")
	b.WriteString("\r\n# Engine\r\n")
	b.WriteString("engine:")
	b.WriteString(s.cfg.Engine)
	b.WriteString("\r\n")
	if stats != nil {
		b.WriteString("\r\n# Stats\r\n")
		for k, v := range stats {
			b.WriteString(k)
			b.WriteString(":")
			b.WriteString(v)
			b.WriteString("\r\n")
		}
	}
	conn.WriteBulkString(b.String())
}

func sliceRange(val []byte, start, end int64) []byte {
	n := int64(len(val))
	if n == 0 {
		return []byte{}
	}
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		return []byte{}
	}
	if start >= n {
		return []byte{}
	}
	if end >= n {
		end = n - 1
	}
	if start > end {
		return []byte{}
	}
	return val[start : end+1]
}

func (s *Server) handleScan(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		wrongArgs(conn, "SCAN")
		return
	}
	cursor, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil || cursor < 0 {
		conn.WriteError("ERR invalid cursor")
		return
	}
	pattern := "*"
	count := 10
	args := cmd.Args[2:]
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			pattern = string(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = n
			i++
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	keys, nextCursor, err := s.scanKeys(cursor, count, pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(2)
	conn.WriteBulkString(strconv.Itoa(nextCursor))
	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulk(k)
	}
}

func (s *Server) collectKeys(pattern string) ([][]byte, error) {
	var out [][]byte
	err := s.withDB(func(db kvstore.DB) error {
		return s.iterateKeys(db, pattern, 0, func(key []byte) bool {
			out = append(out, append([]byte(nil), key...))
			return true
		})
	})
	return out, err
}

func (s *Server) scanKeys(cursor int, count int, pattern string) ([][]byte, int, error) {
	var out [][]byte
	skipped := 0
	err := s.withDB(func(db kvstore.DB) error {
		return s.iterateKeys(db, pattern, cursor, func(key []byte) bool {
			if skipped < cursor {
				skipped++
				return true
			}
			out = append(out, append([]byte(nil), key...))
			return len(out) < count
		})
	})
	if err != nil {
		return nil, 0, err
	}
	next := 0
	if len(out) == count {
		next = cursor + len(out)
	}
	return out, next, nil
}

func (s *Server) iterateKeys(db kvstore.DB, pattern string, cursor int, fn func(key []byte) bool) error {
	prefix, prefixOK := prefixPattern(pattern)
	if rs, ok := db.(kvstore.RangeScanner); ok && (pattern == "*" || prefixOK) {
		var start []byte
		var end []byte
		if prefixOK {
			start = []byte(prefix)
			end = prefixEnd(start)
		}
		it, err := rs.Iterator(start, end)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.Valid() {
			key := it.Key()
			if pattern == "*" || match.Match(string(key), pattern) {
				if !fn(key) {
					break
				}
			}
			it.Next()
		}
		return it.Error()
	}
	if fe, ok := db.(kvstore.ForEacher); ok {
		err := fe.ForEach(func(k, _ []byte) error {
			if pattern == "*" || match.Match(string(k), pattern) {
				if !fn(k) {
					return errStopIteration
				}
			}
			return nil
		})
		if err == errStopIteration {
			return nil
		}
		return err
	}
	return fmt.Errorf("ERR unsupported")
}

func (s *Server) countKeys() (int, error) {
	count := 0
	err := s.withDB(func(db kvstore.DB) error {
		return s.iterateKeys(db, "*", 0, func(_ []byte) bool {
			count++
			return true
		})
	})
	return count, err
}

var errStopIteration = fmt.Errorf("stop")

func prefixPattern(pattern string) (string, bool) {
	if pattern == "*" {
		return "", false
	}
	if !strings.HasSuffix(pattern, "*") {
		return "", false
	}
	base := pattern[:len(pattern)-1]
	if strings.ContainsAny(base, "*?\\") {
		return "", false
	}
	return base, true
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
