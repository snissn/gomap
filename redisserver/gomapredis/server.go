package gomapredis

import (
	"log"
	"os"
	"sync"

	"github.com/snissn/gomap"
	"github.com/tidwall/redcon"
)

type RedisServer struct {
	store *gomap.Hashmap
	lock  sync.RWMutex
}

func NewRedisServer(dbdir string) *RedisServer {
	if err := os.MkdirAll(dbdir, 0755); err != nil {
		log.Fatalf("failed to create gomap folder: %v", err)
	}

	var store gomap.Hashmap
	if err := store.New(dbdir); err != nil {
		log.Fatalf("failed to open gomap: %v", err)
	}

	return &RedisServer{
		store: &store,
	}
}

func (s *RedisServer) Serve(addr string) error {
	return redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) == 0 {
			conn.WriteError("empty command")
			return
		}

		switch string(cmd.Args[0]) {
		case "PING":
			conn.WriteString("PONG")

		case "SET":
			if len(cmd.Args) < 3 {
				conn.WriteError("ERR wrong number of arguments for 'SET'")
				return
			}
			key := cmd.Args[1]
			val := cmd.Args[2]

			s.lock.Lock()
			err := s.store.Add(key, val)
			s.lock.Unlock()

			if err != nil {
				conn.WriteError(err.Error())
				return
			}

			conn.WriteString("OK")

		case "GET":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for 'GET'")
				return
			}
			key := cmd.Args[1]

			s.lock.RLock()
			val, err := s.store.Get(key)
			s.lock.RUnlock()

			if err != nil || val == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(val)
			}

		case "DEL":
			// You can implement DEL later if gomap.Hashmap supports deletion.
			conn.WriteError("DEL not supported yet")

		default:
			conn.WriteError("unknown command")
		}
	}, nil, nil)
}
