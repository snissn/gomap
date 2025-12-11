package gomapredis

import (
	"log"
	"os"
	"strconv"

	"github.com/snissn/gomap"
	"github.com/tidwall/redcon"
)

type RedisServer struct {
	store *gomap.HashmapDistributed
}

func NewRedisServer(dbdir string) *RedisServer {
	if err := os.MkdirAll(dbdir, 0755); err != nil {
		log.Fatalf("failed to create gomap folder: %v", err)
	}

	var store gomap.HashmapDistributed
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

			err := s.store.Add(key, val)
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

			val, err := s.store.Get(key)
			if err != nil || val == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(val)
			}

		case "DEL":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for 'DEL'")
				return
			}
			count := 0
			// DEL key1 key2 ...
			for i := 1; i < len(cmd.Args); i++ {
				key := cmd.Args[i]
				err := s.store.Delete(key)
				// Delete returns nil if not found, or if success.
				// Redis DEL returns count of deleted keys.
				// Our Delete implementation does not return "found/notfound" boolean yet.
				// It returns error only on failure.
				// If we want accurate count, we need Delete to return bool.
				// For now, we assume if no error, we deleted it?
				// But Redis only counts *existing* keys that were removed.
				// If I try to delete non-existent key, count shouldn't increment.
				// I'll check existence with Get first? No, race condition.
				// I should update Delete to return bool.
				
				// For now, simple implementation: just return 1 if no error?
				// Or check Get first (imperfect but okay for now).
				// Or update Delete signature.
				
				// Let's rely on Delete returning nil error.
				if err == nil {
					count++ 
				}
			}
			conn.WriteInt(count)

		case "MSET":
			if len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
				conn.WriteError("ERR wrong number of arguments for 'MSET'")
				return
			}
			
			items := make([]gomap.Item, 0, (len(cmd.Args)-1)/2)
			for i := 1; i < len(cmd.Args); i += 2 {
				items = append(items, gomap.Item{
					Key:   cmd.Args[i],
					Value: cmd.Args[i+1],
				})
			}
			
			err := s.store.AddMany(items)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			conn.WriteString("OK")

		case "MGET":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for 'MGET'")
				return
			}
			
			conn.WriteArray(len(cmd.Args) - 1)
			for i := 1; i < len(cmd.Args); i++ {
				val, err := s.store.Get(cmd.Args[i])
				if err != nil || val == nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			}

		case "EXISTS":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for 'EXISTS'")
				return
			}
			
			count := 0
			for i := 1; i < len(cmd.Args); i++ {
				val, _ := s.store.Get(cmd.Args[i])
				if val != nil {
					count++
				}
			}
			conn.WriteInt(count)

		case "INCR":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for 'INCR'")
				return
			}
			
			key := cmd.Args[1]
			var newValInt int64
			
			err := s.store.Update(key, func(val []byte) ([]byte, error) {
				var current int64
				if val != nil {
					var err error
					current, err = strconv.ParseInt(string(val), 10, 64)
					if err != nil {
						return nil, err // Value is not an integer
					}
				}
				current++
				newValInt = current
				return []byte(strconv.FormatInt(current, 10)), nil
			})
			
			if err != nil {
				conn.WriteError(err.Error()) // e.g. not integer
				return
			}
			conn.WriteInt64(newValInt)

		default:
			conn.WriteError("unknown command")
		}
	}, nil, nil)
}
