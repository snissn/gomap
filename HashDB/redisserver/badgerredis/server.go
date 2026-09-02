package badgerredis

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

type RedisServer struct {
	db *badger.DB
}

func NewRedisServer(dbPath string) (*RedisServer, error) {
	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &RedisServer{db: db}, nil
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
			s.db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, val)
			})
			conn.WriteString("OK")
		case "GET":
			key := cmd.Args[1]
			s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err == badger.ErrKeyNotFound {
					conn.WriteNull()
					return nil
				}
				val, _ := item.ValueCopy(nil)
				conn.WriteBulk(val)
				return nil
			})
		case "DEL":
			key := cmd.Args[1]
			s.db.Update(func(txn *badger.Txn) error {
				return txn.Delete(key)
			})
			conn.WriteInt(1)
		default:
			conn.WriteError("unknown command")
		}
	}, nil, nil)
}
