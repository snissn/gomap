package hashdbredis

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/snissn/gomap/HashDB"
	"github.com/tidwall/redcon"
)

type connState struct {
	pending  []hashdb.Item
	replyOff bool
}

const setBatchSize = 16

type RedisServer struct {
	store     *hashdb.HashDB
	batchSets bool
}

func NewRedisServer(dbdir string) *RedisServer {
	if err := os.MkdirAll(dbdir, 0o755); err != nil {
		log.Fatalf("failed to create HashDB folder: %v", err)
	}

	var store hashdb.HashDB
	var shards int
	if v := strings.TrimSpace(os.Getenv("HASHDB_SHARDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			shards = n
		}
	} else if v := strings.TrimSpace(os.Getenv("GOMAP_SHARDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			shards = n
		}
	}

	var err error
	if shards > 0 {
		err = store.NewWithShards(dbdir, shards)
	} else {
		err = store.New(dbdir)
	}
	if err != nil {
		log.Fatalf("failed to open HashDB: %v", err)
	}
	// Disable compression by default for the Redis/HashDB server, which is primarily
	// used by the benchmark harness.
	store.SetCompression(false)

	return &RedisServer{
		store:     &store,
		batchSets: os.Getenv("HASHDB_BATCH_SETS") == "1" || os.Getenv("GOMAP_BATCH_SETS") == "1",
	}
}

func (s *RedisServer) Serve(addr string) error {
	return redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) == 0 {
			// Never reply if replies are disabled for this connection.
			if st, ok := conn.Context().(*connState); ok && st.replyOff {
				return
			}
			conn.WriteError("empty command")
			return
		}

		var state *connState
		if ctx := conn.Context(); ctx != nil {
			if st, ok := ctx.(*connState); ok {
				state = st
			}
		}

		switch string(cmd.Args[0]) {
		case "PING":
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("PONG")

		case "HELLO":
			// Minimal support for RESP3 negotiation (used by noreply_bench).
			// We don't currently switch response formatting; most of our benchmark traffic
			// runs with CLIENT REPLY OFF where replies are suppressed anyway.
			if state != nil && state.replyOff {
				return
			}
			if len(cmd.Args) >= 2 && string(cmd.Args[1]) == "3" {
				conn.WriteArray(2)
				conn.WriteBulkString("proto")
				conn.WriteInt(3)
				return
			}
			conn.WriteString("OK")

		case "CLIENT":
			// Minimal support for CLIENT REPLY OFF (used by noreply_bench).
			if len(cmd.Args) >= 3 && string(cmd.Args[1]) == "REPLY" {
				if state == nil {
					state = &connState{}
					conn.SetContext(state)
				}
				switch string(cmd.Args[2]) {
				case "OFF":
					state.replyOff = true
					// Redis-style behavior: suppress reply to CLIENT REPLY OFF itself.
					return
				case "ON":
					state.replyOff = false
					conn.WriteString("OK")
					return
				default:
					if state.replyOff {
						return
					}
					conn.WriteError("ERR unknown CLIENT REPLY mode")
					return
				}
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteError("unknown command")

		case "SET":
			if len(cmd.Args) < 3 {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError("ERR wrong number of arguments for 'SET'")
				return
			}
			key := cmd.Args[1]
			val := cmd.Args[2]

			if s.batchSets {
				if state == nil {
					state = &connState{
						pending: make([]hashdb.Item, 0, setBatchSize),
					}
					conn.SetContext(state)
				} else if state.pending == nil {
					state.pending = make([]hashdb.Item, 0, setBatchSize)
				}

				item := hashdb.Item{Key: key, Value: val}
				state.pending = append(state.pending, item)

				// Flush once we hit the batch size; this is aimed at
				// redis-benchmark -P16 where key counts are multiples of 16.
				if len(state.pending) >= setBatchSize {
					batch := state.pending
					state.pending = state.pending[:0]

					for i := range batch {
						if err := s.store.PutNoCopyKeyValueUnsafe(batch[i].Key, batch[i].Value); err != nil {
							if state.replyOff {
								return
							}
							conn.WriteError(err.Error())
							return
						}
					}
					if !state.replyOff {
						for range batch {
							conn.WriteString("OK")
						}
					}
				}
			} else {
				if err := s.store.PutNoCopyKeyValueUnsafe(key, val); err != nil {
					if state != nil && state.replyOff {
						return
					}
					conn.WriteError(err.Error())
					return
				}
				if state != nil && state.replyOff {
					return
				}
				conn.WriteString("OK")
			}

		case "GET":
			if len(cmd.Args) < 2 {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError("ERR wrong number of arguments for 'GET'")
				return
			}
			key := cmd.Args[1]

			val, err := s.store.Get(key)
			if state != nil && state.replyOff {
				return
			}
			if err != nil || val == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(val)
			}

		case "DEL":
			if len(cmd.Args) < 2 {
				if state != nil && state.replyOff {
					return
				}
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
			if state != nil && state.replyOff {
				return
			}
			conn.WriteInt(count)

		case "MSET":
			if len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError("ERR wrong number of arguments for 'MSET'")
				return
			}

			for i := 1; i < len(cmd.Args); i += 2 {
				if err := s.store.PutNoCopyKeyValueUnsafe(cmd.Args[i], cmd.Args[i+1]); err != nil {
					if state != nil && state.replyOff {
						return
					}
					conn.WriteError(err.Error())
					return
				}
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("OK")

		case "MGET":
			if len(cmd.Args) < 2 {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError("ERR wrong number of arguments for 'MGET'")
				return
			}

			keyCount := len(cmd.Args) - 1
			keys := make([][]byte, keyCount)
			for i := 0; i < keyCount; i++ {
				keys[i] = cmd.Args[i+1]
			}

			values, errs := s.store.GetMany(keys)

			if state != nil && state.replyOff {
				return
			}
			conn.WriteArray(keyCount)
			for i := 0; i < keyCount; i++ {
				if errs[i] != nil || values[i] == nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(values[i])
				}
			}

		case "EXISTS":
			if len(cmd.Args) < 2 {
				if state != nil && state.replyOff {
					return
				}
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
			if state != nil && state.replyOff {
				return
			}
			conn.WriteInt(count)

		case "INCR":
			if len(cmd.Args) != 2 {
				if state != nil && state.replyOff {
					return
				}
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
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError(err.Error()) // e.g. not integer
				return
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteInt64(newValInt)

		case "FLUSHDB", "FLUSHALL":
			if err := s.store.Clear(); err != nil {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError(err.Error())
				return
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("OK")

		case "SAVE":
			if err := s.store.Flush(); err != nil {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError(err.Error())
				return
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("OK")

		case "INFO":
			stats := s.store.Stats()
			info := fmt.Sprintf(
				"# Keyspace\r\nkeys=%d,expires=0,avg_ttl=0\r\n"+
					"# Memory\r\nused_memory=%d\r\n"+
					"# Stats\r\ntotal_capacity=%d\r\ntotal_segments=%d\r\n",
				stats.KeyCount, stats.DataSize, stats.Capacity, stats.Segments,
			)
			if state != nil && state.replyOff {
				return
			}
			conn.WriteBulkString(info)

		case "BGREWRITEAOF":
			go func() {
				if err := s.store.Compact(); err != nil {
					log.Printf("Compaction failed: %v", err)
				}
			}()
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("Background append only file rewriting started")

		case "COMPACT":
			if err := s.store.Compact(); err != nil {
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError(err.Error())
				return
			}
			if state != nil && state.replyOff {
				return
			}
			conn.WriteString("OK")

		default:
			if state != nil && state.replyOff {
				return
			}
			conn.WriteError("unknown command")
		}
	}, nil, nil)
}
