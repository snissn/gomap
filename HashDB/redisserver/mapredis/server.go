package mapredis

import (
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/tidwall/redcon"
)

type connState struct {
	replyOff bool
}

// RedisServer is a minimal Redis-compatible server backed by a plain Go map.
//
// This backend exists mainly as a baseline for quantifying protocol + redcon
// overhead versus the storage engines (HashDB/TreeDB).
//
// NOTE: For performance, values are stored as-is from redcon's cmd.Args slices.
// Redcon makes a full copy of the raw RESP command per request, so the slices
// remain valid after the handler returns. This does mean we retain the raw
// command backing array for each stored value.
type RedisServer struct {
	shards []mapShard
}

type mapShard struct {
	mu sync.RWMutex
	kv map[string][]byte
}

const defaultShardCount = 256

func NewRedisServer(_ string) *RedisServer {
	s := &RedisServer{
		shards: make([]mapShard, defaultShardCount),
	}
	for i := range s.shards {
		s.shards[i].kv = make(map[string][]byte)
	}
	return s
}

func (s *RedisServer) Serve(addr string) error {
	return redcon.ListenAndServe(addr, s.handle, nil, nil)
}

func (s *RedisServer) shardForKey(key []byte) *mapShard {
	// defaultShardCount is a power of two so we can mask rather than mod.
	idx := int(xxhash.Sum64(key) & uint64(len(s.shards)-1))
	return &s.shards[idx]
}

func (s *RedisServer) handle(conn redcon.Conn, cmd redcon.Command) {
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
		// Minimal RESP3 negotiation (used by noreply_bench).
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
		// Minimal support for CLIENT REPLY OFF/ON (used by noreply_bench).
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

		keyBytes := cmd.Args[1]
		val := cmd.Args[2]

		keyLookup := bytesToStringNoAlloc(keyBytes)

		shard := s.shardForKey(keyBytes)
		shard.mu.Lock()
		if _, ok := shard.kv[keyLookup]; ok {
			shard.kv[keyLookup] = val
		} else {
			// Allocate a stable key string once per unique key. This avoids keeping
			// the full cmd.Raw (incl. value bytes) alive forever via the map key.
			shard.kv[string(keyBytes)] = val
		}
		shard.mu.Unlock()

		if state != nil && state.replyOff {
			return
		}
		conn.WriteString("OK")

	case "GET":
		if len(cmd.Args) < 2 {
			if state != nil && state.replyOff {
				return
			}
			conn.WriteError("ERR wrong number of arguments for 'GET'")
			return
		}

		keyLookup := bytesToStringNoAlloc(cmd.Args[1])

		shard := s.shardForKey(cmd.Args[1])
		shard.mu.RLock()
		val, ok := shard.kv[keyLookup]
		shard.mu.RUnlock()

		if state != nil && state.replyOff {
			return
		}
		if !ok || val == nil {
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
		for i := 1; i < len(cmd.Args); i++ {
			keyBytes := cmd.Args[i]
			keyLookup := bytesToStringNoAlloc(keyBytes)
			shard := s.shardForKey(keyBytes)
			shard.mu.Lock()
			if _, ok := shard.kv[keyLookup]; ok {
				delete(shard.kv, keyLookup)
				count++
			}
			shard.mu.Unlock()
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
			keyBytes := cmd.Args[i]
			val := cmd.Args[i+1]
			keyLookup := bytesToStringNoAlloc(keyBytes)
			shard := s.shardForKey(keyBytes)
			shard.mu.Lock()
			if _, ok := shard.kv[keyLookup]; ok {
				shard.kv[keyLookup] = val
			} else {
				shard.kv[string(keyBytes)] = val
			}
			shard.mu.Unlock()
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

		keys := cmd.Args[1:]
		if state != nil && state.replyOff {
			// Still do the work to match server-side behavior, but suppress reply.
			// This keeps the fast benchmark path consistent.
			for _, key := range keys {
				shard := s.shardForKey(key)
				shard.mu.RLock()
				_, _ = shard.kv[bytesToStringNoAlloc(key)]
				shard.mu.RUnlock()
			}
			return
		}

		conn.WriteArray(len(keys))
		for _, key := range keys {
			shard := s.shardForKey(key)
			shard.mu.RLock()
			val, ok := shard.kv[bytesToStringNoAlloc(key)]
			shard.mu.RUnlock()
			if !ok || val == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(val)
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
			keyBytes := cmd.Args[i]
			shard := s.shardForKey(keyBytes)
			shard.mu.RLock()
			v, ok := shard.kv[bytesToStringNoAlloc(keyBytes)]
			shard.mu.RUnlock()
			if ok && v != nil {
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

		keyBytes := cmd.Args[1]
		keyLookup := bytesToStringNoAlloc(keyBytes)

		var out int64
		var err error
		shard := s.shardForKey(keyBytes)
		shard.mu.Lock()
		val, ok := shard.kv[keyLookup]
		if ok && val != nil {
			out, err = strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				shard.mu.Unlock()
				if state != nil && state.replyOff {
					return
				}
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
		}
		out++
		if ok {
			shard.kv[keyLookup] = []byte(strconv.FormatInt(out, 10))
		} else {
			shard.kv[string(keyBytes)] = []byte(strconv.FormatInt(out, 10))
		}
		shard.mu.Unlock()

		if state != nil && state.replyOff {
			return
		}
		conn.WriteInt64(out)

	case "FLUSHDB", "FLUSHALL":
		for i := range s.shards {
			shard := &s.shards[i]
			shard.mu.Lock()
			clear(shard.kv)
			shard.mu.Unlock()
		}
		if state != nil && state.replyOff {
			return
		}
		conn.WriteString("OK")

	case "SAVE":
		// In-memory backend has nothing durable to write.
		if state != nil && state.replyOff {
			return
		}
		conn.WriteString("OK")

	case "INFO":
		keys := 0
		for i := range s.shards {
			shard := &s.shards[i]
			shard.mu.RLock()
			keys += len(shard.kv)
			shard.mu.RUnlock()
		}
		info := fmt.Sprintf("# Keyspace\r\nkeys=%d,expires=0,avg_ttl=0\r\n", keys)
		if state != nil && state.replyOff {
			return
		}
		conn.WriteBulkString(info)

	default:
		if state != nil && state.replyOff {
			return
		}
		conn.WriteError("unknown command")
	}
}

func bytesToStringNoAlloc(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
