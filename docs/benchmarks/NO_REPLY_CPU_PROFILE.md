# No-Reply CPU Profiles (CLIENT REPLY OFF)

This file records an "engine hot path" CPU profile run where we disable replies
on the client connection (`CLIENT REPLY OFF`) so the server isn't dominated by
socket write syscalls for `+OK` responses.

Environment
- Date: 2026-01-23
- Host: local Mac (loopback)

## Commands

HashDB (server):
```sh
./bin/redisserver -engine hashdb -dir $(mktemp -d) -addr :6380 \
  -cpuprofile /tmp/hashdb_noreply.pprof -cpuprofile-seconds 20 -cpuprofile-delay 2
```

TreeDB (server):
```sh
./bin/redisserver -engine treedb -dir $(mktemp -d) -addr :6380 \
  -cpuprofile /tmp/treedb_noreply.pprof -cpuprofile-seconds 20 -cpuprofile-delay 2
```

Client workload (same for both):
```sh
./bin/noreply_bench -addr 127.0.0.1:6380 -clients 32 -pipeline 128 -test-time 25s \
  -keyspace 100000 -value-size 128 -resp3=true -reply-off=true
```

## Large Value (1KB) Run

Additional no-reply profile run to isolate the large-value write path:

HashDB (server):
```sh
./bin/redisserver -engine hashdb -dir $(mktemp -d) -addr :6380 \
  -cpuprofile /tmp/hashdb_noreply_1kb.pprof -cpuprofile-seconds 20 -cpuprofile-delay 2
```

TreeDB (server):
```sh
./bin/redisserver -engine treedb -dir $(mktemp -d) -addr :6380 \
  -cpuprofile /tmp/treedb_noreply_1kb.pprof -cpuprofile-seconds 20 -cpuprofile-delay 2
```

Client workload (same for both):
```sh
./bin/noreply_bench -addr 127.0.0.1:6380 -clients 16 -pipeline 128 -test-time 25s \
  -keyspace 100000 -value-size 1024 -resp3=true -reply-off=true
```

Profile inspection:
```sh
go tool pprof -top -cum ./bin/redisserver /tmp/hashdb_noreply.pprof
go tool pprof -top -cum ./bin/redisserver /tmp/treedb_noreply.pprof
go tool pprof -top -cum ./bin/redisserver /tmp/hashdb_noreply_1kb.pprof
go tool pprof -top -cum ./bin/redisserver /tmp/treedb_noreply_1kb.pprof
```

## Observations

### HashDB
- No-reply throughput observed: ~4.90M SET/sec (clients=32, pipeline=128, 25s).
- With replies suppressed, the profile shows real work in:
  - `github.com/snissn/gomap/HashDB.(*HashDB).Put` (large fraction of cum time)
  - `github.com/tidwall/redcon.(*Reader).readCommands` (RESP parse / dispatch)
  - Go runtime scheduling/locking overhead (expected under high goroutine + socket load)

### TreeDB
- No-reply throughput observed: ~1.28M SET/sec (clients=32, pipeline=128, 25s).
- Hot path is dominated by cached-mode write path:
  - `github.com/snissn/gomap/TreeDB/caching.(*DB).set`
  - `github.com/snissn/gomap/TreeDB/caching.(*DB).appendWALInline`
  - memtable insertion (`HashSorted` put) + lock contention.

### Large Values (1KB)

- HashDB no-reply throughput observed: ~0.74M SET/sec (clients=16, pipeline=128, 25s).
  - Profile shows heavy value-copying/allocation (`runtime.memmove`, `runtime.memclrNoHeapPointers`)
    plus `HashDB.(*HashDB).Put`.
- TreeDB no-reply throughput observed: ~0.31M SET/sec (clients=16, pipeline=128, 25s).
  - Profile is dominated by cached-mode writes plus value-log appends:
    - `TreeDB/caching.(*DB).set`
    - `TreeDB/caching.(*DB).appendValueLogOne`
    - `TreeDB/internal/valuelog.(*Writer).Append` / append-buffer flush.

## Notes / Limits

- This measures server CPU under loopback client load. Even with replies off, the
  profile includes non-trivial runtime + netpoll overhead (socket reads, scheduling).
- To increase client count beyond ~32 on this host, the client may hit `ENOBUFS`
  (kernel socket buffer exhaustion). See macOS sysctls like:
  - `kern.ipc.maxsockbuf`
  - `net.inet.tcp.sendspace`
  - `net.inet.tcp.recvspace`
