# Redis Protocol Benchmarking (Preferred)

This doc describes the preferred way to benchmark `gomap` behind a Redis-compatible
server wrapper.

## Why Not `redis-benchmark` For Peak Throughput?

`redis-benchmark` is great for "normal" Redis usage, but for peak throughput it can
become client/reply bound:

- every request requires a reply
- server time can be dominated by syscall write / TCP ACK behavior
- clients can become the bottleneck before the engine hot path is saturated

For measuring the write ingest ceiling, we prefer a **no-reply** workload.

## Preferred Method: `noreply_bench` (RESP3 + CLIENT REPLY OFF)

`cmd/noreply_bench` is a tiny benchmark driver that:

- sends `HELLO 3`
- sends `CLIENT REPLY OFF`
- sends a large pipelined stream of `SET` commands
- does not read responses

This is a better approximation of "how fast can the server accept writes" than
reply-on benchmarks.

### Build

Build the Redis wrapper and the benchmark client:

```bash
make build-hashdb
go build -o ./bin/noreply_bench ./cmd/noreply_bench
```

### Run (HashDB)

Start the HashDB Redis wrapper:

```bash
DBDIR=$(mktemp -d)
./bin/hashdb-redis-wrapper hashdb "$DBDIR" :6380
```

In another terminal, run the fast benchmark (2x CPU cores is a good baseline):

```bash
CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
CLIENTS=$((CORES*2))

./bin/noreply_bench \
  -addr 127.0.0.1:6380 \
  -clients "$CLIENTS" \
  -pipeline 512 \
  -test-time 10s \
  -keyspace 100000 \
  -value-size 128 \
  -resp3 \
  -reply-off=true \
  -label "hashdb_c${CLIENTS}_p512"
```

Recommended sweep (same server, repeat each 2-3x and take median):

- `-pipeline 64`
- `-pipeline 256`
- `-pipeline 512`

### Compare To Redis

Start Redis in a comparable "no durability" mode:

```bash
DBDIR=$(mktemp -d)
redis-server --port 6380 --dir "$DBDIR" --save "" --appendonly no
```

Then run the same `noreply_bench` command (just change the `-label`).

## Common Gotchas

- `ENOBUFS` / "no buffer space available" on loopback:
  - reduce `-clients` and/or `-pipeline`
  - increase OS socket buffer limits if you want to push harder
- This benchmark is **SET-only** by design. Use `cmd/unified_bench` for mixed
  read/write workloads and engine-level comparisons.

