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

### Baseline: redcon + Go map

`hashdb-redis-wrapper` also includes a `map` mode that uses a sharded Go builtin
map. This is useful as a rough "protocol + server wrapper ceiling" for this repo:

```bash
DBDIR=$(mktemp -d)
./bin/hashdb-redis-wrapper map "$DBDIR" :6380
```

### Sample Results (Local)

Env:

- Apple M3, 8 cores
- clients=16, keyspace=100000, value-size=128, test-time=10s
- `-resp3` + `-reply-off=true`

| Engine | Pipeline | RPS |
|---|---:|---:|
| map | 64 | 8,245,266.82 |
| map | 256 | 8,843,145.41 |
| map | 512 | 9,052,913.85 |
| hashdb | 64 | 6,233,215.96 |
| hashdb | 256 | 6,617,456.07 |
| hashdb | 512 | 6,938,040.39 |
| redis | 64 | 1,433,283.34 |
| redis | 256 | 1,395,026.31 |
| redis | 512 | 1,382,830.41 |

## Common Gotchas

- `ENOBUFS` / "no buffer space available" on loopback:
  - reduce `-clients` and/or `-pipeline`
  - increase OS socket buffer limits if you want to push harder
- This benchmark is **SET-only** by design. Use `cmd/unified_bench` for mixed
  read/write workloads and engine-level comparisons.
