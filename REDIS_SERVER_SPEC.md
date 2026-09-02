# Redis/Valkey-Compatible Server Spec (TreeDB + HashDB via redcon)

This repo historically contained a Redis wrapper under `HashDB/redisserver/` that was used
for proof-of-concept and for driving `redis-benchmark`. TreeDB is now the primary, more
advanced engine, but HashDB remains useful (often faster for random point workloads).

This spec describes a single Redis-compatible server implementation that can run against
either engine (TreeDB or HashDB) behind `github.com/tidwall/redcon`.

Status: design/spec only (no code in this document).

## Compatibility Target

There is no single, formal "Redis API spec". In practice, compatibility means:

- RESP wire protocol compatibility (RESP2 minimum; optionally RESP3/HELLO later).
- Command name + argument parsing compatibility.
- Command semantics compatibility (atomicity guarantees, error strings, edge cases).

For this repo, treat **Valkey** as an equal-to-Redis canonical reference implementation
for command behavior (and track Redis OSS behavior where it differs).

## Goals

- Provide one server entrypoint that can run with `-engine=hashdb` or `-engine=treedb`.
- Be compatible with common Redis clients and `redis-benchmark` for string workloads:
  `SET`, `GET`, `MSET`, `MGET`, `DEL`, `EXISTS`, `INCR`, `PING`.
- Keep the server thin: map Redis commands onto engine operations; do not reimplement Redis.
- Allow optional benchmark-focused optimizations (pipelined SET batching) without forcing
  correctness regressions for normal usage.
- Reuse existing engine adapters where possible (prefer `kvstore` adapters).

## Non-goals

- Full Redis compatibility (data types like hashes/lists/sets/zsets, Lua, pubsub, cluster,
  replication, ACL/auth, transactions, RESP3).
- Redis persistence formats (AOF/RDB). Engine-native persistence is used.
- Multi-database support (`SELECT`) beyond DB0 for the MVP (it is a reasonable Phase 2).

## Multi-Backend Modes (HashDB + TreeDB Together)

The simplest and most Redis-like model is **one server instance = one backend engine**.
If you want both engines available at the same time, make the routing explicit.

Supported design options (in increasing complexity):

1) **Separate ports / processes (recommended default)**
   - Run two servers (e.g. `:6380` HashDB, `:6381` TreeDB).
   - Simple operational model; no surprises; best isolation.

2) **Redis databases (`SELECT`) as namespaces**
   - DB0 -> HashDB instance, DB1 -> TreeDB instance (or vice versa).
   - Each logical DB has its own engine handle and (usually) its own on-disk directory.
   - Pros: uses an existing Redis concept; explicit per-connection selection.
   - Cons: many client stacks assume DB0 only; cluster mode historically only DB0.
   - Suggested configuration shape:
     - `-db-map "0=hashdb,1=treedb"`
     - plus per-engine directories under `-dir`, e.g. `dir/db0/` and `dir/db1/`.
   - Semantics:
     - Commands operate only within the selected DB.
     - Cross-DB commands (`MOVE`, `SWAPDB`) can be `-ERR unsupported`.

3) **Key-prefix routing**
   - Example: keys beginning with `h:{...}` go to HashDB, `t:{...}` go to TreeDB.
   - Pros: single endpoint; selection is explicit in the key.
   - Cons: application-visible; KEYS/SCAN become "scan both + merge" unless you scope
     patterns; cross-namespace renames are awkward.

4) **Read-through / write-through cache**
   - Example: TreeDB is durable primary; HashDB is a hot cache.
   - Pros: potentially best of both.
   - Cons: correctness and invalidation complexity; needs careful design for deletes,
     TTLs, and atomic operations. Treat as a separate project.

This spec assumes mode (1) for correctness and simplicity, and allows (2) later if needed.

## Proposed Layout

Introduce a new, unified server package and CLI:

- `internal/redisserver/`
  - Command routing and connection-state batching logic.
  - A small engine abstraction (primarily `kvstore.DB` + optional capabilities).
- `cmd/redisserver/`
  - Binary entrypoint (flags/env, opens engine, starts redcon server).

The legacy `HashDB/redisserver/` can remain for now, but should be considered deprecated
once the unified server exists.

## Engine Abstraction

Prefer building the server around `kvstore.DB` and its optional capability interfaces:

- Required:
  - `kvstore.DB` (`Get`, `Set`, `Delete`, `Close`, `Name`)
- Optional (detected via type assertions):
  - `kvstore.Batcher` for efficient `MSET` and benchmark batching.
  - `kvstore.Syncer` for durability variants (if exposed via custom commands).
  - `kvstore.StatsProvider` for `INFO`.
  - `kvstore.RangeScanner` for ordered scans (TreeDB).
  - `kvstore.ForEacher` for full iteration (HashDB).

Command availability should be capability-gated:
- If an engine lacks a capability (e.g. ordered iteration for HashDB), return a Redis-like
  `-ERR unsupported` (or implement a slower best-effort fallback when reasonable).

Add a small server-local interface for operations not in `kvstore`:

```go
// internal/redisserver/engine.go
type Opener func(dir string) (kvstore.DB, error)

// Optional: used by SAVE/BGSAVE. Both TreeDB and HashDB adapters already expose this today.
type Checkpointer interface{ Checkpoint() error }

// Optional: used by COMPACT/BGREWRITEAOF.
type Compactor interface{ Compact() error }
```

Notes:
- TreeDB already exposes `Checkpoint()` (see `TreeDB/public.go`) and compaction methods
  (`CompactCandidates`, `CompactIndex`, etc.). The wrapper used by the Redis server can
  decide which compaction is appropriate (see "Admin commands").
- HashDB exposes `Sync()/Flush()/Compact()/Clear()` style operations; if not available via
  adapters, the server can implement some admin commands by close+reopen+dir wipe.

## CLI / Configuration

Create a new CLI with flags and environment-variable overrides:

- `-addr` (default `:6380`)
- `-dir` (required; engine data directory root)
- `-engine` (`hashdb` | `treedb`) (MVP: single-backend mode)
- `-db-map` (optional Phase 2): map Redis DB numbers to engines, e.g. `0=hashdb,1=treedb`

Benchmark-oriented toggles:

- `-batch-sets` (default false): enable per-connection SET batching (see below).
- `-batch-size` (default 16): number of SETs per commit when batching is enabled.
- `-batch-flush-on-nonset` (default true): flush pending SET batch before executing any
  non-SET command (required for correct semantics).

Engine-specific options (minimal set; add only as needed):

- HashDB:
  - `-hashdb-shards` (default 0 = engine default; also allow env `HASHDB_SHARDS`)
  - `-hashdb-compression` (default false, benchmark-friendly)
- TreeDB:
  - `-treedb-mode` (`cached` | `backend`, default `cached`)
  - `-treedb-flush-threshold` (default something conservative, e.g. 64MiB)
  - `-treedb-value-log-threshold` (default 0 = TreeDB default inline threshold)
  - `-treedb-disable-value-log` (default false; legacy WAL framing when true)

Mode default (TreeDB):
- The Redis server defaults to **mode3** semantics: value-log enabled + journal enabled.
- To force legacy behavior (inline values only), set `-treedb-disable-value-log=true`.

## redcon Server Model

Use `redcon.ListenAndServe(addr, handler, accept, closed)`:

- `handler(conn, cmd)` routes commands and writes responses.
- `accept(conn)` initializes per-connection state (batch buffers, metrics).
- `closed(conn, err)` flushes any pending batched writes and releases resources.

Command name handling:
- Treat command names case-insensitively: normalize `strings.ToUpper(string(cmd.Args[0]))`.

Binary safety:
- Keys and values are treated as raw `[]byte` (RESP bulk strings).
- No UTF-8 assumptions.

## Command Set (MVP)

Target: enough for `redis-benchmark -t set,get` and basic manual usage.

### Client Compatibility "Glue" (Highly Recommended)

Many real Redis/Valkey client libraries send a small set of commands on connect. Even if
we don't implement the full feature, it's worth implementing no-op/safe subsets to avoid
clients failing at startup.

Implement as minimal, server-local state + best-effort replies:

- `AUTH` (if no password configured, reply `+OK`; otherwise enforce)
- `SELECT <db>` (if only DB0 supported, accept `SELECT 0` and error on others)
- `CLIENT SETNAME/GETNAME/ID/INFO` (store name in connection context)
- `COMMAND` / `COMMAND INFO` / `COMMAND COUNT` (report supported commands)

### Connection / Introspection

- `PING [message]`
  - No args: `+PONG`
  - One arg: bulk reply with the same payload
- `ECHO <message>`: bulk reply with the same payload
- `QUIT`: `+OK` then close connection
- `HELLO [proto]`: accept `HELLO 2` / `HELLO 3` and reply `+OK`
- `CLIENT REPLY ON|OFF|SKIP`: supported to allow benchmark modes without replies
- `INFO`:
  - Return a Redis-style bulk string with a minimal set of sections/fields.
  - Include engine name and `Stats()` output if available.

### String KV operations

- `SET <key> <value>`
  - Reply `+OK` on success.
  - (Optional later) Support `SET key val NX|XX` and `GET` modifier; initially return
    `-ERR syntax error` if extra args are supplied.
- `GET <key>`
  - Return bulk string or null.
- `SETNX <key> <value>`
  - Set only if key does not exist. Return 1 if set, 0 otherwise.
- `GETSET <key> <value>`
  - Atomically set and return the old value.
- `GETDEL <key>`
  - Return the old value and delete the key.
- `DEL <key> [key ...]`
  - Return integer count of keys removed.
  - If engine `Delete()` cannot indicate "existed", implement count conservatively:
    - Preferred: `Has()` if available (from `kvstore.Haser`), then `Delete()`.
    - Fallback: `Get()` first (best-effort; racey under concurrent writers).
- `EXISTS <key> [key ...]`
  - Return integer count of keys existing.
  - Use `Has()` if supported, else `Get()!=nil` fallback.
- `MSET <key> <value> [key value ...]`
  - Use `kvstore.Batcher` if available; else loop `Set`.
  - Reply `+OK`.
- `MGET <key> [key ...]`
  - Reply array with bulk-or-null entries.
- `INCR <key>`
  - Parse current value as base-10 int64 (missing = 0).
  - Write back incremented value, reply integer.
  - Required semantics: atomic per key.
  - Implementation:
    - Preferred: use engine atomic update if available (HashDB has `Update`).
    - Fallback: server-side per-key lock (sharded locks) + `Get`/parse/`Set`.
      This preserves Redis-like atomicity within the server process.
- `INCRBY/DECR/DECRBY`
  - Same semantics as INCR, with integer deltas.
- `APPEND/STRLEN/GETRANGE/SETRANGE`
  - String mutations and slicing.
- `RENAME/RENAMENX`
  - Key rename with optional NX guard.
- `TYPE`
  - Return `string` or `none`.

### Admin / Maintenance (Benchmark-friendly)

These commands are primarily for test harnesses and benchmarking; implement a sensible
subset that works for both engines:

- `SAVE`
  - If engine implements `Checkpointer`, call `Checkpoint()` and reply `+OK`.
  - Else fallback to `Close()` + reopen (only if safe/desired), or reply `-ERR unsupported`.
- `BGSAVE`
  - Run `Checkpoint()` in a goroutine; reply `+Background saving started` (Redis-like).
- `FLUSHDB` / `FLUSHALL`
  - Implement as a full reset of the engine directory:
    1) Block all connections/commands (global server write lock).
    2) Close engine.
    3) Remove contents of `-dir` (or remove+recreate).
    4) Reopen engine with the same config, swap into server atomically.
    5) Reply `+OK`.
  - This is slow but consistent and works for both engines.
  - Optional fast-path: if engine exposes `Clear() error`, call it instead.
- `BGREWRITEAOF`
  - For Redis, this rewrites AOF; here it is repurposed to mean "background compaction".
  - If engine implements `Compactor`, run `Compact()` in a goroutine and reply with
    Redis-like status.
  - For TreeDB, define what "compact" means (see below).
- `COMPACT` (custom extension; synchronous)
  - Run compaction synchronously and reply `+OK` on success.

TreeDB compaction choice:
- Minimum viable: call `CompactCandidates` with conservative defaults (or a no-op).
- Optional: support `COMPACT INDEX` to call `CompactIndex` / `VacuumIndexOnline`.

## Optional: Scan/Iteration Commands

These are not required for `redis-benchmark` set/get scenarios but are useful for manual
inspection and for TreeDB strengths (ordered scans).

### `KEYS <pattern>`

- Implement `KEYS *` and a fast-path for simple prefix patterns: `prefix*` where `prefix`
  contains no glob metacharacters.
  - TreeDB: use `RangeScanner.Iterator(start=prefix, end=prefixNext(prefix))`.
  - HashDB: use `ForEach()` and filter with glob matching.
- For complex patterns, use a glob matcher (e.g. `github.com/tidwall/match`).

### `SCAN <cursor> [MATCH pattern] [COUNT n]`

Implement a minimal RESP2-compatible SCAN:

- Cursor is an opaque string representing the last returned key (or empty for start).
- TreeDB:
  - Use ordered iteration starting at `(lastKey + 0x00)` (lexicographic next).
  - Produce up to `COUNT` keys matching pattern, return next cursor (last key returned),
    or `0` when iteration is complete.
  - Provide an optimized path when `MATCH` is `prefix*` (same prefix range trick).
- HashDB:
  - Either return `-ERR unsupported` (acceptable if we document it),
    or implement a slow SCAN by snapshotting keys (alloc-heavy).

### `DBSIZE`

- Return total key count (implemented via engine stats if available, else full scan).

## SET Batching (Pipelining Optimization)

The legacy HashDB Redis wrapper included an optimization for `redis-benchmark -P16`:
buffer SETs per connection and commit in groups of 16.

In the unified server, keep this as an explicit opt-in (`-batch-sets`) and make it safe:

Per-connection state:

```go
type connState struct {
  pending int              // number of queued SETs since last commit
  batch   kvstore.Batch    // only when engine supports kvstore.Batcher
  // or: pendingItems []hashdb.Item for a HashDB-specific fast path
}
```

Rules:
- On `SET` when batching enabled:
  - enqueue into the batch
  - if `pending == batchSize`, commit the batch, reply `+OK` batchSize times, reset state
- Before executing any non-SET command, flush pending SETs first (commit + respond).
- On connection close, flush pending SETs (commit) and (optionally) log errors.

Important: response ordering
- When batching is enabled, the server delays responses to SET until the batch commits.
  This is acceptable for pipelined workloads, but not for interactive clients; that is why
  batching must be opt-in and documented as benchmark-only.

## Concurrency / Engine Swapping

FLUSHDB/FLUSHALL requires replacing the underlying engine instance safely.

Server structure:

- Keep an `atomic.Pointer[kvstore.DB]` (or equivalent) to the active engine.
- Protect destructive operations (reset/close/reopen) with a global mutex.
- In handlers:
  - Load the engine pointer once at the top of the command handler.
  - If a reset is in progress, either block or return `-ERR busy` (blocking is simpler).

Connection batches must not outlive an engine swap:
- When swapping engines, ensure no connection is still holding a batch bound to the old
  engine. The simplest approach is to:
  - block all commands with a global lock during reset, and
  - force-close all active connections (optional), or
  - accept that per-connection close hooks will fail and drop pending writes (not ok).
Preferred: maintain a connection registry so FLUSHDB can close connections cleanly.

## Error Mapping / RESP Conventions

- Wrong-arity: `-ERR wrong number of arguments for '<cmd>'`
- Unsupported: `-ERR unsupported`
- Parse failures (e.g. INCR on non-int): `-ERR value is not an integer or out of range`
- Unknown command: `-ERR unknown command '<cmd>'`

Use RESP types as Redis does:
- Simple strings for OK/PONG/status.
- Bulk strings for values and INFO payload.
- Integers for counts and INCR.
- Arrays for MGET/SCAN responses.

## Implementation Checklist

1) Create `cmd/redisserver` with flag parsing and engine open.
2) Create `internal/redisserver`:
   - `server.go` (redcon integration, handler, accept/closed hooks)
   - `commands.go` (command implementations)
   - `engine_registry.go` (engine name -> opener)
3) Implement MVP command set: PING/SET/GET/DEL/MSET/MGET/EXISTS/INCR/INFO/QUIT.
4) Add optional batching (`-batch-sets`) with correct flush-on-nonset and close flush.
5) Implement SAVE/BGSAVE (Checkpoint).
6) Implement FLUSHDB via close+wipe+reopen with global lock + connection management.
7) (Optional) Implement KEYS/SCAN (TreeDB first, HashDB best-effort).

## Tests

Add integration-style tests under `internal/redisserver/`:

- Start the server on `127.0.0.1:0` (ephemeral port), open an engine in a temp dir.
- Use a tiny RESP2 client in tests (net.Conn + manual encoding/decoding) to avoid external
  dependencies.

Test cases:

- `TestSETGET` (both engines): SET, GET returns exact bytes.
- `TestMSETMGET` (both engines).
- `TestINCR` (both engines): missing=0, increments, non-int errors.
- `TestBatchSetsFlushOnClose` (both engines when batching enabled): write N not divisible
  by batch size, close connection, reopen new conn, GET must see all keys.
- `TestFLUSHDB` (both engines): data cleared and subsequent GET returns null.

Benchmark sanity:
- Update the legacy `HashDB/benchmark` runner to call the new unified server and include
  `treedb` as an engine option.

Real-world sanity:
- Smoke test with `redis-cli` and `valkey-cli` against the server for the commands we claim
  to support.
- Consider vendoring/running a small subset of the upstream Redis/Valkey test suite over
  TCP as a long-term compliance check (command semantics are the real spec).

## Compatibility Roadmap (What We Can Offer Now vs Later)

This section maps Redis/Valkey feature areas to implementation complexity given the
current repo architecture (TreeDB/HashDB are "bytes KV stores" and do not natively store
Redis data types).

### Easy (KV + light server-side state)

- **Client glue:** `AUTH` (optional), `SELECT 0`, `CLIENT SETNAME/GETNAME/ID`, `COMMAND`.
- **More string ops:** `SETNX`, `GETSET`, `GETDEL`, `INCRBY/DECR/DECRBY`, `APPEND`,
  `STRLEN`, `GETRANGE`, `SETRANGE`.
- **Key ops:** `UNLINK` (alias to DEL), `TYPE` (always `string`), `RENAME/RENAMENX`
  (copy+delete semantics), `DBSIZE` (implemented via scan; O(n)).
- **SCAN/KEYS:** TreeDB fast-path (ordered iteration), HashDB best-effort (ForEach).

### Medium (needs metadata + background work or careful atomicity)

- **Expiry/TTL:** `EXPIRE/PEXPIRE/EXPIREAT`, `TTL/PTTL`, `PERSIST`, `SETEX`, `GETEX`.
  - Requires durable per-key expiration metadata + read-path enforcement + background
    active expiration. (Also affects SCAN/DBSIZE correctness.)
- **MULTI/EXEC (transactions):**
  - Implement per-connection queueing and apply via engine batches.
  - WATCH/optimistic concurrency is significantly harder without a CAS/version API.
- **Pub/Sub:** `PUBLISH`, `SUBSCRIBE`, pattern subscriptions.
  - Implementable in-memory at the server layer (does not require DB support).
- **SLOWLOG/MONITOR:** server-side observability (not engine features).

### Hard (requires substantial new layers or engine capabilities)

- **Redis data structures:** hashes, lists, sets, sorted sets, streams.
  - Requires an internal encoding/representation and often efficient iteration by
    secondary keys (e.g. zset score ordering, stream IDs).
  - Practical implication: a serious push toward data-type parity almost certainly means
    TreeDB as the primary backend (or a multi-backend mode where type-heavy keys route to
    TreeDB).
- **Lua scripting (`EVAL`):** requires a Lua VM and strict atomicity/timeout semantics.
- **Replication (PSYNC) / Cluster:** requires protocol-level state machines and a lot of
  operational behavior beyond a simple KV engine.
- **Modules:** out of scope unless we design a module API.

## Appendix: Redis-Compatible Server Landscape (Non-Exhaustive)

These projects are often described as "Redis-compatible" in the sense that they speak
RESP and implement subsets/supersets of the Redis/Valkey command surface:

- **Redis OSS** (the canonical reference historically).
- **Valkey** (Redis OSS fork; treat as canonical for this repo).
- **Dragonfly**, **KeyDB**, **Garnet** (high-performance in-memory alternatives).
- **Kvrocks** (RocksDB-backed; Redis protocol surface over an embedded KV store).
- **Pika** (Redis-like interface, different internal architecture).

This list is not meant to be complete; it exists to frame expectations around "Redis
compatibility" being a spectrum rather than a binary property.
