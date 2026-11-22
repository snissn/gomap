# Sustained public raw COW transactions

This opt-in driver measures concurrent two-key read-modify-write through
`DB.NewConditionalTxn`, `Get`, `Set`, and ordinary `Commit`. It uses
`ProfileCommandWALDurable` with the explicit `cow_btree` mode, the profile's
normal grouped or solo durability route, and stock background maintenance and
resource limits. There are no alternate ACK classes. For semantics, see the
[raw transaction contract](../../TreeDB/docs/spec/raw-cow-transactions.md).

From the root checkout, with the experiment's pinned Go, run:

```sh
out=$(mktemp -d)
GOWORK=off go build -p 1 -o "$out/driver" ./scripts/raw_cow_txn_sustained
GOMAXPROCS=2 "$out/driver" -dir "$out/db" -clients 16 -pairs 128 -ops 1000 \
  -layout disjoint > "$out/run.json"
GOMAXPROCS=2 "$out/driver" -dir "$out/db" -audit-receipt "$out/run.json" \
  > "$out/reopen.json"
```

Use a nonexistent database path and a fresh process per sample. The driver
seeds known balances and checkpoints before timing. One-way unit transfers
make missing, duplicated or partly applied transactions visible in both exact
account values. `disjoint` uses private account pairs for each client; `hot`
shares the same pair set. With `-pairs 1` all hot clients touch one pair. Each
client runs its fixed logical workload closed-loop. Only exact pre-assignment
conflict or history-expiry errors from Commit receive a bounded fresh
transaction attempt; unexpected, exhausted or ambiguous outcomes fail the
sample. The latency clock starts before the first attempt and includes failed
attempts and scheduler yields. These are **fixed-concurrency logical-op**
latencies, not a fixed-arrival service SLO.

The JSON records all worker counts, foreground elapsed and nearest-rank
p50/p99/p99.9/max, cumulative production counters around the foreground, and
total process allocation delta. Peak RSS is Linux-only and process-wide,
including untimed setup. Production writer wait, hold, validation, group wait
and WAL sync counters are cumulative and overlap; never sum them as exclusive
parts of foreground elapsed. `-phases` enables additional client Begin,
read/RMW, Set and Commit clocks for diagnosis only. Do not mix its measurements
with performance-comparison rows. These are standalone evidence, not benchprof
inputs.

Setup and final explicit checkpoint, live audit, checked Close and fresh audit
are separate from foreground. `-hold-reader` keeps an independent old cut
through workload and the checked explicit checkpoint, verifies its original
values and checks its Close. This can change retention costs; compare held and
ordinary runs only as separate fixtures. Reopening uses the supported COW
owner and makes no application writes; the explicit COW `ReadOnly` option is
not supported. A recorded automatic checkpoint completion inside the active
window is not proof that it covered the whole DATA cut or overlapped a
particular ACK. On the external command-WAL route it can be WAL cleanup only.
We check logical publications and raw mutation appends separately from total
physical LSN progression, which also includes durable-prefix barrier frames.

The [first campaign](../../TreeDB/docs/benchmarks/raw-cow-txn-sustained/) records
the raw JSON output, digest-bound inputs and interpretive limits. This driver
does not qualify the full native engine or Dgraph/Badger.
