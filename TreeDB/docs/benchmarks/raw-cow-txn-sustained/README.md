# Public raw COW transaction capacity and timing

**Result:** One client sustains 200,000 real durable two-key transactions. The
stock engine cannot sustain this 16-client workload. All six concurrent
cohorts stopped at accepted-prefix capacity, not measured memory exhaustion.
No transaction speedup, general service SLO, or complete engine qualification
is claimed.

## Exact boundary

Runtime main `1b710ee3f135c0666c246b7680a1e4709a246fad`, Go 1.26.11/linux-amd64
and two Go processors, default GC and engine limits. The opt-in
[public driver](../../../../scripts/raw_cow_txn_sustained/) used production
command-WAL durable ACK, explicit `cow_btree`, fixed-size account pairs, fresh
files, and ordinary automatic maintenance. Each started a real transaction,
read and staged two keys, and committed using ordinary `Commit`. Latencies are
closed-loop fixed-concurrency logical-operation latencies, including any
safe preassignment conflict/history retry and scheduler yield. No unknown or
capacity error was retried. The main comparison contains no added phase clocks;
the shorter client diagnostics do. Setup, explicit final checkpoint, audit,
checked Close and new-process reopen are separate.

## Stock result

| Case | Acknowledged | Foreground | p99 | p99.9 | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 client, pass 1 | 200,000 | 45.962 s | 1.220 ms | 3.008 ms | Passed, reopened |
| 1 client, pass 2 | 200,000 | 43.389 s | 0.819 ms | 2.572 ms | Passed, reopened |
| 1 client, pass 3 | 200,000 | 42.159 s | 0.677 ms | 2.466 ms | Passed, reopened |
| 16 disjoint, three runs | 16,644 / 8,058 / 25,114 | 1.283 / 0.518 / 1.952 s | Not scored | Not scored | Capacity refusal |
| 16 hot, three runs | 8,252 / 17,309 / 26,312 | 0.747 / 1.068 / 1.928 s | Not scored | Not scored | Capacity refusal |

The 1-client goodput was 4,351–4,744 successful transactions/s. All passed
under actual in-window stock automatic WAL maintenance, as established by the
counter and completion timestamp. This is not a claim about automatic
checkpointing of the whole visible DATA root. Final full checkpoint was
105–181 ms and checked Close 3.52–4.84 ms, separately. Process allocation
delta was 11.94–11.96 kB and 69.3 allocations per acknowledgement, including
background/runtime. Process peak RSS was about 84–90 MiB and the engine's
charged peak about 214 MiB; the two are different measures. Each solitary
transaction produced one raw and one total WAL frame, one completion group
and no collection timeout. Physical WAL file sync took 180–196 µs per ACK.
The apparent solo-bypass counter is not populated in this source; zero is
not evidence of missing singleton optimization.

The six concurrent rows were not shortened and reclassified as positive
performances. A separate checked reopen audited every exact balance and
key in all six failed stores. The number of logical installations and raw
mutation frames equals the acknowledged prefix in each; there was no extra
applied transfer in those samples. This does not make the generic capacity
sentinel safe to retry across other paths. Stock 16-client rows had no
independently held reader. The separate short held-reader control crossed
a real checked full checkpoint and verified its original cut, the current
balances and the fresh reopen.

## Client phases and capacity diagnosis

In separate 4,000-ACK short diagnostics (not a sustained pass), each shape
ran fresh uninstrumented and instrumented rows in ABBA order. Approximate
per-ACK means for the two instrumented rows:

| Shape | Begin / capture | Read / check | Stage | Commit | Production group-wait sum/ACK |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 disjoint | 9–12 µs | 2.8–3.3 µs | 0.39–0.43 µs | 205–215 µs | 177–184 µs |
| 16 disjoint | 179–197 µs | 9.2–10.1 µs | 0.36–0.38 µs | 635–730 µs | 579–672 µs |
| 16 hot | 80–85 µs | 2.2–2.3 µs | 0.30–0.34 µs | 529–569 µs | 495–532 µs |

Hot row clocks charge its failed attempts too. Disjoint ran about 6.6–7.7 ACKs
per durable group in the four short arms. Actual group sync and file-sync
work are separately recorded. The aggregate validation, writer hold/wait
and sync counters overlap client clocks and other requests; they are not
exclusive phases or p99 values. The existing public WriteSync breakdown
does not instrument this COW route. Short phase-instrumented and plain rows
vary, so there is no assertion of zero instrumentation overhead.

Three separate failure-only overlays reproduced the refusal with unchanged
limits. Each traced actual `Coordinator.ReserveAccepted(false)` at zero
free of 2,048 ordinary entries with the separate maintenance slot unoccupied.
The stack includes both ordinary mutation admission and the possible
durable-group marker reservation. An example refusal saw visible sequence
6,542, canonical LSN 7,565, stable WAL 7,553, and sealed DATA sequence/LSN 1/1.
After the clients joined, 6,545 acknowledged transactions exactly matched
6,545 COW installs/raw frames; durable/next LSN was 7,572/7,573, with no
active transaction handles or group errors. The group marker refusal occurs
**before** the mutation append callback. No memory-budget refusal was logged
in those traces. In other shapes the public capacity sentinel alone does not
declare whether a mutation was assigned.

[#5610](https://github.com/snissn/gomap/issues/5610) already owns the native
advisory scheduler; the held [draft #5613](https://github.com/snissn/gomap/pull/5613)
requires nonnil native custody, which raw transactions do not supply, so we
did not run a raw A/B on its ineligible path. [#5612/#5616](https://github.com/snissn/gomap/pull/5616)
owns the existing pre-assignment group marker. The next narrow test is
whether raw durable transaction admission can present the necessary original
resources before its write/append gates and reuse safe bounded backpressure.
No capacity bump, faster ACK shortcut, or competing scheduler was measured or
selected. We did not run the optional historical #5637 A/B: its inline Set
optimization does not apply to this Commit path, and comparing censored
concurrent runs or unattributed older source differences would not answer
the viability question.

## Retained data and replay

`results.tar.gz` (SHA-256
`f298729abee0b7bfdcaa23e80375d2122f5234a32449c198cf20fa00e652fc06`) contains
the exact per-process stock/calibration/phase JSON, stderr, separate reopen
receipts, plans/provenance bindings, and failure-only diagnostic logs. Debug
and test logs normalize local file locations; exact original log digests
are also retained. Primary raw measurement data is unmodified. Fixture
databases and executables are reproducible rather than embedded. Successful
reopen of a failed run's acknowledged prefix is explicitly not a passed timing
row. No primary row used a diagnostic overlay.

Run from a source-bound checkout with the driver supplied, Go 1.26.11 and a
quiet Linux test host. Use a new output directory for each invocation:

```sh
GOWORK=off go build -p 1 -o ./cow-txn-driver ./scripts/raw_cow_txn_sustained
hash=$(sha256sum ./scripts/raw_cow_txn_sustained/main.go | cut -d' ' -f1)
src=1b710ee3f135c0666c246b7680a1e4709a246fad
bin=TreeDB/docs/benchmarks/raw-cow-txn-sustained
GOMAXPROCS=2 python3 "$bin/campaign.py" --plan "$bin/plan.json" \
  --binary ./cow-txn-driver --source "$src" --driver-sha256 "$hash" \
  --go-build "$(command -v go)" --out ./raw-cow-stock
```

The stock runner retains and exits nonzero if a cohort fails. Use
`phase-plan.json` for separate instrumentation checks. For the failure-only
overlays, call `make_capacity_overlay.py` with the checkout root and a new
overlay output directory, then rebuild with the emitted Go `-overlay`
JSON. The archive binds the diagnostic argv and changed overlay source
hashes. See [#5652](https://github.com/snissn/gomap/issues/5652); this experiment
does not close [#5145](https://github.com/snissn/gomap/issues/5145) or
[#4879](https://github.com/snissn/gomap/issues/4879).
