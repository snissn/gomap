# Dgraph MVCC closeout evidence

This directory indexes the compact, reproducible evidence for gomap #3673.
The measured code commit is
`2f0a687f048ece277ab039303f6e28a1a7906bcb`; the base commit for paired
no-regression gates is
`f9c9b2a37838909d0e669818cfa2840c0a8d5f85`. The following evidence-only
commit changes no production or benchmark code. Dgraph must still pin the
first merged-main descendant that contains the closeout PR, not this worker
commit.

## Host and correctness

- host: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-124-generic`;
- CPU: 11th Gen Intel Core i5-11400F, 6 cores / 12 threads;
- Go: `go1.26.0 linux/amd64`;
- timing CPU: `0`, `GOMAXPROCS=1`, `GOWORK=off`;
- `go vet ./...` and `go test ./... -count=1`: PASS;
- `go test -race ./TreeDB/mvcc/... -count=1`: PASS;
- durable commit/prune crash tests, `-count=3`: PASS;
- codec `FuzzRoundTrip` and `FuzzDecodeNeverPanics`, 10 seconds each: PASS.

## Performance gates

The raw-path gate used seven alternating sequential samples per revision,
`-benchtime=2s`, a 5% median timing ceiling, no allocation increase, and a
`B/op` increase ceiling of the smaller of 1% or 64 bytes:

```sh
BASELINE_HASH=f9c9b2a37838909d0e669818cfa2840c0a8d5f85 \
CANDIDATE_HASH=2f0a687f048ece277ab039303f6e28a1a7906bcb \
RUNS=7 BENCHTIME=2s CPUSET=0 GOWORK=off \
OUT_DIR=/mnt/fast4tb/gomap-3673-evidence/raw-path-2f0a687 \
./scripts/mvcc_raw_path_gate.sh
```

Result: PASS for raw point, batch, snapshot, iterator, and durable synced write
rows. See [raw-path-summary.md](raw-path-summary.md) and its machine-readable
companion `raw-path-summary.json`.

The adapter-overhead gate used the same samples and thresholds, plus a 2x
candidate MVCC/direct ceiling. It always captured candidate MVCC CPU profiles:

```sh
BASELINE_HASH=f9c9b2a37838909d0e669818cfa2840c0a8d5f85 \
CANDIDATE_HASH=2f0a687f048ece277ab039303f6e28a1a7906bcb \
RUNS=7 BENCHTIME=2s PROFILE_BENCHTIME=1s CPUSET=0 GOWORK=off \
OUT_DIR=/mnt/fast4tb/gomap-3673-evidence/adapter-overhead-2f0a687 \
./scripts/mvcc_adapter_overhead_gate.sh
```

Result: PASS. Candidate MVCC/direct medians were 1.042x for commit, 1.119x for
get, and 1.109x for all-version iteration. See
[adapter-overhead-summary.md](adapter-overhead-summary.md) and
`adapter-overhead-summary.json`.

## Durability-matched matrix

```sh
CANDIDATE_HASH=2f0a687f048ece277ab039303f6e28a1a7906bcb \
RUNS=5 BENCHTIME=750ms CPUSET=0 GOWORK=off \
OUT_DIR=/mnt/fast4tb/gomap-3673-evidence/closeout-matrix-2f0a687 \
./scripts/mvcc_closeout_matrix.sh
```

Result: all 24 exact benchmark rows and all required metrics were present for
five samples in durable-sync, WAL-on relaxed, and WAL-off relaxed classes. The
ten timed processes used 100.64 seconds user and 6.85 seconds system CPU in
aggregate. Maximum RSS was 622,068 KiB for a duration-calibrated regular
process. The five one-operation prune processes used 82,624-91,004 KiB, so the
622 MiB headline must not be attributed to one prune fixture. Prune delete
write amplification was 0.829 in every row. See
[closeout-matrix-summary.md](closeout-matrix-summary.md) and
`closeout-matrix-summary.json`.

The three `OUT_DIR` paths above contain the retained raw logs, environment and
process snapshots, binary checksums, and profiles on the measurement host.
Large raw logs, benchmark binaries, and binary CPU profiles are deliberately
not committed.
