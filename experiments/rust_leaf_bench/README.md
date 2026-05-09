# Rust Leaf Page Smoke Benchmark

This is a small TreeDB leaf-page experiment. It ports the benchmark shapes from
`TreeDB/node` into standalone Go, C, and Rust smoke binaries so we can separate
the production TreeDB Go path from narrower matched-algorithm comparisons.

The goal is smoke-test signal, not a production Rust dependency.

## What It Covers

- Builder smoke cases matching:
  - `BenchmarkAddLeafEntryWithPrefix_NoPrefix`
  - `BenchmarkAddLeafEntryWithPrefix_PrefixHeavy`
  - `BenchmarkAddLeafEntryWithPrefix_PrefixLight`
- Search smoke cases matching:
  - `BenchmarkSearchLeaf_Columnar_FixedBE8`
  - `BenchmarkSearchLeaf_Columnar_Variable16`
  - `BenchmarkSearchLeaf_PrefixV2`
  - `BenchmarkSearchLeaf_ColumnarPrefixV2`

The standalone Go, C, and Rust code all mirror TreeDB constants such as 4 KiB
pages, 16-byte page headers, 2-byte directory entries, 16-entry prefix restart
blocks, 16-byte value pointers, and the same key/value sizes used by the Go
microbenchmarks.

## Run

From the repo root:

```sh
experiments/rust_leaf_bench/run_leaf_smoke.sh
```

The script runs the production Go microbenchmarks, runs the matched standalone
Go smoke binary, compiles/runs the matched C smoke binary, runs the Rust release
binary, and prints a compact comparison table. It also writes raw output files
under `/tmp`.

Useful knobs:

```sh
BENCHTIME=2s COUNT=5 experiments/rust_leaf_bench/run_leaf_smoke.sh
LEAF_BUILD_ITERS=2000000 LEAF_SEARCH_ITERS=5000000 experiments/rust_leaf_bench/run_leaf_smoke.sh
CC=clang CFLAGS="-O3 -std=c11 -Wall -Wextra" experiments/rust_leaf_bench/run_leaf_smoke.sh
```

## Profile Go And Rust

To regenerate matched Go/Rust perf profiles without the C row:

```sh
experiments/rust_leaf_bench/profile_leaf_go_rust.sh
```

The profiler builds the standalone matched Go binary, builds Rust release with
frame pointers and the `profile-attribution` feature, then records one `perf`
profile per selected case. It writes a Markdown summary plus raw `perf.data`,
`perf.report.txt`, and stdout files under `/tmp`.

Useful knobs:

```sh
CASES="search/prefix_v2 search/columnar_prefix_v2" experiments/rust_leaf_bench/profile_leaf_go_rust.sh
LEAF_SEARCH_ITERS=50000000 PERF_FREQ=1997 experiments/rust_leaf_bench/profile_leaf_go_rust.sh
GO_PROFILE_GOGC=100 experiments/rust_leaf_bench/profile_leaf_go_rust.sh
OUT_DIR=/tmp/leaf_profiles experiments/rust_leaf_bench/profile_leaf_go_rust.sh
```

There is also a Go-only diagnostic probe for fixed-width columnar keys:

```sh
LEAF_CASE=search/columnar_fixed_be8_direct LEAF_SEARCH_ITERS=20000000 go run ./experiments/rust_leaf_bench/matched_go
```

That probe bypasses the generic `columnarKeyAt` slice-return path and compares
the 8-byte key in place. It is intentionally not part of the default
apples-to-apples table.

## Caveats

- The matched Go, C, and Rust paths are intentionally standalone and do not FFI
  into TreeDB.
- The benchmark shapes and constants match the Go benches, but the standalone
  fixture generator is deterministic rather than a byte-for-byte clone of Go
  `math/rand`.
- The most useful reads are `c/mgo`, `r/mgo`, and `r/c`, because they control
  for the narrower smoke implementation better than comparing C or Rust directly
  to production Go.
- The profile script uses a Rust-only `profile-attribution` feature to prevent
  selected hot functions from being inlined away in `perf` reports. Use
  `run_leaf_smoke.sh` for throughput numbers.
- The profile script defaults Go to `GOGC=off` to keep setup-time garbage
  collection out of the smoke profiles. Set `GO_PROFILE_GOGC=100` to profile
  with Go's normal collector setting.
- Treat small deltas as noise. This script is for quickly spotting large,
  interesting differences worth investigating in the Go implementation.
