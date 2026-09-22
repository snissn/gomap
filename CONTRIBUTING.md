# Contributing

## TL;DR

- Format: `make fmt`
- Tests: `make test` (plus `make test-race` optionally)
- Bench: `make unified-bench && ./bin/unified-bench`
- Bench analysis: `make benchprof && ./bin/benchprof -profiles-dir <dir>`

## Scope / Philosophy

This repo is a dev playground for two storage engines (HashDB + TreeDB) and benchmark tooling.
“Tests are the north star”: changes should keep `go test ./...` passing and extend tests for new behavior.

## Requirements

- Go `1.26+` (see `go.mod`; CI installs the version declared there)
- Linux/macOS recommended (TreeDB is Unix-focused due to mmap/locking specifics)

## Common Commands

- `make help`
- `make fmt`
- Optional local hook: `make hooks` (runs `gofmt` on staged `.go` files at commit time)
- `make test`
- `make vet`
- `make tidy`
- `make build`
- `make unified-bench && ./bin/unified-bench`
- `make benchprof`
- Optional (slow): `make test-race`

## CI

GitHub Actions runs with `actions/setup-go` using the root `go.mod` as the version file.

- Non-TreeDB root packages: `go vet` + modulo `go test` shards (Linux + macOS)
- TreeDB: separate vet jobs and weighted test shards (Linux, macOS, Windows), plus weighted race shards (Linux)
- `cmd/unified_bench` `go vet` + `go test ./...` (Linux + macOS)
- HashDB `go vet` + `go test ./...` (Windows)
- Manual: `-race` runs (workflow dispatch)

## Code Guidelines

- Keep commits small and reviewable.
- Prefer clear naming and simple control flow over cleverness.
- Keep the intended stable surface small:
  - `github.com/snissn/gomap/TreeDB` (package `treedb`)
  - `github.com/snissn/gomap/HashDB` (package `hashdb`)
  - See `docs/API_STABILITY.md`.
- When changing semantics (durability/iteration/locking), update:
  - `docs/contracts/*`, and
  - the relevant spec tests.

## Benchmark Profiling Workflow

- Prefer `-profile-dir` over ad-hoc profile flags:
  - `OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)`
  - `./bin/unified-bench ... -profile-dir "$OUT"`
  - `./bin/benchprof -profiles-dir "$OUT"`
- `benchprof` consumes:
  - `benchprof_results.json/md`
  - `cpu_<test>_<db>.pprof`
  - `allocs_<test>_<db>.pprof`
  - `checkpoint_cpu_checkpoint_<test>_<db>.pprof`
  - `block.pprof`, `mutex.pprof`, `trace.out`
- If you change profile flag behavior, output names, or benchmark test names,
  update `cmd/benchprof/main.go` parsers and `cmd/benchprof/main_test.go` in
  the same PR.

## On-Disk Format Changes

Be extra careful when touching metadata formats or recovery logic:

- Add/extend tests that cover crash/reopen and truncated/corrupt tail cases.
- Prefer versioned metadata changes (explicit version fields).
- If compatibility is intentionally broken, document it in `docs/API_STABILITY.md` / `CHANGELOG.md`.
