# RaBitQ go-highway readiness (#2448)

Status: dependency/toolchain readiness only. This note does not define TreeDB
RaBitQ storage, codec behavior, search APIs, or production scoring.

## Decision

TreeDB's root module now targets Go `1.26` so future accelerated RaBitQ backend
work can consume `github.com/ajroetker/go-highway/hwy/contrib/rabitq`.
Repository CI workflows install Go from the root `go.mod` via
`actions/setup-go` `go-version-file: go.mod`, so the workflow toolchain follows
this module-level requirement.

No explicit `toolchain` directive is used in the root `go.mod`; contributors
with older local Go releases should use the default `GOTOOLCHAIN=auto` behavior
or install Go 1.26 directly.

## Dependency evaluation

- Module: `github.com/ajroetker/go-highway`
- Version evaluated: `v0.0.12`
- Package: `github.com/ajroetker/go-highway/hwy/contrib/rabitq`
- License: Apache-2.0 (module `LICENSE` and package headers)
- Upstream module requirement: `go 1.26`
- Kernel surface smoke-tested in this repo:
  - `rabitq.QuantizeVectors`
  - `rabitq.BitProduct`
  - `rabitq.CodeWidth`

The root module imports go-highway only in a small test smoke surface today.
Future production backend wiring belongs to the RaBitQ acceleration follow-up,
not this readiness issue.

## Build notes

The go-highway RaBitQ package provides pure-Go fallback dispatch and architecture
accelerated paths. AMD64 SIMD acceleration is tied to Go's SIMD experiment in
upstream docs; ARM64 NEON is available without a special build flag. TreeDB does
not enable production RaBitQ scoring in this issue, so these build notes are
future-backend input rather than a runtime feature guarantee.

## Boundaries

- Do not copy CockroachDB, Antfly/ELv2, AGPL, or C++ RaBitQLib code into this
  repository.
- Do not add cgo/C++ RaBitQLib dependencies for this readiness node.
- Do not make the pure-Go RaBitQ reference/spec work depend on go-highway.
- Do not reinterpret existing scalar_u8 quantized benchmark evidence as RaBitQ
  evidence.

## Local smoke command

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestGoHighwayRaBitQSmoke$' -count=1
```
