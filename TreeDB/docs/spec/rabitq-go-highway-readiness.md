# RaBitQ go-highway readiness (#2448)

Status: dependency/toolchain readiness plus #2453 no-land closeout. This note
does not define TreeDB RaBitQ storage, codec behavior, search APIs, or
production scoring, and it does not claim that go-highway acceleration is active.

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

The root module imports go-highway only in a small test smoke surface today. The
#2453 acceleration follow-up investigated production backend wiring and closed as
no-land/not-planned for this stack.

## Build notes

The go-highway RaBitQ package provides pure-Go fallback dispatch and architecture
accelerated paths. AMD64 SIMD acceleration is tied to Go's SIMD experiment in
upstream docs; ARM64 NEON is available without a special build flag. TreeDB does
not enable production RaBitQ scoring in this issue, so these build notes are
future-backend input rather than a runtime feature guarantee.

## #2453 no-land decision

The go-highway bit-product kernel is fast in isolation, but its RaBitQ package
shape does not match TreeDB's v1 contract:

- TreeDB stores durable LSB-first `packed_bit_vector` rows plus `code_count` and
  `quantized_dot_product_inv` side arrays.
- TreeDB's scorer uses exact float32 query absolute weights in the weighted
  sign-dot estimator from `rabitq-1bit-v1.md`.
- The go-highway candidate path expects a different bit/word and quantized-weight
  shape. Using it for TreeDB v1 would require lossy query-weight quantization or
  many residual/bit-plane passes, violating #2453's no-semantic-change and
  maintainability gates.

Therefore #2454 closeout tables must include only `RaBitQ pure-Go` rows. Do not
publish a `RaBitQ accelerated` row unless a future issue lands a compatible
backend with parity tests, same-fixture before/after benchmarks, and profiles.

## Boundaries

- Do not copy CockroachDB, Antfly/ELv2, AGPL, or C++ RaBitQLib code into this
  repository.
- Do not add cgo/C++ RaBitQLib dependencies for this readiness node.
- Do not make the pure-Go RaBitQ reference/spec work depend on go-highway.
- Do not reinterpret existing scalar_u8 quantized benchmark evidence as RaBitQ
  evidence.
- Do not overclaim go-highway readiness as landed RaBitQ acceleration.

## Local smoke command

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestGoHighwayRaBitQSmoke$' -count=1
```
