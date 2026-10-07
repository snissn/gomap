# Private actual-M example execution preparation

`run-example.py` is prepared for root review, transfer and later execution on111 **after actual D**, inside the externally held canonical `/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock`. No example, vet, Go, SSH or remote command was run during preparation. The helper is not a benchmark or acceptance authority.

Root's later invocation, inside its existing lock wrapper:

```sh
R1_RECEIPT_LOCKED=yes python3 -B /absolute/reviewed/run-example.py
```

The helper refuses an existing `/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/example` and uses its absolute NEW unused `db` child as the required `-dir`. The DB is first created by the original example; it must remain private. `go-tmp` is also output-owned. No private parent checkout is modified; Go caches use the independently frozen root config paths.

Source/config are independently pinned to landed M `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`, A config SHA `f2b95b22d5e1c12a00c5cc77dc5846b199535aa3e30e2cc77ed8b8f8b03257b4` and BEFORE source manifest SHA `d01cabb0159552b9d326977758278b9fd8f5df4f9f1f04b7587e79d95481998f`. Runtime inventory is reconstructed from actual Git objects and compared with the pinned manifest/config. Actual Git bytes are compared with working runtime, A/C harness and example files; clean exact HEAD and tree are recorded before/after. Example blob IDs are `main.go=f54a456e2d16bbb78cfc637d80f8b6b48410c563` and `README.md=a268b790e245aecc77712ee82e04be19cc564b27`, with actual SHA256/size and working hashes retained.

The config-pinned real Go executable path and its actual SHA256 are recorded, and its bytes must stay unchanged. The helper observes `go version`, requiring `go version go1.26.4 linux/amd64`, then executes exactly `go vet ./examples/typed_rows` followed by `go run ./examples/typed_rows -dir ABSOLUTE_NEW_DB`. No extra source test or retry is performed. Environment passes only inherited HOME/PATH plus config GOROOT/GOCACHE/GOMODCACHE, GOMAXPROCS16/GOGC100/GOMEMLIMIToff, GOWORKoff/GOTOOLCHAINlocal/GOENVoff/empty GOFLAGS+GODEBUG, LEFTHOOK0, PYTHONDONTWRITEBYTECODE1 and the output-owned TMPDIR.

Each actual command retains a UTC start record with argv/cwd/environment, separate untouched stdout/stderr logs, exit/hash/size records and post-command HEAD/status. Successful source/config/compiler/helper before/after byte equality is required. The example's exact meaningful line must exist in original stdout **after original exit zero**:

```text
Verified durable reopen: 2 complete rows; current and removed secondary postings; deleted row absent.
```

The actual stdout must also report the required absolute DB directory. The helper never reconstructs successful stdout. The inspected actual-M example itself asserts complete rows, typed insert, generic indexed update, mixed upsert, delete, held-view reuse, bounded current index range, removed/current postings, flush/checkpoint and durable reopen. The printed verification line precedes deferred cleanup, so requiring exit zero preserves cleanup failures too.

All failures retain original/partial files and emit a new failure record only in output created by this invocation. Refusal of an existing output does not write to it. No deletion, replacement, automatic retry, success invention or evidence publication occurs. Marker `R1_RECEIPT_LOCKED=yes` is a precondition and not independent proof of flock ownership or D ordering; root retains both responsibilities. Final example correctness receipt keeps numeric capacity/performance/power-loss acceptance explicitly separate. The current compiler hash is observed, not fabricated as an earlier independent compiler-byte freeze.

Preparation checked actual-M Git source/README and performed static AST checks only. No remote config/manifest/results were read and no helper runtime behavior was tested.
