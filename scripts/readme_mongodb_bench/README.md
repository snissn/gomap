# README YCSB and MongoDB comparison recipe

This is the portable version of PR #5408's direct CLI recipe. The timed
producers remain `cmd/mongo_gateway_bench` and external
`snissn/go-ycsb@3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`. It preserves the
48 fixed slots, workload arguments, repetition order, fresh directories,
public full-data census, graceful stop and independent reopen checks.
It does not qualify the separate Linux runtime-attested YCSB matrix.

Build before collection with Go 1.26.10, `GOWORK=off`, CGO enabled and persistent
Go caches. Use a clean product checkout and a separately pinned, reviewed and
landed harness checkout. Copy this directory's seven Python/Go recipe sources
listed in `freeze.py` to a fresh task's `recipe` directory. Build the census
with `go build -o "$TASK/bin/census" "$TASK/recipe/census.go"` from the product
module root. The other required binaries are `treedb-mongo-gateway`
(`TreeDB/mongo_gateway/server.go`), `mongo_gateway_bench`,
`mongo_gateway_compare_report`, `ycsb-correctness-fixture`
(`scripts/ycsb_correctness_fixture.go`) and `go-ycsb`. Build the external
client with `-tags libsqlite3,sqlite_omit_load_extension`; do not patch sources.
Place its clean source worktree at `$TASK/go-ycsb-source`.

```sh
python3 "$TASK/recipe/freeze.py" --root "$TASK" --repo "$PRODUCT" \
  --harness-repo "$HARNESS" --go "$GO" --mongo-image-id "$IMAGE_ID"
shasum -a 256 "$TASK/freeze.json"
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite smoke
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite ycsb
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite shape
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite sweep
python3 "$TASK/recipe/aggregate.py" --root "$TASK" \
  --expected-freeze-sha256 "$RECORDED_FREEZE_SHA256"
python3 "$TASK/recipe/archive.py" --root "$TASK" --output "$EVIDENCE"
python3 "$TASK/recipe/verify_archive.py" --directory "$EVIDENCE"
```

Record the freeze digest outside the mutable task before running. An exclusive
task lock prevents simultaneous suites. A failure stops the suite, leaves its
failure receipt and database, and must never be retried as a replacement sample.
A reviewed continuation may execute only never-started slots. Builds, tests,
compression and cleanup of unrelated outputs stay outside timed collection.
The server185 freeze records the user-selected host as shared infrastructure;
unrelated services are preserved. MongoDB uses a frozen image ID and a fresh bind
mount with an ephemeral loopback port. On Linux, MongoDB runs with the host
user/group IDs so its bind-mounted files remain readable and releasable without
privileged cleanup. Read the host and filesystem metadata
before interpreting results.

The census proves observed key/typed field state and selected query behavior,
not every acknowledged update or physical index activation. YCSB original Close
receipts are checked. The macrobenchmark's original deferred Close return is
unobserved; independent reopen does not recover it. The durable YCSB label uses
different durability requests and does not prove crash or power-loss equivalence.
Storage is stopped filesystem allocation (`st_blocks * 512`) of the complete DB,
including redo/WAL and persistent value logs, excluding image/runtime overhead.
Summaries require all three valid repetitions; tails are medians of per-run
percentiles, not pooled requests. Reports must use the actual frozen host and
attempt ledger; the historical Mac renderer is deliberately excluded.

Offline controls, from this directory:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_recipe.py
"$TASK/bin/census" -self-test
```
