# Repository agent notes

## TreeDB storage and compatibility

- TreeDB is pre-alpha: APIs and disk formats may change without backward
  compatibility; rebuild experimental databases when needed. Update documentation
  and tests for format changes rather than adding migration scaffolding now.
- The value log is persistent storage, distinct from the redo WAL; it is not an
  ephemeral journal. Large values may be referenced by long-lived index pointers.
  Never truncate or delete a value-log segment because of age: remove it only
  when unreachable or safely replaced by rewrite/compaction. These rules hold
  even when WAL is disabled and value-log writes are deferred to a flush.
- TreeDB has no legacy slab value storage; update or remove documentation that
  says otherwise. HashDB still uses slab segments.
- Test pointer durability across checkpoint or synchronous write and reopen, and
  GC removal of unreferenced segments while retaining referenced ones. Existing
  tests are in `TreeDB/reopen_verify_test.go` and `TreeDB/db/vlog_gc_test.go`.
  `TreeDB/node/leaf_density_test.go` benchmarks prefix-compressed leaf density;
  adjacent tests enforce compression effectiveness. See `TreeDB/AGENTS.md` and
  the [TreeDB specs](TreeDB/docs/spec/README.md) for storage and recovery details.

## Benchmark profiling contract

Use `unified-bench -profile-dir` and the [profile workflow](cmd/unified_bench/README.md#standard-profile-workflow-benchprof);
`benchprof` runs automatically and can also be run separately. Preserve the
results, CPU, allocation, checkpoint CPU, block/mutex, and trace artifacts.
When benchmark names, profile names, or defaults change, update parsers and tests
in `cmd/unified_bench/main.go`, `cmd/benchprof/main.go`,
`cmd/benchprof/main_test.go`, and `cmd/unified_bench/profile_artifact_dir_test.go`, plus both
`cmd/unified_bench/README.md` and `cmd/benchprof/README.md`. Standalone package
profiling may use a dedicated fresh-process script; document its benchmark names,
artifacts, and reproduction in both READMEs and do not label its Go test profiles
as benchprof inputs. The focused collection insert compression recipe is in the
[unified-bench README](cmd/unified_bench/README.md#focused-collection-insert-compression-profile).
