# Repo Agent Log (Go Cleanup)

## Goals

- [x] Make `go test ./...` pass in all modules (`./`, `./TreeDB`, `./cmd/unified_bench`).
- [x] Rename `GeminiTreeDB` → `TreeDB` and remove `gemini` references from Go code.
- [x] Fix `cmd/unified_bench` to match current DB(s) (legacy DB already removed).
- [ ] Move `gomap` → `HashDB/` module directory.
- [x] Rename `btree` → `BTreeOnHashDB`.
- [ ] Apply Go best practices (gofmt, clearer naming, simpler APIs) without breaking tests/features.
- [ ] Track any “safe to delete” candidates in `TENTATIVE_DELETIONS.md` (do not delete unless clearly unneeded).

## Progress Log

- 2025-12-14: Baseline tests
  - `go test ./...` (root): PASS
  - `go test ./...` (`GeminiTreeDB`): FAIL (memtable test compile error)
  - `go test ./...` (`cmd/unified_bench`): FAIL (expects `../../TreeDB` module path; directory missing)
- 2025-12-14: Housekeeping
  - Added `TENTATIVE_DELETIONS.md`
  - Stopped tracking `.DS_Store` and added it to `.gitignore`
- 2025-12-14: TreeDB rename + tests
  - Renamed `GeminiTreeDB/` → `TreeDB/` and updated module/import paths
  - `go test ./...` (`TreeDB`): PASS
- 2025-12-14: unified_bench cleanup
  - Removed legacy/gemini DB references; now benchmarks `treedb` and `treedbcached`
  - `go test ./...` (`cmd/unified_bench`): PASS
- 2025-12-14: Repo formatting + artifacts
  - Ran `gofmt` across the repo
  - Removed checked-in build artifact `cmd/unified_bench/benchmarker` (now gitignored)
- 2025-12-14: CI
  - Added GitHub Actions workflow to run `go test ./...` in `./`, `TreeDB/`, and `cmd/unified_bench/`
- 2025-12-14: HashDB rename (in progress)
  - Renamed root module/package `gomap` → `HashDB` (`github.com/snissn/gomap/HashDB`, package `hashdb`)
  - Updated internal imports and `cmd/unified_bench` to build against the new module path
  - `go test ./...` (root): PASS
- 2025-12-14: BTreeOnHashDB rename
  - Renamed `btree/` → `BTreeOnHashDB/` (package `btreeonhashdb`)
  - Updated `cmd/unified_bench` import to `github.com/snissn/gomap/HashDB/BTreeOnHashDB`

## Notes / Conventions

- Prefer small, reviewable commits; run relevant tests before/after each.
- Keep renames “modest”: mostly package/module names and imports; avoid large API churn unless tests demand it.
