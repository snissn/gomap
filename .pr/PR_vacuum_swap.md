Restores swap-based index vacuum semantics (online/offline) for the WAL-on/off-only backend.

What changed
- Reintroduced backend online/offline vacuum with index swap (`TreeDB/db/vacuum_online.go`, `TreeDB/db/vacuum_offline.go`).
- Restored vacuum recorder tracking to capture committed ops during online vacuum.
- Wired public API to call backend vacuum instead of aliasing `CompactIndex`.
- Treemap CLI now distinguishes:
  - `compact`: in-place rebuild
  - `vacuum`: swap-based offline vacuum (shrinks `index.db`).
- Added vacuum tests (offline shrink, online swap/snapshot, recorder race, concurrent write safety).

How to test
- `GOWORK=off go test ./TreeDB/... -count=1`

Notes
- Online vacuum is still unsupported on Windows (same as before).
- Vacuum is index-only; value-log semantics unchanged.
