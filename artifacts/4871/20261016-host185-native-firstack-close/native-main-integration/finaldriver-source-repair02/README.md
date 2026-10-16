# Real diagnostic fixture repair02

One test file changes from immutable repair01 `30e0fe7f`: `TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go`. The guard now rejects `cache.COWMode`, with a source-authority comment. `MaintenanceNativePreCursorCacheForTestV6` reports `COWMode = db.cow != nil` (caching/native_mvcc_prune_v6.go:403); actual `StepNativeMVCCCaptureV6` rejects that state (line129). Thus the supported adaptive/native route has no COW cache. The failed fixture had required precisely the unsupported state before cursor/G construction. No production configuration or constructor is changed.

The earlier real-ready-cancel failure remains preserved at `../finaldriver-real-controls/real-ready-cancel`; it reached no cursor/G and establishes no native publication result.

`freeze.json` binds full source/manifest/archive, unchanged index and exact one-file delta. All10,430 other paths remain byte-identical to repair01. Source code, G/F formulas, retained accounting, Q32 logic and all focused controls are unchanged. `reused-focused-controls.json` binds all56 prior passing top tests/116 child tests; none is rerun.

`proposed-real-commands.json` retains the original five arms, all executed:false, each with a fresh absolute TMPDIR at `finaldriver-real-controls-repair02/<name>/tmp`. Root must separately select runtime; no execution occurs in this repair lease. No Go/compiler/gofmt, test body, native publication or benchmark was run. No additional allowance is chosen.

Both source/runtime leases are released at freeze. Q32 remains RED/ACK0.
