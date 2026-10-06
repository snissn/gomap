# Windows Close notification diagnosis

**REVISE the regression test's phase attribution.** The failed hosted014 log does not establish a runtime callback deadlock. No Close deadline is waived.

Inspected current `61fbd36655a9a8641f1cacee013c8f2bf2dc1553` and failed `0142519b2cc4c097f50b4e7a64a5b6993ff0d9c6` have byte-identical inspected test/public/cache/checkpoint/background source. Test `TreeDB/cow_close_notification_test.go:48–55` starts its five-second timer around the entire `database.Close()` goroutine and calls any timeout a notification lifecycle-lock blockage. It never records actual notification entry. Fatal returns while Close can still be running; the two later TempDir dictdb/index.db busy failures are secondary cleanup observations, not proof of the callback phase.

Public Close first stops/waits background vacuum and performs optional maintenance, then owns lifecycleMu exclusively (`public.go:1823–1844`). Final real cached checkpoint refusal triggers synchronous reportError/NotifyError (`1849–1852`, `1880–1885`), followed by cache, backend, dictdb and templateDB teardown. Stats and Get's owner capture use TryRLock (`2520`, `596`), so this callback's public reads refuse nil/ErrClosed while Close owns lifecycleMu. Given this source ordering, a notification-entry/completion witness is the direct deadlock test. The currently buffered one-result notification channel does not itself block the one expected final error.

Actual Windows events show both WAL profiles hitting test line55, then busy TempDir cleanup; each failed subcase totals7.08 seconds. The following public checkpoint/Close/reopen fixtures pass in7.99 and8.00 seconds. Those are whole fixture durations, not isolated Close latency, and neither prove the timeout was mere I/O nor locate callback progress. The absence of entry tracing limits causal diagnosis.

The test-only repair should meet this contract:

- Signal actual final-checkpoint notification entry immediately before Stats/Get, and completion after both calls; use nonblocking/bounded fixture channels.
- Check actual notification and returned Close error preserve wrapped final-checkpoint ErrCOWCapacity; Get is ErrClosed and Stats is nil under exclusive lifecycle ownership.
- Start callback-completion deadline at notification entry; keep a separately identified explicit finite whole-Close deadline from Close launch, including pre/post-notification I/O. Do not skip Windows or remove whole-Close liveness coverage.
- Verify notification completion precedes Close return under synchronous reportError ordering; do not settle for callback entry alone.
- Join the started Close before normal TempDir cleanup. Failure-path cleanup must remain bounded and report any unjoined Close rather than claiming resources were closed.
- Keep real COW pressure lease until tested refusal/notification; release it and verify retained cache TotalBytes/ExternalLeases zero after actual completed Close.
- Preserve async Close-hook witness; no production lifecycle/M7/owner authority change on source/log evidence alone.
- Retain original old014 failure honestly, run affected normal/race/safe checks on exact test-only freeze and require repaired final-head actual Windows/strict CI/hosted review. Historical974 runtime/cost identity remains original; test-only delta needs applicability, no automatic 45-run recollection.

Exact source/log bindings and retained relevant events are in the accompanying JSON. No source edits or runtime jobs were performed; final actual hosted Windows and strict current-head gates remain required.

Failure log SHA-256: `27131c64bae66d140c8ad89237a4389f43433a756beb29d9317b099f7a8b1583`.

- `TreeDB/cow_close_notification_test.go`: `35ebac9f24cf009d570bcfd9f397c57b98d3c7276f8bf306fe9df7a2516bc252`
- `TreeDB/public.go`: `626b7d2f1ab12545d1d8ace49c89b81da09821d588819cdbb95ae2f170278c65`
- `TreeDB/caching/db.go`: `1a9ed3a991107632a492949e033a81359dbec618f84d5b31ce16d7c204878882`
- `TreeDB/command_wal_public_cached.go`: `01f9874ef67d733112166f7b12e0e4c8bb03069d400dad1f05f97186be3ef331`
- `TreeDB/bg_vacuum.go`: `ae477993a01960095ed9d0c40722593d4ffa45f62c8441c683371609b8018528`
