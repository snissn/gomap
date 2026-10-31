# Standalone Mongo profiling producer preparation

Accepted deliverable: an out-of-repository, **unbuilt** standalone gateway profiling overlay and fail-closed receipt validator. This completes source preparation only. No gateway was compiled or executed, no real profile was collected or decoded, and no public performance or causal claim follows.

The original bounded attribution inquiry remains closed through merged [PR #5358](https://github.com/snissn/gomap/pull/5358). Parent [#5225](https://github.com/snissn/gomap/issues/5225) remains open with its existing owners. This preparation does not activate the conditional native-cache inquiry or select a production change.

## Delivered and checked

- `prototype/make_overlay.py` assembles the pinned standalone CLI with the diagnostic helper, without creating a worktree or building. Its overlay targets a future clean checkout of `95c6ff3de473786bd98b692375cfdb9cef951668`.
- `prototype/PRODUCER.md` defines the local Unix-socket phase control, CPU/block/mutex capture, 64 MiB aggregate profile cap, original owner/checkpoint/checked-close lifecycle, failure behavior, and future qualification procedure. `off` captures receipt/control overhead without samplers.
- `prototype/validate_profile.py` invokes the complete pinned maintained campaign/lifecycle validator before adding full-owner, phase-time, finalization, regular-file, size, hash and cap checks.
- Eight Python synthetic control methods passed, including negative mutation subcases. Their placeholder bytes test receipt and artifact custody only; they are not pprof output and do not exercise the Go producer. See `evidence/validator-controls.json` and its retained log.
- Pinned Go 1.26.10 gofmt parsed/formatted the generated source. This provides no typecheck, build or runtime result. The patch applies to a temporary original-source shadow and reproduces the generated source byte-for-byte; the shadow was removed. See `evidence/integration-source-check.json`.
- Independent renewed source review **ACCEPT**: `review/independent-review-final.md`. The initial **REJECT** and exact rejected candidate remain preserved. Both response-flush and request-admission findings were repaired in one functional batch. Documentation accurately limits shutdown error propagation to requests admitted before Close marks the control closed; later rejection errors are best effort.

Final generated source SHA-256: `987cd212e24c2bd1d4c28ce65501a353aacca6289b48ae3548b7eae39fb14101`.

Final patch SHA-256: `a106c82aa05bc3868e49799fda0af789a265ba9a004cb9cba6d72ba5ce1c3de0`.

The exact final review, source, validator and evidence hashes are bound in `state.json` and `SHA256SUMS`.

## Infrastructure and remaining execution

`INFRASTRUCTURE_UNAVAILABLE: storage`: `/mnt/fast4tb` has zero free bytes. The root filesystem has approximately 4.5 GiB available, below the unchanged 8 GiB build and 10 GiB public-diagnostic free-space floors. No other persistent mounted filesystem with usable capacity was found. `evidence/resource-preflight-final.json` records the measured bytes and blocked gates. No build/runtime retry or reserve reduction was attempted; no unrelated storage was deleted.

An execution owner must first provide sufficient isolated persistent storage and establish exclusive measurement custody. The next bounded proof is the pinned entry's build/typecheck followed by actual `off` and `cpu_wait` process lifecycle controls and decoding/symbolization of all three profile payloads. Qualification must also cover env-unset equivalence, sampling start/stop, checked write/short-write/overflow/Sync/Close and listener/filesystem failures, routing and response failures, concurrent shutdown, controller-side failed-exchange veto, and preservation of original checkpoint/owner cleanup. These are open gates, not synthetic-test results. The exact procedure and required provenance are in `prototype/PRODUCER.md` and the final review.

Only after those controls are accepted may an owner select a public diagnostic under the original workload, durability, resource and repetition contracts. Such a run is still needed to attribute the representative durable-B slowdown. No runtime owner is active and measurement custody has not been requested while storage is unavailable.

## Retention and handoff

This small packet is unique source/control/review evidence, not a complete worktree archive. It retains successful and rejected evidence for resumption. No production checkout, permanent test, benchmark harness or CI was modified. No new worktree, binary or build cache exists to retire. Temporary synthetic fixtures and the patch-application shadow were removed; final packet inventory contains no temporary directories or Python bytecode caches.

The prior lane's completed harness goal and merged result remain historical facts. The followup artifact is accepted as source preparation; actual runtime/public profiling remains infrastructure-blocked. There is no additional production or merge task selected by this packet.
