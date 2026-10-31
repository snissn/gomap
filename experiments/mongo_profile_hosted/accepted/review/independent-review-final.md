# Independent renewed source review: ACCEPT

**Acceptance is limited to the exact-source, UNBUILT diagnostic producer prerequisite.** Both P2 source findings from `independent-review.md` are resolved by one coherent functional repair batch. The coordinator subsequently clarified the shutdown boundary in documentation without changing code. No material source-review finding remains within this scope. This does not establish Go type correctness, runtime behavior, valid pprof output, public campaign qualification, durable-read attribution, or performance improvement.

## Exact candidate

Frozen original base: `95c6ff3de473786bd98b692375cfdb9cef951668`. Packet-relative SHA-256 bindings verified during renewed review:

| File | SHA-256 |
| --- | --- |
| `prototype/profile_control.go.txt` | `04e81ab5113745f937544081cbe9fe7805ec0f9b0df1aed7a56054eb27622bb1` |
| `prototype/make_overlay.py` | `d8b27acfbacb1efe74a740460281986e855b229d60587ec2798930ef65c182dd` |
| `prototype/diagnostic_server.go.txt` | `987cd212e24c2bd1d4c28ce65501a353aacca6289b48ae3548b7eae39fb14101` |
| `prototype/source.patch` | `a106c82aa05bc3868e49799fda0af789a265ba9a004cb9cba6d72ba5ce1c3de0` |
| `prototype/overlay.json` | `17cd771d83cf6012888eb4c9b3112b856a5aa780a412cb63aca3b4aa4d5630e9` |
| `prototype/PRODUCER.md` | `43e7abbed2ff123e07c618f1cf2bc6ada90c50f761c5dc5d4191910ab422a81d` |
| `evidence/producer-manifest.json` | `61792d75112154d2c9ad770b8ba212a9b81463ecbb9c7654773e688415b090e4` |
| `prototype/validate_profile.py` (unchanged) | `48bd478a902db158994185b9a859daab2bcebf143494d76570685c9c1a7ba8bd` |
| `prototype/test_validate_profile.py` (unchanged) | `c36c94e6c1d207fa326734f234e13fdf37d17c7d14b823e44c9d97beb209dd35` |
| `evidence/validator-controls.json` (reused) | `2fa01f032b15ba8d032874da7f121d837610fae0138abde2186108e466c7fc04` |

The original source manifest remains `478a2b7e082fb58a5fda231c3e490faf0849c888dd5a285ccc9c1b54461787ec`, binding original `server.go` hash `2b19dfec2741a5dfaacfdd50cf31cccf95e0228b565420c9992d9fe4e6a73490` and the maintained validator exports. The first rejection and its exact source/evidence are preserved under `evidence/rejected-candidate-1/`; the original review is not overwritten. The coordinator separately owns mechanical patch/hash/source verification and final acceptance recording.

## Findings disposition and shutdown boundary

1. **Buffered response failure: resolved in source.** The shared `respond` method declares the exact Content-Length before writing, checks write/short-write errors, and calls checked `http.NewResponseController(w).Flush()`. Success and error responses use that method under `c.mu`. An admitted handler keeps its WaitGroup lifetime through the local flush; the error is joined into `c.err` before releasing the lock. This removes the previously unchecked buffered payload/final-chunk tail from the admitted application's response boundary. A successful local flush still cannot prove client receipt; the final documentation correctly requires an independent controller-side failed-exchange veto.

2. **Invalid request routing bypass: resolved in source.** `http.HandlerFunc(c.admit)` replaces `ServeMux`, and `DisableGeneralOptionsHandler: true` removes the special `OPTIONS *` shortcut. Parsed canonical-path candidates now reach the application's explicit route/method validation. The final documentation accurately excludes malformed framing rejected by net/http before handler entry.

3. **Post-close admission distinction: accurately documented.** `c.closed` and `handlers.Add` are serialized by the same mutex. Close marks the boundary before waiting, so no newly admitted work can race an Add against Wait. It joins all application handlers admitted before that boundary. The untracked `c.closed` rejection branch uses checked framing under the mutex but may execute after Close returned; its later error cannot retroactively change that return. The final documentation explicitly treats those errors as best effort and limits the propagation guarantee to requests admitted before Close marks the control closed. No functional change is needed to claim that bounded contract. This source acceptance does not claim that every future HTTP request is joined or that every framework connection error is observable by the helper.

## Other reviewed contracts

The renewed diff and assembled server retain the original owner observation, owning collection checkpoint callbacks, listener validation, signal context, `Serve`, and checked `evidence.Close(standalone.Close)`. Diagnostic startup/shutdown errors remain joined with the original cleanup returns. Storage-only owner-close success remains distinct from the actual successful gateway exit required by the maintained validator.

The profile budget, CPU writer join/sticky-error handling, profile and receipt Sync/Close checks, failed-start cleanup, one-phase `run_1` state, and sampler/off lifecycle are unchanged by the repair and retain the source-review assessment in the first review. Process-global block/mutex sampling and control overhead still do not provide exact operation attribution. The fresh-process/no-other-profiler assumption remains necessary.

The generator now rejects nonempty `git status --porcelain=v1 --untracked-files=all --ignored=matching` output for an existing overlay worktree, in addition to the pinned HEAD and original-entry checks. Its no-conflicting-overwrite rule remains. The future build still needs an independently verified clean frozen source and complete toolchain/overlay/binary provenance; this guard is not evidence that a worktree or build presently exists.

The complete maintained `c.validate(campaign)` call and the added owner/phase/time/artifact/hash/size/cap checks are unchanged. The retained eight Python synthetic control methods, including their negative mutation subcases, can therefore be reused for their original receipt-semantics scope. They were not rerun. Placeholder profile bytes are explicitly not pprof output, and these controls do not exercise the new producer or either HTTP repair.

## Remaining gates and limits

No Go build/test, runtime injection, server/client launch, public collection, or GitHub write occurred during either review. During renewed review, I read the repaired files and retained evidence, inspected diffs/hashes, and wrote only this new review file. The retained gofmt result is parse/format evidence only: its typecheck/build/runtime fields remain false.

The unchanged host-space and exclusive-measurement-custody gates still block building and running. After those gates reopen, required work includes exact-entry typecheck/build, env-unset equivalence, fresh `cpu_wait` and `off` process runs, actual sampling activation/cessation, pprof decoding/symbolization for all three payloads, complete source/toolchain/binary/process custody, and independently accepted maintained lifecycle plus profile custody evidence. Real negative integration must cover all documented filesystem/listener/profile failures and concurrent shutdown paths, including checked overflow/short-write/Sync/Close failures, canonical-path and `OPTIONS *` requests, response disconnect/write/short-write/flush failures, and controller rejection when an exchange fails despite a successful local flush. Original checkpoint and checked owner cleanup must still run on all applicable failure paths.

This ACCEPT completes only the source-review prerequisite for the unbuilt artifact under the `codex-issue-graph-executor` completion contract; it does not qualify execution or close any unmet public performance, recovery, or attribution outcome. Review-relevant changes require a new exact-candidate review.
