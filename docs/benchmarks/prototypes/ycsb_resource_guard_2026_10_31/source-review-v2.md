# Independent second source review: repaired public outer guard

Verdict: the three prior guard-source findings are addressed. No additional
acceptance-critical production-guard defect was identified in this source pass.
The guard source is suitable for the next bounded synthetic-validation step;
the current draft test harness must first repair the unverified cleanup authority
below. Neither source review nor draft controls establish readiness for public
collection or maintained landing. The remaining validation gates below are
bounded to the stated contract, not a request for a broader supervisor framework.

This was source-only: no guard, tests, builds, workload, GitHub mutation or child
fixture was executed. Only this report was written. Line references below are
relative to `prototypes/public_guard_source/` unless explicitly qualified.

## Exact reviewed inputs

- `public_guard.py`: SHA256
  `39369f135883731d94426cdf3616b0c80b120a3c2637bddf09c979b46aed75f3`.
- `test_public_guard_draft.py`: SHA256
  `b76b5a5a3ccb504073cd783bdbfea4d4b150a00ceb9ceab4aecd6d839ec66f43`.
- `README.md`: SHA256
  `948312ccc41f8cafe1efb0d3d50fdc5dc73cae2106c33d53db9cdc9c7df5afcb`.

Rechecked the maintained reuse source under packet `baseline-current`, documented
baseline `4dc879a15b9acaf5dd45444627c7e62b3f829a9d`:

- `scripts/ycsb_compare_validate.py`:
  `1b14289667212b44dc7257f9cd5221267c12239bc2e3f5bf58d7dcbda9fadd04`.
- `scripts/ycsb_evidence_json.py`:
  `86989ebbfa14b8f9f3c20ebe470314a29fd28c7db2ab10734d9518e39d270ea6`.
- `scripts/ycsb_runtime_observer.py`:
  `fab36e1396205c2cddff0c24fa0f5c6faaa4e0fd47f46fdb423ffe3dd4e31077`.

The validator imports the strict reader and checks the runtime observer hash.
The frozen import closure remains an external campaign binding; these hashes
are source inspection evidence, not execution receipts.

## Prior findings: closed in source

1. **Prior HIGH fd(A)/proc(B) identity race:** `public_guard.py:59-68` now checks
   both snapshots' start token and parent, then requires the newly opened child's
   own pidfd to remain live for nondirect descendants before installing its row.
   A reused PID cannot have supplied replacement proc metadata while the old
   task's pidfd remains live at that later check. Exceptions close the new fd
   (`:69-71`) without leaving a mismatched row. Direct/adopted children retain
   their separate unreaped-parenthood proof, including quick-exit zombies.
   The deterministic test at `test_public_guard_draft.py:85-103` discriminates
   the original defect: rejects exited fd(A)/proc(B), checks close/no installed
   row, then re-pins B and verifies aggregate RSS. The quick-zombie control at
   `:105-112` preserves the intended direct-child case.
2. **Prior MEDIUM numeric waitpid reuse:** adopted reaping now uses
   `waitid(P_PIDFD, retained_fd, WEXITED|WNOHANG)` at `public_guard.py:140-147`,
   with ECHILD scoped to that exact old task. Root remains with its Popen owner.
   Preflight `:163-174`, `:258` rejects missing kernel support before launch.
   Tests `:114-137` forbid numeric waitpid, cover exact adopted reaping/ECHILD,
   and unsupported-kernel refusal. They are meaningful source controls, not yet
   kernel execution evidence.
3. **Prior quick-root receipt gap:** `public_guard.py:267-269` immediately writes
   `root.identity.json` and retains the same identity in the result before any
   root poll/wait. Original-shell TERM includes its start token at `:220`.
   Quick exit0 checks the retained token (`test_public_guard_draft.py:45-49`);
   injected tests compare durable/result root identity and original-shell signal
   identity (`:181-185`, `:211-214`). The ordinary quick-exit test can additionally
   assert the standalone identity file directly; the receipt write is present
   in production source.

## New findings: synthetic-test authority, not guard signaling

### H1 — HIGH: failed fixture identity verification can authorize an unrelated kill

`test_public_guard_draft.py:171-174` opens a pidfd using a declared numeric fixture
PID and **adds it to `fixture_fds` before** checking the declared start token.
If that task exited/reaped and the PID was reused, `:173` correctly raises an
assertion for the replacement's different token. But the `finally` block at
`:189-190` then treats the just-added, unverified replacement fd as owned and
can send SIGKILL to it. No production guard signaling path is implicated; this
is a real test-owner cleanup-authority error on the failure path of the proposed
synthetic execution.

Minimum repair before executing these tests: keep a new fd local until its exact
fixture identity and live binding have been verified, close it without signaling
on any mismatch/verification failure, and register only verified fixture fds for
cleanup. Retain the final pidfd-liveness check after numeric proc verification so
fd(A)/proc(B) cannot register a mismatched row. A deterministic mismatch/exception
control must assert close, no cleanup registration, and no signal to that fd;
retain the ordinary verified fixture exit assertion.

### M2 — MEDIUM: test failure cleanup depends on the supervisor under test

`test_public_guard_draft.py:167-176` launches the guard but the `finally` block
`:187-191` does not stop/wait that direct child on timeout, missing fixture JSON,
or an intervening assertion. In addition, unconditional unrelated-sleeper TERM
at `:188` can raise if the very survival assertion under test failed, skipping
later fixture cleanup. Thus a failing safety control can abandon the running
supervisor or skip already-verified fixture cleanup.

Minimum repair: retain exact authority for the direct test-owned guard child,
perform bounded stop/wait on exceptional test exit, and make cleanup of the
unrelated sleeper and each verified fixture independent so one exited task or
cleanup error cannot suppress the rest. Record the original failure and cleanup
errors. Do not add a second process-discovery framework: this test owner already
has the direct guard child and verified fixture descriptors. The coordinator
must also bound the overall synthetic runner, rather than treating the guard's
own timeout as proof that a broken guard cannot remain alive.

## Controls already strengthened

The injected fixture cases now pin declared live fixture children before release
and require those pidfds to be exited after the guard returns (`:171-180`). They
also hold a separate test-owned unrelated sleeper pidfd, require unchanged live
identity, and reject a guard signal naming it (`:157-159`, `:178-179`). With H1/M2
repaired, these assertions establish actual survivor-state checks, unlike the
older intent-only checks at `:57-67`.

The new cases cover live setsid children, rapid original-parent fork/reap plus
adoption, a child forked during cleanup, cancellation inside Popen, known-owned
cleanup after discovery/sample-log failure, and strict missing/malformed/
duplicate-key/invalid/valid validation (`:193-229`). Rapid real PID reuse need not
be forced: the deterministic identity control supplies the discriminating race.

## Minimum remaining acceptance-critical controls

README `:32` already acknowledges these gaps. Run the repaired existing controls,
then add only the following contract checks before maintained/public acceptance:

1. **Resource aggregate and free floor:** use several live descendants each
   individually below a small synthetic RSS cap but jointly above it; verify
   sampled identities/sum, RED result and exact cleanup. The one-byte test at
   `:69-72` mostly catches interpreter startup. Exercise initial free-floor
   refusal with a never-launched marker, and a bounded injected mid-run
   `f_bavail*f_frsize` crossing with owned children present. No filling the host
   or requiring an actual 24 GiB/10 GiB boundary excursion. Keep brief/exited RSS
   peaks explicitly outside continuous-enforcement claims.
2. **Startup and interrupted cleanup:** inject root-pin failure after Popen and
   prove the maintained direct-unreaped-child fallback plus descendants cleanup.
   Extend cancellation to shell grace, TERM/KILL escalation and receipt
   finalization; assert permanent failure, bounded completion or explicit cleanup
   failure, and actual verified fixture exit/unrelated survival. The existing
   Popen-cancellation case covers only one of these transitions.
3. **Original shell lifecycle and cleanup exit:** an actual Bash EXIT-trap fixture
   waiting on a foreground helper must demonstrate shell-first ordering and
   checked cleanup receipts, including cleanup returning nonzero after otherwise
   successful operations and a stubborn child requiring force. The maintained
   shell has `trap finish EXIT` and explicit cleanup-exit vetoes at
   `baseline-current/scripts/ycsb_compare_mongodb_treedb.sh:261-320`; TERM attempt
   alone does not prove that those paths ran or checked Close/reopen succeeded.
4. **Observation/final receipt failure:** extend the existing discovery/log
   injection to resource observation and result-write/finalization failure.
   Check known-owned cleanup, retained uncertainty, unrelated survival and a
   nonzero wrapper exit; absence/partial receipt or nonzero exit must veto public
   acceptance. Success finalization (`public_guard.py:314-319`) is outside the
   inner exception handler, so a completed-looking receipt alone is insufficient
   when process exit is nonzero. Retain all failing namespaces and driver logs.

This is execution-gated assurance, not a new unresolved production defect for
each unexecuted case. Source edits required by H1/M2 and any failures need their
own exact hash and proportional re-review before reuse.

## Contract and acceptance limits retained

- `public_guard.py:190-201`, `:329-333` retain sampled live owned-descendant RSS
  24 GiB and account-available free floor 10 GiB. Guard RSS is excluded; shared
  mappings can be counted per process, short peaks/exited children missed, and
  full proc discovery/scheduling adds overhead to the nominal 200ms interval.
  The explicit false continuous/unperturbed flags at `:185` remain truthful.
- Childless subreaper setup, live-parent discovery, retained pidfds through exec/
  setsid/adoption, exact pidfd signaling and terminal root/adoptee checks remain
  coherent (`:73-147`, `:154-160`, `:263-283`). No killpg/name kill was added.
  The maintained numeric launch fallback is confined to its exact direct,
  unreaped Popen child and WNOWAIT proof, not an external numeric PID registry.
- Failure status is permanent across guard intervention, resource/deadline
  breach, observation error, command nonzero and validation failure
  (`:218-246`, `:281-313`). Original shell gets TERM first, then grace and
  exact-owned TERM/KILL passes. Discovery failure retains errors and attempts
  known-pinned cleanup; it cannot certify an unobservable descendant's exit.
- Public use must freeze `--validation-json` to the fresh maintained campaign
  artifact; the argument is optional only for generic/synthetic use. Strict
  status/errors and wrapper exit0 are necessary, and the original maintained
  raw lifecycle/typed census consumers still supply correctness acceptance.
- CPU affinity0-11/runtime12/cgroup, no forced GOMAXPROCS, quiet host lease,
  persistent writable mount and every database/output path on the guarded
  filesystem remain external frozen preconditions. This source checks
  GOMAXPROCS absence and guard-output device, not all those conditions. Freeze
  original baseline/candidate wrapper command/env, helper closure and exact
  source/client/server ELF manifests before either accepted public run. No
  benchmark or performance conclusion follows from this review.
