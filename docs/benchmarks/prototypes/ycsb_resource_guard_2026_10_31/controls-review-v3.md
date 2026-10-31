# Final narrow source/artifact check: public guard controls v3

**Verdict: READY for immutable source/control publication and maintained-adoption
review.** Prior test-authority findings H1/M2 are closed in the reviewed source.
The retained batch supports the bounded synthetic controls requested in v2.
No further blocker was found within this narrow check. This is not production
adoption, public A/B authorization, continuous resource enforcement or performance
acceptance. Only these review artifacts were written; no tests, guard, build or
workload were rerun.

## Exact identities

- Production guard unchanged: `prototypes/public_guard_source/public_guard.py`
  SHA256 `39369f135883731d94426cdf3616b0c80b120a3c2637bddf09c979b46aed75f3`.
- Repaired controls: `prototypes/public_guard_source/test_public_guard_draft.py`
  SHA256 `ac2c50d25320c67c191fda57a5503622d40a7d63160b618959851262be8152a4`.
- Reviewed final README SHA256
  `f5482f85405b4175001a28c27466c852db4dcc5ce2196557e387495a99f9ce01`.
  Invocation recorded its earlier documentation hash
  `d05e2401d522537aeacc9e586865589d766397997663f64b0a1a4272c1781261`;
  the final README adds the execution handoff. Guard and executable test source
  hashes match both the invocation and final manifest. Do not describe the final
  README hash as the pre-execution README hash.

Retained evidence root: `evidence/public-guard-synthetic-v3`.

| Artifact | SHA256 |
|---|---|
| `invocation.json` | `08d5882d6888bbf097adfeac8306f537bddb87e33605d645da8e9b2c2322f61e` |
| `suite-result.json` | `e9bb69bce0a8b042938e7c4a65d78068316cfd6dbc60f3a8c65daacb50d265c1` |
| `unittest.full.log` | `8f562787d87ec9a7d9bba134764357f0b3991303181900d9c606b32cdb0af731` |
| `final-source-and-results.json` | `da537c16144cd43f30f6f8d080bc28309be6187b67a90ed74307eef47f2c7965` |

Invocation pins the actual maintained helper/import closure:
`ycsb_compare_validate.py` SHA256
`1b14289667212b44dc7257f9cd5221267c12239bc2e3f5bf58d7dcbda9fadd04`,
`ycsb_evidence_json.py`
`86989ebbfa14b8f9f3c20ebe470314a29fd28c7db2ab10734d9518e39d270ea6`,
`ycsb_runtime_observer.py`
`fab36e1396205c2cddff0c24fa0f5c6faaa4e0fd47f46fdb423ffe3dd4e31077`.
It records `/usr/bin/python3 -B .../test_public_guard_draft.py -v`, fast-mount
output/TMPDIR, maintained baseline scripts and GOMAXPROCS absent. The separate
`launcher-preflight-failure.json` records an erroneous guessed helper filename
during hashing, explicitly before tests launched; it is retained, not counted as
a safety-test pass or silently omitted failure.

## H1/M2 disposition

**H1 closed — verified registration/no signaling on mismatch.** Test source
`:30-38` keeps a newly opened fixture fd local until expected start ticks and a
later pidfd-liveness check succeed. Any verification exception closes the fd
before registration and supplies no signal authority. The mismatch regression
`:59-64` checks empty registry, exact close and no signal. The exited-fd regression
`:66-70` covers the live-binding seam even when the supplied proc token matches.
Both are listed `ok` in the retained suite log. Ordinary integration fixture
registration at `:159-160` uses this function.

**M2 closed — bounded test-owner cleanup independent of guard success.** Source
`:154-155` pins the direct unreaped supervisor before polling/waiting. `:175-183`
independently attempts cleanup of that supervisor, each verified fixture and the
test-owned unrelated sleeper, retaining the original failure plus cleanup errors.
One exited task no longer suppresses later cleanup. `stop_test_owned` at `:41-55`
uses retained pidfds, TERM/KILL and bounded waits; no bare PID/killpg was added.
The explicit owner-timeout test `:199-204` expects TimeoutExpired and then checks
the retained empty cleanup-error list. Its namespace records the expected timeout,
failed guard receipt, original-shell-first signal and fixture termination path.

These are test-owner authority repairs. The production guard is unchanged; prior
production fd/proc binding, pidfd reaping and durable-root findings remain closed
under the v2 source review rather than being relabeled as new test-harness fixes.

## Retained execution evidence checked

The full unittest log contains **26 test methods, all `ok`, `Ran 26 tests in
16.966s`, `OK`**. `suite-result.json` records suite exit0. Independent readback
of every immediate integration namespace found **29 namespaces**, each retaining
`command.json`, driver logs and `test-owner-cleanup.json`, with
`cleanup_errors=[]`. Exactly one owner receipt contains the expected TimeoutExpired;
the other 28 have no original test-driver failure. Looping cases account for more
namespaces than integration methods; seven of the 26 methods are unit controls.

Actual guard results comprise 24 intentionally failed receipts, two intentionally
missing results, and three completed-looking receipts. Those three are quick
exit0, valid validation, and the deliberately interrupted finalization case;
the latter's nonzero supervisor exit is asserted by the passing test and vetoes
acceptance despite its completed-looking receipt. A guard receipt's `exit_code`
is the wrapped command exit, not a substitute for supervisor exit.

The bounded previously missing gates are exercised meaningfully:

| Gate | Source controls and artifact observation |
|---|---|
| Aggregate RSS / free floor | `:206-224`: cap 50 MiB, observed sum 82,882,560 bytes across three live tasks; each individual was below cap. Initial zero available space prevents launch; injected mid-run floor/resource-I/O cases fail and clean owned fixtures. No host-filling exercise. |
| Startup / interrupted cleanup | `:226-237`, `:279-282`: root-pin refusal, Popen cancellation and cancellation at shell/descendant/KILL send stages. All three cleanup-stage results retain pending SIGTERM and remain failed; stubborn fixtures require actual SIGKILL. |
| Shell cleanup order / failed exit | `:249-259`: foreground Bash fixture retains `term\ncleanup\n` and original-shell first signal. Separate operations-passed fixture retains cleanup-failed and wrapped exit9. This demonstrates the cooperative synthetic shell contract, not actual TreeDB Close/reopen. |
| Observation / finalization | `:239-247`, `:284-292`: discovery/sample/resource/result I/O faults and finalization interruption fail closed. Missing receipt and nonzero supervisor exit cannot become acceptance. |
| Ownership / validation | `:261-297`: missing/malformed/duplicate/invalid/errorful/valid validation, setsid/adoption/rapid reap/fork-during-cleanup; verified fixture exit and unrelated live identity assertions run before normal test-owner cleanup. |

For ordinary completed integration invocations, source `:166-173` checks the
unrelated sleeper remains live with the same token, rejects a guard signal naming
it, verifies declared fixture pidfd exit and compares durable/result root identity.
Passing source-bound assertions support those outcomes; the receipts do not
contain a separately replayable full unrelated-process survivor census. The
intentional test-owner timeout exits before those ordinary post-return assertions
and is assessed through its exact-owned cleanup receipt. This review did not
perform fresh live process observations.

## Remaining scope limits

The unchanged defaults remain 24 GiB sampled live owned-descendant RSS and 10 GiB
account-available free space; synthetic overrides test the same branches at safe
small/injected limits. Guard RSS is excluded, shared mappings may be summed more
than once, and brief/exited-child peaks may be missed. Neither continuous caps
nor unperturbed timing are established; the false receipt flags remain accurate.

The synthetic shell uses explicit TERM/EXIT traps. It proves the bounded
cooperative fixture behavior and nonzero cleanup veto, not that the original
maintained launcher's EXIT-only trap performed checked TreeDB Close/reopen.
Those actual original consumers, frozen source/import/ELF and environment bindings,
runtime12/affinity0-11/no-forced-GOMAXPROCS contract, quiet host lease, persistent
mount and all output/database paths remain the maintained-adoption/public campaign
responsibility. Publication gives an implementer immutable source/control inputs;
it does not waive these existing gates or constitute production adoption.
