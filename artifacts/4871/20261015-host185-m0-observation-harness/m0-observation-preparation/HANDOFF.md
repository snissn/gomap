# M0 observation harness preparation — frozen and released

Six new harness-only paths are frozen in `/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-harness`, base HEAD `5bca87ffe6e1bfba2d693264d2dc82936adc234f`. Existing tracked files, all product source, maintained M0 workflow/capture/summarizer and limits are unchanged. No Git mutation, commit, push, dispatch, Go fixture/test body, benchmark or profile was performed. Source and runtime writer leases are now RELEASED to root.

`freeze.json` binds all9,942 current tracked-plus-new source paths; SHA256 `8faa4d1d02a76f17e68ba9bc26c9e3692d50ad3fbd65dc660bf81861716a7e45`. `changed-files.json` identifies the six additions:

- `.github/scripts/mvcc_m0_observation.py`
- `.github/scripts/mvcc_m0_observation_exec.py`
- `.github/scripts/mvcc_m0_observation_gc.go.txt`
- `.github/scripts/test_mvcc_m0_observation.py`
- `.github/workflows/treedb-vacuum-m0-observation.yml`
- `docs/benchmarks/treedb-vacuum-m0-observation.md`

All10 pure harness controls PASS on Python3.10.12; workflow PyYAML parse and Python AST checks PASS. Controls cover exact22-call schedule, checked-preimage refusal, complete timed-body/writer byte equality, export completeness negative cases, exact candidate observer compatibility, missing-data flags, parent quota retention, actual toy process-group kill/adopted-child join, actual wrapper ELF/exit7 propagation, and manual workflow trust boundary. Initial local hashlib.file_digest incompatibility is retained in validation history and fixed with streaming SHA256; the imported candidate observer is unchanged.

Pinned official Go1.26.9 compile-only pristine and overlay DB executables PASS; product source before/after hashes are identical. Pristine SHA256 `d311c01bcaec510c801fd8e40b15101a72566872660af625d87bc60ace44852f`; overlay `6ba3a60a327a49a9f84b85d8163efdca0b41bb9786135523c546ea58ef3c7bf7`. Actual compiler/linker receipt is `local-toolchain.json`; complete local SDK manifest is `sdk-local-manifest.json`. Compile commands/env/buildinfo/ELFs and exact overlay inputs are retained under `compile-only/`. Neither binary was executed.

The frozen plan pins measured product `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49`, original two fixtures then L1,P1…L10,P10, original1x/count1/CPU0,1/GOMAX2/8GiB/default10min Go operation cap and20min workflow cap. Driver reserves one minute for finalization/upload within that cap. One10s/2% quiet preflight refuses before any fixture/benchmark; no retry. All fresh Go processes are preserved, with explicit overlay and -exec capture of every actual temporary ELF. Wrapper and telemetry add diagnostic I/O/CPU; they cannot replace original uninstrumented M0 acceptance.

`caller-timer-audit.md` proves initial GC observation precedes ResetTimer and foreground goroutine creation; post observation/export follows StopTimer. Timed body and foreground writer are unchanged. Exported durations are already collected values with ordinal/parity type; runtime GC counters/pause ring and wall spans are read outside timing. Existing runtime/setup activity can still be perturbed by ReadMemStats. PL06 and fixture bodies are untouched. Original summarizer outcome propagates unchanged.

The runtime packet retains fixed-source archive/manifests, checked overlay/harness identity, full SDK archive/manifests, all actual binaries, full raw attempts/failure/refusal receipts, argv/environment, process groups/kill+join, telemetry/missing flags and original summary. Artifact upload excludes only cache/modcache/tmp. Telemetry includes nominal250ms host/process/cgroup/pressure counters; hidden ancestors, hypervisor activity, exited-between-samples processes, scheduler waits and GC assists remain partly unobservable. Hard runner loss/job timeout can prevent upload. No persistent runner/cache is claimed; GitHub30day artifacts require durable project archival.

Pending ROOT integration: review exact six-path packet; stage intended additions; refresh reviewed CI inventory per `docs/CI_IMPACT_SELECTION.md`, explicitly resolving ownership of the new manual workflow; commit and independently review/land harness on main. Product remains fixed2588 and harness SHA must be its separately landed commit. Only then may root grant and dispatch exactly one diagnostic. Actual hosted Python/runner/telemetry compatibility, diagnostic results and all original gates remain unclaimed. No execution grant has been consumed or inferred.
