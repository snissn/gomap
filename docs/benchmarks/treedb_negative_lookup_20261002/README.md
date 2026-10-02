# Bounded negative point-read PR qualification (#342)

This packet covers 8192 persisted keys and 500ms read samples. It qualifies the
PR's bounded workload; the authoritative larger campaign waits for landed graph
node H. No retained timing has been collected yet.

The constructor checks every byte of every original value by regenerating one
256-byte or 4096-byte expected buffer, checks all interleaved misses, and requires
actual covered backend captures before timing. It preserves payload size and
entropy across 64 updates while retaining an old snapshot. Production filter
defaults remain off. The test rejects a same-length corrupted final value and a
present supposed miss, then checks coverage through checkpoint and public GC.

Preparation compiles four frozen test binaries once: candidate TreeDB, tree and
db packages, plus reference TreeDB. It records every compile argv, exit status,
source identity and binary SHA256 under Go 1.26.3, GOMAXPROCS=2 and
GOMEMLIMIT=2GiB. Collection runs these same binaries in 36 fresh processes, with
alternating paired order:

| Family | Processes | Samples |
| --- | ---: | ---: |
| Public 2 payloads × 2 distributions × 4 miss rates × 5 routes × 2 modes × 5 pairs | 10 | 800 |
| Unchanged canonical hit routes, exact merged R versus N default-off, 5 pairs | 10 | 60 |
| Public update acknowledgement / checkpoint publication and whole backend open-close | 5 | 30 |
| Fixed filter memory/saturation and normalized publication preparation | 10 | 40 |
| Separate counter-only public matrix, 10000 iterations | 1 | 160 diagnostic rows |

Total: **930 performance samples and 160 diagnostic rows**. Nominal 500ms leaf
timers total 450 seconds; calibration, fixture validation, storage, canonical
setup, 10-iteration update/open samples and counter collection add wall time.
Reserve approximately 20–30 minutes on the allocated shared Linux runner and
report a checkpoint at 25 minutes if still running. This is an estimate, not a
measured capture duration. Counter timings never support throughput claims.

Only the coordinator may grant exclusive timing. After its preflight, prepare
separate exact candidate/reference archives and their frozen visible Go/module
source manifests. Compile without collecting benchmarks, then let the
coordinator audit preparation before its timing grant (replace arguments with
the approved identities):

```sh
python3 capture_qualification.py \
  --prepare \
  --source /mnt/fast4tb/gomap-issue-342-negative/source \
  --reference /mnt/fast4tb/gomap-issue-342-negative/reference \
  --output /mnt/fast4tb/gomap-issue-342-negative/preparation-FROZEN_HEAD \
  --candidate-head FROZEN_HEAD \
  --reference-head a68a84c7195e0c5d8c339343a385b40acf818d03 \
  --candidate-manifest /path/to/candidate.sha256 \
  --reference-manifest /path/to/reference.sha256
python3 capture_qualification.py \
  --source /mnt/fast4tb/gomap-issue-342-negative/source \
  --reference /mnt/fast4tb/gomap-issue-342-negative/reference \
  --output /mnt/fast4tb/gomap-issue-342-negative/qualification-FROZEN_HEAD \
  --candidate-head FROZEN_HEAD \
  --reference-head a68a84c7195e0c5d8c339343a385b40acf818d03 \
  --candidate-manifest /path/to/candidate.sha256 \
  --reference-manifest /path/to/reference.sha256 \
  --prepared-directory /mnt/fast4tb/gomap-issue-342-negative/preparation-FROZEN_HEAD \
  --grant COORDINATOR_GRANT_ID
python3 parse_qualification.py /path/to/completed-packet > parsed.json
```

The scripts pin Go 1.26.3, GOWORK=off, GOMAXPROCS=4 and GOMEMLIMIT=1GiB.
Every process retains its bound binary digest, command, source selection,
timestamps, exit status and raw log digest. Source, executable script and binary
identities are verified before and after preparation and capture. The parser
requires every exact case once in each expected process, successful completion,
finite timing/allocation metrics, actual enabled storage, key counts, checkpoint
publication counts, fixed-memory probes and separate diagnostic counters. It
rejects partial captures, altered logs or binaries, duplicate cases, wrong
commands and source/script changes. Parsing verifies evidence completeness; it does not accept a
performance regression or substitute theoretical false positives for measured
speed. Interpret medians, spread, allocations, enabled hit/update costs, seed
variation and shared-host limits before readiness handoff.
