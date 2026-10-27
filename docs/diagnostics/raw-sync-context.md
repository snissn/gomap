# Prospective raw-path WAL sync context

Set `RAW_SYNC_CONTEXT=1` when invoking `scripts/mvcc_raw_path_gate.sh` to retain
`raw-sync-{baseline,candidate}-{sample}.json` for each durable-sync sample.
Default `0` invokes the existing benchmark command directly and creates no
context artifacts. The existing eight alternating pairs, workload, original logs,
checker thresholds, allocation limits and exit status remain authoritative.
This option does not launch extra samples or retries.

Each record binds the source commit/tree and exact executable SHA256 to bounded
before/after system CPU, disk, VM, pressure and cgroup v2 counters. Each file is
limited to 64 KiB; missing, unsupported and truncated inputs are explicit. Cgroup
v1 counters are unavailable. These are system/container observations, not proof
of request attribution, storage isolation or physical-host isolation. Do not sum
these counters into an attributed sync latency.

The benchmark uses Go `b.TempDir`. The collector records the inherited `TMPDIR`
(or Go's Linux `/tmp` default), resolved parent mount/source/device and its stat
device. The actual fixture child directory/device remains explicitly unavailable;
a nested mount or benchmark change could invalidate the parent inference.
Monotonic boundaries enclose the child process, including calibration, setup and
teardown. They do not identify the benchmark timer phase. Context reads and writes
are outside the benchmark process but can perturb the host. Context-enabled
results must retain that label and are not evidence that instrumentation is free.
In opt-in mode the driver refuses any preexisting output directory or symlink,
preserving prior evidence; per-sample artifacts are exclusively created. Incomplete
mountinfo is labeled truncated/unavailable and parent mount inference is withheld.

No syscall, scheduler or blocked-stack tracing is implemented or required for the
gate. A separate future tracing mode must bind exact executable/build identity,
PID and benchmark timer start/stop markers, bound duration/output and record lost
events and unavailable permissions/tools. Process-lifetime tracing alone cannot
identify the timed phase. Traced timings must be diagnostic only.

Before any controlled A/A experiment, declare the exact retained executable hash,
fixture/temp mount, CPU placement, eight alternating context-off/context-on pairs,
fixed workload/duration, all-result retention and overhead summary. Obtain a quiet
measurement slot and preserve every sample and failure; do not execute a campaign
as part of installing this collector. A future tracing experiment needs its own
predeclared off/on overhead control. No experiment is authorized by this document.

No causal mechanism has been established by adding this collector. The #5262
failed gate remains failed; prospective evidence cannot retroactively change it.
