# Fixed hosted maintenance diagnostic

This manual-only diagnostic branch compares product commits
`5bca87ffe6e1bfba2d693264d2dc82936adc234f` and
`2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49`. Neither product checkout contains
this harness branch's workflow/script changes. Root must independently review
the final harness commit and explicitly authorize its manual dispatch first.
There is no PR/push trigger, arbitrary ref input, retry, or self-hosted runner.

The collector preserves reviewed collect-02.py's fourteen calls: three AB/BA
pairs of dirty durable pointer Checkpoint16x, three AB/BA pairs of actual
production foreground-churn Vacuum1x, then candidate-only cohort8/16 at128x.
The exact selectors, metric parser, zero-error/fixed-work receipts, CPU0,1,
GOMAXPROCS2, strict2% external-process threshold,250ms sampling,10s preflight,
120s Go child timeout and150s outer child cap are unchanged. Busy preflight
refuses before a benchmark; busy measurement is contaminated. No sample is
retried or selectively discarded. Hosted execution is not presumed quiet.

Bootstrap uses pinned setup-go1.26.3 and enforces the original Go executable
SHA256. It creates fresh task build/module caches under RUNNER_TEMP, downloads
and verifies downloaded modules before the fixed build freeze. It inventories
the two actual compiled packages and their test dependencies in read-only mode
with verified network access, then repeats the same inventory offline,
then compiles/collects with GOPROXY/GOSUMDBoff. Toolchain provenance, compiler
executables/full toolchain archive, host/image/process information, source
manifests, build environment/logs/buildinfo, four binaries, authorization/binding,
all attempted raw samples and quiet-process observations are uploaded even when
preparation or collection fails. Only artifacts produced before failure can
exist. Hidden harness files are included; expendable task caches/product worktrees
are represented by manifests rather than uploaded wholesale. A setup-action failure has the initial scope marker plus GitHub action
logs. A platform-forced runner termination can prevent even an always upload.

Adaptations from prepare-02.py/collect-02.py are paths under RUNNER_TEMP and a
manual-dispatch-bound authorization receipt instead of the host185 grant file.
The preparation stage additionally preserves timeout/interruption receipts and
kills/joins the build's process group at its unchanged600s cap. The fixed
drivers run within the bootstrap process so no second supervisor can kill a
collector before its original child-group cleanup. Network/bootstrap commands
have separate bounded failure receipts. No source overlay or benchmark/test
body was added, and no existing product or acceptance threshold was changed.

Infrastructure differs from host185: Ubuntu24.04 ephemeral hosted CPU/storage
and image, fresh task caches, no persistent runner/cache guarantee. setup-go
may use the hosted tool cache; the exact executable hash remains mandatory.
No actions/cache is used. The120-minute job bound includes dependency setup,
up to four600s build caps and the maximum14x150s benchmark envelope plus
preflight, inventories and artifact upload; actual stage exits/logs are retained.

COMPLETE_BOUNDED_DIAGNOSTIC is not cost acceptance or sustained/native/M0/current
CI qualification. Three matched pairs and one cohort sample per size retain
the original limits: cohort cleanup is one event, not a latency distribution;
process sampling can miss short-lived interference; the vacuum cost fixture
does not provide full digest/physical-bytes/shrink evidence. Root must disposition
the unchanged applicable requirements and any observed tradeoff separately.

Local preparation may run only mvcc_maintenance_test.py: pure Python syntax,
parser positives/negatives and static workflow/context checks. It does not call
bootstrap, compile Go, execute a Go test body, benchmark, fetch or dispatch.
