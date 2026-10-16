# Fixed hosted maintenance diagnostic

This independently landable, manual-only harness compares fixed product commits
`5bca87ffe6e1bfba2d693264d2dc82936adc234f` and
`a17febafb5353897c59a4e17f992e34c47a2585b`. Neither product
checkout is replaced with the harness checkout. The retained-evidence policy
requires independent harness/schema review and landing BEFORE future expensive
collection. Root must then freeze the landed harness SHA and exact product
sources, and separately authorize one manual dispatch on main.
The candidate is the independently reviewed allocation-packing successor of
`2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49`; prior hosted evidence for that parent
does not qualify this candidate. There is no PR/push trigger, arbitrary ref input,
retry, or self-hosted runner.

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
executables/full toolchain archive and SDK file manifest, host/image/process information, source
manifests, build environment/logs/buildinfo, four binaries, authorization/binding,
pristine product/harness source archives, all attempted raw samples and quiet-process observations are uploaded even when
preparation or collection fails. Only artifacts produced before failure can
exist. Hidden harness files and exact tracked-source archives are included;
expendable caches and worktree metadata are excluded. A setup-action failure has the initial scope marker plus GitHub action
logs. A platform-forced runner termination can prevent even an always upload.

Adaptations from prepare-02.py/collect-02.py are paths under RUNNER_TEMP and a
main-only manual-dispatch-bound authorization receipt instead of the host185 grant file.
The preparation stage additionally preserves timeout/interruption receipts and
kills/joins the build's process group at its unchanged600s cap. Linux subreaper
mode lets the SAME parent reap actual adopted descendants as well as the
leader. Every supervised bootstrap/build/benchmark command records leader join,
adopted child wait statuses, group emptiness, and any kill or unexpected
surviving descendant. A survivor is killed and refuses success. Original
operation kill triggers remain unchanged; only killed-child reaping has a
bounded2s cleanup allowance, with no additional operation work. Old packets
proved only leader wait, and this does not retroactively prove their joins.
SIGTERM/SIGINT raise into the active child's cleanup rather than silently
leaving its group. The fixed
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
parser positives/negatives, toy process-group join controls and static workflow/context checks. It does not call
bootstrap, compile Go, execute a Go test body, benchmark, fetch or dispatch.
