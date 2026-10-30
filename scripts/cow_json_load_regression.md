# Opt-in public COW JSON load and reopen regression

This is a bounded correctness reproduction, not a performance benchmark. The
historical failure required a concurrent 100,000-row load; its trigger is not
deterministic. Passing one invocation does not prove all interleavings safe.
The independent 4,096-row exact-value lifecycle test and deterministic zipper
RED/GREEN regression remain required. This script does not replace them.

On Linux, use clean, separately checked-out Gomap and go-ycsb trees, a Go
toolchain compatible with each, libsqlite3/CGO build prerequisites, sufficient
space, and a new output directory on a suitable filesystem:

```sh
python3 scripts/cow_json_load_regression.py \
  --gomap-repo /path/to/clean/gomap --gomap-revision FULL_COMMIT_SHA \
  --client-repo /path/to/clean/go-ycsb-c195a9bc \
  --out /path/to/new/task-output
```

The Gomap source must equal the explicitly supplied full revision, and the
client must be clean at `c195a9bc777b6aeb1e2f5550db96d872338f2f40`. The script
constructs and checks the exact retained protocol-minor-only client overlay:
source SHA-256 `051f211d72ca0ba194ba1fcf4d50fe6bac9e6964a6e83ab933e0ac071e1a44d2`,
overlay SHA-256 `b09334ad0f53ea49ca553953b123a2617625c04a766dbed624e4058c2399f787`.
It records source, harness and binary hashes, exact commands, Go version,
child executable/environment/affinity observations, timestamps, raw output
and terminal receipts; it refuses a preexisting output directory. Removing
`GOMAXPROCS` from the actual server, client and verifier environments is
verified on Linux. Builds and execution are bounded; the three profiles use
fresh independent databases and are not automatically retried.

Each profile requires the native server's actual `shared_cow` startup, 16
writers, 100,000 successful JSON INSERTs and zero INSERT errors, checked
server/owner shutdown, then a fresh process reopening the same database. The
reopen verifier uses the real native owner, checks all 100,000 expected hashed
YCSB keys and ten correctly sized fields per key, rejects duplicate/missing
keys and truncated scans, and checks checkpoint and all closes. Its scan and
cursor limits are 100,001 documents and 256 MiB. It cannot compare exact
random values, which the historical client did not save. That client's own
Close/FlushAll return is discarded; successful client exit is not proof of
checked client close. Server/owner shutdown and the separate verifier's closes
are checked. A failure in any profile makes the overall process fail.

Each terminal receipt records whether server stderr was observed and whether it
contained `span-native reducer validation failed: no replacement changed root`.
`baseline_reproduced` requires a completed client load, INSERT and TOTAL
counts that agree, all 100,000 operations accounted for, operation errors and
that signature. A missing error series means zero errors; missing success or
TOTAL series is inconclusive. Unrelated startup, timeout, or other failures are
`inconclusive`. A signature rejects GREEN even when all counts and exits look
successful. A GREEN also requires checked shutdown and successful reopen.

The original three-profile public RED, corrected-source GREEN, executable
bindings and original failures are retained in the [#5333 evidence
record](https://github.com/snissn/gomap/issues/5333#issuecomment-6511168099)
and [execution checkpoint](https://github.com/snissn/gomap/issues/5225#issuecomment-6509396582).
The large public reproduction and component-level deterministic RED/GREEN are
distinct evidence; the 4,096-row public lifecycle test passes the unfixed
source and is not a reduced public RED. Use `--gomap-repo` to test either a
clean historical source or the repaired source with the identical runner and
verifier. No automatic rerun or favorable-sample replacement is performed.
