# Current-main power-loss certification

Issue [#3684](https://github.com/snissn/gomap/issues/3684) is the final evidence
owner for the durability graph rooted at #1595. Certification is deliberately
fail-closed: clean shutdown, process termination, ordinary reopen, green CI,
and benchmark results do not satisfy modeled power-loss obligations.

## Evidence tiers

- `clean-process` records graceful close, process termination, or ordinary
  reopen evidence. It is regression coverage, not a corruption-safety claim.
- `modeled-crash` records an allowed stable/dirty image from the deterministic
  power-loss model and reopens only a copy of stable bytes through the normal
  public `treedb.Open` path. Only this tier owns the required corruption-safety
  matrix.
- `block-device` is reserved for a separately provisioned destructive runner.
  No block-device claim is made by the repository certification suite.

## Bundle contract

A certification bundle contains:

```text
power_loss_certification/
  binaries/*.test
  evidence/<case-id>/
  inputs/power_loss_counterexamples.json
  risk_inventory.json
  run_plan.json
  performance.json
  coverage_report.json
  selection_plan.json
  summary.md
  manifests/*.json
```

The versioned risk inventory freezes required values, mandatory retained
counterexamples, negative controls, and explicit covering interactions. Child
manifests carry exact repository and PR provenance, structured test commands,
observed cut metadata, recovery state, expected and actual outcomes, and
SHA-256-addressed artifacts.

`TreeDB/internal/powerlosscert` rejects unknown JSON fields, stale or partial
SHAs, duplicate IDs, undeclared inventory values, incomplete interactions,
non-production profiles, unobserved cuts, missing modeled-crash artifact
classes, and artifact hash mismatches. Coverage and representative selection
count only `modeled-crash` witnesses. Mandatory counterexamples and negative
controls are selected before deterministic greedy set cover.

## Replay artifact capture

Ledger-addressed tests continue to use the existing replay selectors:

```text
TREEDB_POWERLOSS_CUT_ID
TREEDB_POWERLOSS_VARIANT_ID
TREEDB_POWERLOSS_SEED
```

Setting all three variables below enables fail-closed evidence capture for a
public reopen:

```text
TREEDB_POWERLOSS_EVIDENCE_DIR
TREEDB_POWERLOSS_EXPECT_CUT_POINT
TREEDB_POWERLOSS_REOPEN_MODE=read-write|read-only
```

The evidence directory must be empty. Capture writes:

- `operation_trace.json` with the declared cut and actual observed event count;
- immutable `stable-image/` and `dirty-image/` trees plus deterministic tree
  manifests that bind the complete directory namespace (including empty
  directories) and every regular file;
- a separate `recovery-input/` copy so read-write recovery cannot mutate the
  crash-image evidence;
- an immutable `recovery-preopen/` snapshot of the exact stable tree supplied
  to public open, so writable recovery remains verifiable after it mutates
  `recovery-input/`;
- `recovery_trace.json` from the normal public open path, bound to the SHA-256
  of `stable_image_tree.json` and the model's stable fingerprint; and
- `metrics.json` with image sizes, file counts, trace count, and stable-image
  fingerprint.

Every modeled witness registers those five JSON files and
`command_log.json` at their canonical names directly below its declared
`TREEDB_POWERLOSS_EVIDENCE_DIR`; modeled witnesses may not reuse that lexical or
resolved directory as independent coverage, and no evidence-directory component
may be a symlink. The verifier strictly parses every schema,
rehashes every file named by both image-tree manifests, compares their exact
directory sets, rejects extra image paths and symlinks, cross-checks metrics
against the trace and image trees, and binds the recovery trace to the immutable
stable image. The preserved `recovery-preopen/` namespace and bytes must always
match the stable tree exactly. For read-only recovery, the unchanged
`recovery-input/` must match it too; writable recovery may mutate only that
working copy after public open begins.

The command runner owns `command_log.json`. Its versioned JSON envelope records
the exact repository SHA, test-binary path and SHA-256, package, test name,
arguments, replay environment, observed outcome, completion status, exit code,
and captured stdout/stderr. Modeled outcomes use `accepted:<state>` or
`rejected:<reason>`; the verifier binds that class to the public-open recovery
result and the full outcome to the command log. It requires a completed zero
exit and an exact match to the child manifest; a hashed arbitrary log file is
not evidence.

## Exact-SHA runner

`TreeDB/cmd/power_loss_certify` consumes a strict risk inventory and run plan.
The plan freezes the exact repository SHA, PR provenance, replay selector,
profile, reopen mode, expected outcome, typed error, and complete expected
recovery state before any case runs. The runner refuses a different SHA,
tracked worktree changes, and a non-empty output directory. It builds and
hashes distinct test binaries, executes each case once without retry, compares
the observed recovery trace with the frozen expectation, and only then writes
the child manifest.

The final pass verifies coverage, artifacts, image namespaces, command logs,
and the reloaded bundle before reporting success. The performance report
records generation and execution runtime, cases per second, stable-image and
artifact bytes, peak child memory when the platform exposes it, and explicit
zero retry/flaky-retry counts. A failed attempt is retained separately; a rerun
must use a new empty output directory.

Example:

```sh
GOWORK=off go run ./TreeDB/cmd/power_loss_certify \
  -repo-root . \
  -inventory /path/to/risk_inventory.json \
  -plan /path/to/run_plan.json \
  -out /path/to/new/power_loss_certification
```

## Current fail-closed status

The infrastructure alone does not certify current main. Before #3684 can
close, the frozen inventory must be committed and its validator must report
complete modeled-public-reopen ownership at one exact `origin/main` SHA.
Known current gaps include durable-profile modeled cuts, several public API and
authoritative-resource interactions, maintenance/cleanup cuts, durable-prefix
negative controls, and counterexample occurrences currently tolerated by the
canonical enumerator without ledger identities. These gaps must be assigned
and corrected; helper-only or clean/process tests must not be relabeled as
modeled crash evidence.
