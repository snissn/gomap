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
  inputs/power_loss_witness_contracts.json
  risk_inventory.json
  run_plan.json
  performance.json
  coverage_report.json
  selection_plan.json
  summary.md
  manifests/*.json
  bundle_seal.json
```

The committed versioned risk inventory freezes required values, mandatory
retained counterexamples, negative controls, and explicit covering
interactions. The committed witness-contract file binds the certification
issue, the complete ordered graph PR-number sequence, every replay selector,
and every expected recovery state to the risk labels they are allowed to own.
The runner requires the supplied inventory to be byte-identical to the
exact-SHA committed copy, the plan's PR-number sequence to equal the committed
graph sequence, and every plan case to be identical to its committed contract,
so a caller cannot omit implementation provenance or shrink or relabel
coverage. The runner reads the inventory, witness contracts, counterexample
ledger, and test sources from one private exact-SHA checkout; the caller's
inventory path is only accepted when it is byte-identical to that checkout.
Child manifests carry exact repository and PR provenance, structured
test commands, observed cut metadata, recovery state, expected and actual
outcomes, and SHA-256-addressed artifacts.

`TreeDB/internal/powerlosscert` rejects unknown JSON fields, stale or partial
SHAs, duplicate IDs, undeclared inventory values, incomplete interactions,
non-production profiles, unobserved cuts, missing modeled-crash artifact
classes, and artifact hash mismatches. `bundle_seal.json` additionally binds
every regular file in the finished bundle; its own SHA-256 is printed for
publication outside the artifact location. Coverage and representative
selection count only `modeled-crash` witnesses. Mandatory counterexamples and
negative controls are selected before deterministic greedy set cover.

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

- `operation_trace.json` with the declared cut, replay variant, optional
  replay-window contract, scoped observed event count, and complete retained
  operation trace;
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

The current contract uses run-plan and witness-contract schema v4,
child-manifest schema v3, operation-trace schema v2, and recovery-trace schema
v2. The plan freezes the expected slash-separated recovery directory; the
child manifest retains that expectation, and the recovery trace must identify
the same canonical `recovery-input` root or descendant. A windowed case also
freezes one replay window equal to its replay variant. Capture and retained
verification require exactly one matching marker followed by the selected cut;
missing, duplicate, mismatched, late, or undeclared markers fail closed while
the full pre-window trace remains retained. These version bumps are intentional
because strict readers of the preceding schemas do not accept the new fields,
child-root semantics, state comparison, or window-relative cut addressing.

Every modeled witness registers those five JSON files and
`command_log.json` at their canonical names directly below its declared
`TREEDB_POWERLOSS_EVIDENCE_DIR`; modeled witnesses may not reuse that lexical or
resolved directory as independent coverage, and no evidence-directory component
may be a symlink. The verifier strictly parses every schema,
rehashes every file named by both image-tree manifests, compares their exact
directory sets, rejects extra image paths and symlinks, cross-checks metrics
against the trace and image trees, and binds the recovery trace to the immutable
stable image. The resolved production profile in the recovery trace must equal
the profile required by the recorded witness command. The preserved
`recovery-preopen/` namespace and bytes must always match the stable tree
exactly. For read-only recovery, the unchanged
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

`TreeDB/cmd/power_loss_certify` consumes a strict risk inventory and version-4
run plan. The plan freezes `refs/remotes/origin/main`, its exact repository SHA,
PR provenance, replay selector and optional replay window, profile, reopen
mode, expected outcome, typed error, state-comparison rule, complete expected
recovery state, per-case timeout, captured-output limit, per-case evidence
limit, and whole-bundle byte limit before any case runs. Before checking out
evidence, the runner proves that the frozen cases
match the committed witness contracts and cover every inventory value, retained
counterexample, negative control, and required interaction. It then refuses a
different current-main ref or HEAD, tracked or untracked worktree changes, and
a non-empty output directory. It rechecks the ref, HEAD, and worktree after
building and after execution to detect mid-run repository changes. Provenance
validation first requires the plan to contain exactly the immutable ordered
PR-number sequence in the committed witness contract. It then requires every
claimed PR merge to be reachable from the certified repository SHA in that
topological order, binds the PR number to the immutable merge subject, and
requires the claimed head to be either a non-first merge parent,
tree-identical to the squash merge, or the exact clean three-way input that
produces a stale-base squash merge tree. Omitting a graph merge, substituting
another valid PR, or supplying syntactically valid but unrelated PR metadata
therefore cannot enter a sealed bundle. Provenance
checks run with a constrained Git environment, so inherited repository,
worktree, index, object-store, namespace, and configuration overrides cannot
redirect validation away from the certified repository. Git is invoked from a
fixed trusted system path rather than the inherited `PATH`.

The runner materializes the planned commit in a private detached clone with an
independent object store, an empty Git template and hooks directory, and
sanitized checkout configuration. It builds only from that clone, so ignored
files, repository-local hooks, and local configuration from the caller's
worktree cannot alter a claimed exact-SHA binary. It resolves the default Go
binary from the running tool's compiled GOROOT only when no `GOROOT` override
is inherited; an inherited override requires an explicitly absolute `-go`
path. The runner then requires the selected tool's reported version to
match the Go runtime that built the runner, and builds with a minimal fixed
environment (`GOENV=off`, empty `GOFLAGS`, `GOTOOLCHAIN=local`, and
`GOWORK=off`). Witnesses run under a separately recorded minimal environment,
with no inherited TreeDB configuration. It builds and hashes distinct test
binaries, executes each case once without retry and within its frozen resource
bounds, compares the observed recovery trace with the frozen expectation, and
only then writes the child manifest.

The final pass verifies coverage, artifacts, image namespaces, command logs,
the whole-bundle seal, and the reloaded bundle before reporting success. The
performance report records generation and execution runtime, cases per second,
stable-image and artifact bytes, peak child memory when the platform exposes
it, and explicit zero retry/flaky-retry counts. A failed attempt is retained
separately; a rerun must use a new empty output directory.

Example:

```sh
GOWORK=off go run ./TreeDB/cmd/power_loss_certify \
  -repo-root . \
  -inventory /path/to/risk_inventory.json \
  -plan /path/to/run_plan.json \
  -out /path/to/new/power_loss_certification
```

## Current fail-closed status

The infrastructure and candidate witnesses alone do not certify current main.
This suite now contains modeled-public-reopen owners for the frozen inventory,
including all retained counterexamples and negative controls, but #3684 stays
open until all prerequisite corrections merge and one complete run plan passes
preflight and executes from a clean exact `origin/main` SHA. The resulting
bundle, hashes, retry history, performance report, and claim boundary must then
be published as the certification evidence. Helper-only or clean/process tests
must not be relabeled as modeled crash evidence.
