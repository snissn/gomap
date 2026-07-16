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
  risk_inventory.json
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

Setting both variables below enables fail-closed evidence capture for a public
reopen:

```text
TREEDB_POWERLOSS_EVIDENCE_DIR
TREEDB_POWERLOSS_EXPECT_CUT_POINT
```

The evidence directory must be empty. Capture writes:

- `operation_trace.json` with the declared cut and actual observed event count;
- immutable `stable-image/` and `dirty-image/` trees plus deterministic tree
  manifests;
- a separate `recovery-input/` copy so read-write recovery cannot mutate the
  crash-image evidence;
- `recovery_trace.json` from the normal public open path, bound to the SHA-256
  of `stable_image_tree.json` and the model's stable fingerprint; and
- `metrics.json` with image sizes, file counts, trace count, and stable-image
  fingerprint.

Every modeled witness registers those five JSON files and
`command_log.json` at their canonical names directly below its declared
`TREEDB_POWERLOSS_EVIDENCE_DIR`. The verifier strictly parses every schema,
rehashes every file named by both image-tree manifests, rejects extra image
files and symlinks, cross-checks metrics against the trace and image trees, and
binds the recovery trace to the immutable stable image. For read-only recovery,
the preserved `recovery-input/` bytes must still match the stable tree exactly.

The command runner owns `command_log.json`. Its versioned JSON envelope records
the exact repository SHA, test-binary path and SHA-256, package, test name,
arguments, replay environment, observed outcome, completion status, exit code,
and captured stdout/stderr. Modeled outcomes use `accepted:<state>` or
`rejected:<reason>`; the verifier binds that class to the public-open recovery
result and the full outcome to the command log. It requires a completed zero
exit and an exact match to the child manifest; a hashed arbitrary log file is
not evidence.

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
