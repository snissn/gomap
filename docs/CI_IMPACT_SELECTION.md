# Advisory CI impact receipts

TreeDB CI publishes an impact forecast while every original command and matrix
member continues to execute. The forecast cannot authorize an omission:
`omission_authority` is always `false`. The existing `TreeDB required gate`
continues to accept only successful original dependencies. It has no dependency
on the advisory job, and no execution condition reads a forecast.

The reviewed manifest is `.github/ci/ci_impact.json`; the single authority contract
is `.github/scripts/ci_impact.py plan` / `validate`. It inventories all 14 source
workflows, 56 original members, entry command source locations/digests, module and
checkout contexts, platforms, race variants, and explicit dynamic consumers.
TreeDB contributes 33 original members: 32 execution members and its required
aggregate. The independent advisory job makes 34 physical TreeDB jobs. Forecast
members describe the original coverage universe; they do not claim that
push-only, path-filtered, or manual workflows run on every PR.

## Identity and trust

Ordinary PR correctness is bound to the executed merge candidate. Qualification
requires the exact ordered candidate parents to equal the event's base and PR
head. The candidate tree, complete base/candidate inventories, local diff,
accepted policy, runtime planner, harness objects, original member inventory,
repository, PR number, workflow ref, run ID, origin attempt, raw event digest,
and environment are recorded. Stale/missing/zero identities or unavailable
objects produce a full, unqualified forecast. Push, merge-group, dispatch, and
other contexts currently choose full. No reconstructed historical base or landed
merge may stand in for the unavailable historical synthetic candidate.

The MVCC raw-path and other paired benchmark jobs retain their own explicit
head/base checkouts and comparisons. A merge-candidate impact receipt does not
replace those pair contracts or their current scope handling.

Only the policy in the accepted event base supplies forecast ownership. The
candidate cannot install a permissive policy and certify its own exclusions.
Bootstrap has no accepted policy and chooses full. CI/workflow/shard policy,
modules/toolchain, executable/source, discovery drift, new/unowned paths,
unsupported tree entries, or incomplete discovery also choose full.

Receipts are advisory data, not signed execution attestations. A hosted artifact
must be independently correlated with GitHub run/job metadata and actual full
results before it counts as a shadow observation. `forecast_qualified: true`
means the planner's source-bound forecast contract succeeded; it does not mean
an omission is authorized, a job passed, or a domain is qualified for promotion.
Local replay is distinguished from hosted shadow by `execution_context`.

## Input ownership

The planner obtains complete, successful NUL-delimited `git ls-tree` inventories
and `git diff --name-status -z --find-renames` locally. It never uses PR file APIs
or newline splitting. Deletes retain base ownership; renames union old and new
ownership. New inputs force full even inside an existing glob.

The first reviewed forecast boundary covers existing nonexecutable root `docs/`
inputs. It retains all TreeDB test/race members (including every Windows named
partition), root test/docs-check members, and the always-running required gate.
TreeDB's tracked UTF-8 naming scan consumes retained `RESULTS.json`; docs globs,
source authority scans, subprocess witnesses, and Python-launched services are
explicitly inventoried. Go/Python/shell files under docs remain executable/source
inputs and choose full. TreeDB vet, MVCC comparison scope, and the two performance
jobs have separate entry commands; root docs data does not become their input
merely by sharing the workflow. This is a forecast for those commands, not a
blanket docs exemption.

No package reverse closure or individual-test scheduler is installed. Existing
`weighted_shard_file`, TSV pins, preserve-fallback behavior, modulo fallbacks,
caching heavy/rest partitions, power-loss route, and dedicated race routes remain
the execution authority. Assignment is on the full inventory before any future
intersection; affected packages retain every named partition.

The manifest's discovery-source fingerprint covers tracked Go/Python/shell,
assembly/C/C++/Objective-C/header sources, native objects, module files, and
Makefile across the repository, including executable `.github/` harnesses.
An accepted later source edit can introduce a new dynamic consumer. Until the
inventory is reviewed and refreshed, source drift chooses full on later events,
even if those events only edit docs. This conservative behavior can reduce the
qualified sample rate. Report it honestly; do not remove freshness checks to
manufacture forecast opportunity.

Known coverage gaps remain explicit: manual workloads, existing workflow trigger
limits, nested modules, and clients without existing qualification are not new
coverage supplied by the planner. Full fallback means full **existing** execution.

## Artifacts and replay

The artifact is `ci-impact-shadow-<run_id>-<origin_run_attempt>` and contains
`receipt.json` (schema version 1), the raw `event.json`, and `environment.json`.
Retention is 30 days. Original attempts remain distinct; a rerun's current API
attempt number does not rename earlier successful job or receipt lineage.

To replay with exact locally available objects and independently retained event
and environment inputs:

```sh
python3 .github/scripts/ci_impact.py plan --repo . \
  --event-name pull_request --event-path /path/to/event.json \
  --candidate <exact-merge-sha> --run-id <origin-run-id> \
  --run-attempt <origin-attempt> --workflow-ref <original-workflow-ref> \
  --environment-path /path/to/expected-environment.json \
  --output /path/to/replay/receipt.json
```

Use the same arguments with `validate` and the retained receipt path to verify
it by independent recomputation. Validation rejects absent, duplicate, unknown,
changed, or forged member decisions and identity. The expected environment must
come from independently retained original-run evidence; never infer it from the
receipt being validated. Without `--environment-path`, the CLI uses the current
host, which intentionally rejects a receipt from another environment. Retain the
original hosted workflow ref when validating it; the default `local-replay` ref
identifies a local investigation. Output event/environment files are owned by the
output directory; choose a separate directory when preserving original artifacts.

The advisory workflow fetches only exact base/head objects at depth one. Fetch or
planner errors remain nonblocking and leave original execution intact. Missing
artifacts are unqualified observations. There is no all-history fetch or result
reuse. The run owns its small scratch directory; runner teardown and artifact
retention bound its lifetime.

## Refresh and qualification

After reviewing changed consumers, workflow commands, contexts, or membership,
stage only the intended source and workflow paths with `git add -- <paths>`,
including intended additions/deletions and edits to the maintenance scripts.
Review `git diff --cached` before refreshing. Use an isolated worktree when the
primary checkout contains unrelated work; the command never stages or resets it.

Then refresh and check the intended change:

```sh
uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py
uv run --with pyyaml python .github/scripts/test_refresh_ci_impact_inventory.py
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_ci_impact.py
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_treedb_ci_contract.py
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_treedb_windows_core_headroom.py
git add -- .github/ci/ci_impact.json
```

The maintenance command deterministically refreshes source bindings, compact
entrypoint inventories, and original members from one immutable `git write-tree`
snapshot of the index. Both source blob fingerprints and workflow names/bytes
come from that same tree. Untracked files and unstaged edits are excluded, so a
partially staged file uses its staged bytes; staged additions/deletions are
included. An unmerged or unsupported index is rejected before writing the
manifest. Reviewed ownership/rules remain the explicit working-manifest input;
the command preserves them and flags new owners as `UNREVIEWED`. It does not
accept new consumers or activate authority. Runtime qualification rejects unresolved
owners and selectors that name no inventoried workflow/job. Both `.yml` and
`.yaml` workflows are discovered. Review the changed footprint rules,
dynamic-reader inventory, variants, owners, source fingerprint, and known gaps
alongside the staged diff; resolve owners/rules and rerun before committing the
reviewed manifest in the same PR. If any intended source/workflow bytes are staged
after refresh, rerun it before staging the final manifest. A commit containing
the refreshed index and manifest then has the matching discovery fingerprint.
Run checks from a checkout of the intended tree when unrelated unstaged workflow
edits would affect existing worktree-based contract tests. PyYAML is only a
maintenance dependency; the runtime planner uses the Python standard library.

The [parent tracker](https://github.com/snissn/gomap/issues/5049) requires the
[P1 qualification packet](https://github.com/snissn/gomap/issues/5051) to collect
at least 100 **new** full-CI PR candidate events over at least 14 days, including
explicit fallback and unqualified observations, plus adequate named-domain
coverage. A positive named GO decision is required before any later promotion;
NO-GO never authorizes omissions. Shadow forecasts do not measure actual savings.
The packet compares forecasts against unchanged full results,
historical replay where exact identities exist, and injected adverse cases. It
reports planner overhead, fallback/qualified rates, and conservative per-domain
opportunity. A named domain requires adequate source/platform/input coverage and
zero unresolved misses. Negative findings may require a NO-GO decision. Any
future enforcement needs separately accepted evidence, executed-or-authorized
member receipts, success-only aggregate semantics, a permanent owner, and a
proved full rollback. Current advisory receipts cannot supply those approvals.
