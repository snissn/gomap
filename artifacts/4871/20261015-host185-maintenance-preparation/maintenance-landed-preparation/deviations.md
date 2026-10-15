# Changes from reviewed branch-specific harness

Reference: maintenance-hosted-runner HEAD `22e605b5c738ce759c5a7b8a3195f8d3ed53132c`. Each corresponding file has an old-to-new diff beside this report.

- Added a new standalone manual workflow instead of modifying the original M0 workflow. Dispatch is restricted to reviewed main, with fixed literal product SHAs, pinned actions, ubuntu24.04 and contents:read. There are no arbitrary-ref inputs, PR/push triggers, secrets or persistent runners.
- Preserved temporary candidate2588 solely for preparation and added the fail-closed `CANDIDATE_FINALIZED=False` execution guard. Root final binding remains pending.
- Replaced branch-specific harness context with main. Retain exact harness commit/file manifest and archive independently from pristine product manifests/archives; require clean fixed product inputs. Verify dependency preparation leaves source unchanged, then run offline.
- Retain full SDK manifest/archive, compiler/linker/tool identities and relevant Go/CGO/PATH/TMPDIR environment, alongside actual executable bindings. Add failure-time artifact hashes and an initialization marker before checkout.
- Replace leader-only cleanup proof with shared Linux subreaper/process-group cleanup, adopted-descendant waitpid receipts and an observed empty group. Preserve original operation timeouts; add only a bounded two-second cleanup drain. Unexpected survivors refuse success even if the leader exited zero. Signal interruption enters the same cleanup path; repeated signals cannot interrupt the drain. Drivers remain in the same supervisor process. Historical evidence is not relabeled as joined.
- Retain bootstrap and pure-test logs; upload failure/refusal evidence always; explicitly exclude caches, temporary/build directories and source worktrees while preserving source archives and SDK archive.
- Add pure unit/static checks for fixed schedule/context, unchanged reviewed parser/algorithm, malformed/incomplete rows, process-group normal/kill/unexpected-survivor cases and manual workflow restrictions. No product execution is used by these checks.
- Update README with fixed inputs, caller/timer scope, land-before-collection requirement, provenance, process custody and infrastructure limitations.

Unchanged: all 14 calls, fixed selectors, counts16x/1x/128x, AB/BA ordering, parser semantics, 10-second/250ms/2% quiet preflight, CPU/GOMAXPROCS/memory settings, per-child operation caps, absence of retries and diagnostic-only classification. No product, gate, benchmark/test body, M0 capture/summarizer or native code changes.
