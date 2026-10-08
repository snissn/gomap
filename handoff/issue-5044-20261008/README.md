# Issue 5044 host migration recovery bundle

This artifact-only branch is a handoff checkpoint. Do not merge it or treat it as an implementation candidate. The authoritative execution graph is https://github.com/snissn/gomap/issues/5044.

The coordinator has stopped new implementation, runtime, and review requests on the departing Mac. Fetch this bundle on the new system; original absolute paths in retained receipts are historical provenance, not required working directories or permission to run anything.

## Source to resume

| Work | Branch | Published commit | Remaining gate |
| --- | --- | --- | --- |
| C3 read/MVCC, PR 5080 | codex/cow-c3-mvcc-views | 04fd2951ac56af0e0473c4037e71f2bd81612f29 | Refresh current-head CI/policy; Codex clean; unmerged |
| C4 tooling, PR 5082 | codex/cow-c4-sustained-tools | d7328c212285031e3b5dcb76573c5491e1f918fe | Independent bounds review, current CI, final C3 base, genuine v3 |
| Provisional WAL recovery for 5047 | codex/5047-floor-reopen-provisional | 72d7dcda2ad4922157e61ba8229f283c106579e9 | Native all-19 contract and final base before integration |

Clone gomap on the target system, fetch those refs, and create isolated issue worktrees. Preserve active owners and use GOWORK=off. Go 1.26+ and applicable repository/skill instructions still govern.

## Extract and verify

From an existing clone:

~~~sh
git fetch origin refs/heads/codex/cow-5044-handoff-20261008
resume_dir=$(mktemp -d /tmp/cow-5044-resume.XXXXXX)
git archive FETCH_HEAD handoff/issue-5044-20261008 | tar -x -C "$resume_dir"
bundle_dir="$resume_dir/handoff/issue-5044-20261008"
shasum -a 256 "$bundle_dir/recovery-bundle.tar.gz"
mkdir "$bundle_dir/payload"
tar -xzf "$bundle_dir/recovery-bundle.tar.gz" -C "$bundle_dir/payload"
~~~

Expected archive SHA256: 1816db940b1a56a222231fbec1d306d7ac0a803d4ef937b8879f7226e35e0132.

manifest.json gives each archived file's bytes, SHA256, archived mode, original freeze mode, and role. All 250 regular members were verified before publication. Read-only preservation can remove write bits after the original freeze; both modes remain recorded.

## Retained packets and applicability

- pr5082-cow-boundary-budget-preparation-r1 contains the exact five-path bounds-fix preparation, frozen final source, author receipt, original RED, 19 passing contract tests, 41 offline controls, and handoff-commands.md. Independent review of this new batch is pending. The 25 new smoke mutators have not run against a fresh genuine v3 packet.
- c4-current-v3-finite-construction-preparation-r1 preserves 111 source-only derivative files and controllers. This is HELD and predates the new bounds fix/final landed base. Rebuild bindings, review, and obtain actual runtime admission before use. No fresh v3 execution is claimed.
- pr5081-current-v3-qualification-plan-r1 records the earlier five-action plan. Update it for the 25 bounds cases and final source. Historical v2 packets cannot be relabeled v3.
- issue5076-performance-root-acceptance-r1 preserves all 167 original adverse/noise dispositions and the scoped cost-acceptance decision. The full 756-process performance run is accepted within that decision; it is not a claim of a universal speedup.
- native-5047-floor-wal-* retains source/review receipts for 13 passing normal/race nodes. This is process-exit recovery, not power-loss or native-prune qualification.
- full-r1/final-independent-review-r1 and pr5080-final-doc-and-thread-independent-review-r1 retain accepted reviews.
- current-functional/ retains accepted 65 normal plus 65 race ordinary COW fixture receipts. Native resource reclamation remains separate.
- root-checkpoint/ records historical live-state snapshots and the original Windows fixed-peer timeout log. Refresh GitHub facts on resume.

The frozen preparation packet's helper scripts preserve their original paths. For reconstruction on another machine, bind source and fixtures to the extracted payload/new worktree, record the new invocation separately, and do not overwrite historical receipts. The committed c4_contract_test.py can be run directly with python3 -B. Expensive qualification remains held.

## Evidence available directly from GitHub

Accepted performance decision and every flagged cell: https://github.com/snissn/gomap/issues/5076#issuecomment-6065636479
Full run: https://github.com/snissn/gomap/actions/runs/37789918189
Artifact ID: 11564151605, c3-hosted-final-37789918189-1
Download: gh api repos/snissn/gomap/actions/artifacts/11564151605/zip > c3-hosted-final.zip
ZIP SHA256: 24db42c4484e32ae04ce606322768d7d4808d4c84a84de096da64a3d5feb87f7
Contained tar SHA256: 84983221479033247fa873cfb4690e5e1bd4a2a41db67d933149472f552b2842

GitHub Actions artifacts may expire; retain the original downloaded packet if migrating evidence storage. Source, acceptance decisions, and this small recovery bundle are durable Git refs.

## Next actions and ownership

1. Refresh PR 5080's exact-head required aggregate, review threads, policy, and live main. Its 04fd Codex review is already clean; do not request duplicate review.
2. Independently review the d732 bounds batch and dispose the actual PR 5082 finding. Issue 5119 separately owns the Windows fixture runtime-open ACK fix; no implementation was started there. Preserve the 8-second rejection bound and print child logs on forced cleanup.
3. Reconcile PR 5082 once against C3's actual landed base. Rebuild/review the derivative and collect genuine 36-leaf one-epoch v3 evidence with original 12 and new 25 refusal controls plus serialized checks.
4. Respect the active Native owner in issues 5111 and 5118. Their public handoff/checkpoints own their branches and all-19 acceptance; do not infer a usable frozen ABI from component checks.
5. Integrate the provisional 5047 tests only after the Native contract is usable, complete public prune/floor/reclamation fixtures, then qualify the full sustained 5048 outcome.

No Linux185/111 runtime grant is conveyed by these files. Obtain an owner-safe concrete window or a separately reviewed construction-only hosted runner. Existing C3 hosted workflow cannot substitute for C4. Never bypass missing native eligibility, physical-release acknowledgements, required CI, review, or parent outcome gates.
