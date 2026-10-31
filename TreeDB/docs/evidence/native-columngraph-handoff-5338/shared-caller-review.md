# Shared-caller review: ordinary native column-graph handoff

Source inspection found no additional propagation blocker introduced by the handoff repair in the examined callers. This is a caller-boundary review, not execution or protocol-wide certification.

## Source identity

Authenticated GitHub source: `9cd7557fb705b07261b801a2f42aa90dee4ce171`, tree `dff272aeed265395770cb6eba7b17eeb6463618b`. Local HEAD remains baseline `d7db5409e20412f4dac4e1a3f902484b05e014e1`; the candidate is represented by staged/worktree bytes. Every inspected file matched the parent-authenticated frozen source inventory, whose candidate staged tree equals the GitHub source tree.

- Source inventory: `lifecycle-source-freeze.json`, SHA-256 `0e037265f65d222e4a353ccbc51d2c2fe7403b5a8474deb2c87869f962512476`.
- Exact per-file SHA-256 and Git blob identities: `shared-caller-review.source.json`, SHA-256 `ee9464255b69c143ff76eed28f268148f6b910bb2447f616c81cab102b959c41`.
- Binding covers 22 production files and two test files inspected as source. No Go tests or source edits were performed for this review.

## Call paths and outcomes

| Caller | Source conclusion |
| --- | --- |
| `collections.Collection.RebuildVectorIndex` | Native handles call `nativeRebuildVectorGraph` directly and return its status/error unchanged. Only the exclusive physical-source stale refusal can trigger one fresh preassignment attempt. Assigned, uncertain, accepted, logically installed or ambiguous outcomes are ineligible. Logical-installed completion errors and status failure after success retain `ErrCommitAmbiguous`; transaction-close errors are joined and disallow refresh. |
| `documentservice.Service.OptimizeIndex` | Ordinary Optimize drains through `SyncForStandaloneWriteConcern` or the deferred finalizer, then calls the same collection rebuild once. The finalizer returns the first failing drain/checkpoint/build stage without retrying it. Optimize returns a failing rebuild before publishing a successful maintenance response. The typed `build` action also calls the same rebuild and proceeds to serving setup only on success. |
| Service/manager/public owner close | `Service.Close` owns only cached service resources and joins their errors. Manager close retains its original result and joins native flush, cache and final flush errors. Public `DB.Close` records and joins the original final command-WAL checkpoint failure with later teardown/background failures. It cannot turn that first call's failure into success. |
| Native-wire collection operations | JSON insert and omit-result routes call the existing native collection mutation path; native transaction completion marks assigned/logically installed ordinary insert errors ambiguous and preserves cleanup errors. Metadata wrappers use `%w`. Flush/checkpoint routes return a failed physical boundary before emitting success ACK metadata. Thus their pending rows and later handoffs consume the repaired shared producer seam without a caller-specific implementation. |
| Native-wire error transport | `errorCodeFor` recognizes underlying collection ambiguity/recovery markers before cancellation. `writeError` preserves the selected code and retry hint; ambiguity is nonretryable, and recovery-required overrides the ordinary durability retry hint. The insert combiner does not fall back to individual requests after a recognized ambiguous combined result. |
| Native-wire rebuild request | There is no direct production `RebuildVectorIndex` or `OptimizeIndex` call in native-wire. `RequestVectorPartitionRebuildV1` forwards to an optional node-owned enqueue hook and returns its error; that hook's implementation and asynchronous lifecycle are separate from the ordinary inline rebuild audited here. |

## Existing limits that must remain explicit

`mapCollectionMaintenanceError` retains the underlying error through `documentservice.Error.Unwrap`, so Go callers can still inspect original causes, acceptance markers and ambiguity with `errors.Is`/`errors.As`. It does **not** assign dedicated conflict/ambiguity service codes to ordinary maintenance errors: absent a more specific existing service error, they remain outer `CodeInternal`. HTTP returns that existing structured classification; this change does not improve it.

Native-wire likewise does not universally relabel every ACK-barrier failure after a successful mutation as ambiguous. The review establishes preservation of the markers actually supplied by the shared collection/storage path and refusal to send a success ACK after an error; it does not establish a new accepted-outcome protocol.

The cold-hybrid fixture checks service, manager and owner close separately. The close-failure test source checks the first original owner close's injected checkpoint error and recovery-required marker, then exits without replacing it with a retry or second close. Those are source assertions, not test-result claims in this note.

The inspected caller paths and error mappings are otherwise unchanged by this patch. No claim is made that issue #5333's separate zipper/ownership problem is repaired, that its files changed, or that the original historical CI incident's hidden cause has been attributed.
