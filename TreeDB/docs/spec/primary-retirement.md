# PRIMARY retirement provenance

DATA is the only storage representation. The two independent durable recovery
roots, their immediate-parent proofs, stable resource identities, original
registry and snapshot cleanup ownership remain active. Persistent value-log
segments remain reachability managed storage.

The last selected PRIMARY-capable implementation and its representation-specific
diagnostics are preserved at source commit
`1c04403a7cab806ebf55b0e676ae46688fcc3288`. The earlier audited retirement
prototype is `f5c010a226dfb9e2033ea0095ffd22f94dc85ed1`; its source-bound
results do not qualify this current retirement candidate. This includes the removed
`internal/primaryarena`, capsule and paired-vacuum constructors, PRIMARY routing,
and their representation-specific stale-build witnesses.
The earlier strict-refusal prototype is
`b121d2e3167329201fcf0f1780be8d70ef5f82e8`.

The saved PRIMARY fixture consumed by `directDATAExtractPRIMARYFixture` remains
unchanged. `TestDirectDATAPersistedPRIMARYRequiresRebuild` uses its immutable
content and file-identity inventory to prove refusal through public Open and
both backend factories in writable and read-only modes. Existing DATA public
seal, pointer, old-view, fallback, cleanup-failure and power-loss bodies continue
to execute against the DATA path. Retired PRIMARY-only oracle anchors do not
certify DATA behavior and are removed from the active witness registry.

Offline vacuum and value-log rewrite also refuse retired PRIMARY before opening
side stores or acquiring LOCK. Direct index-swap recovery checks the original
namespace before cleanup or rename. The complete saved store and interrupted
staging/joint-decision namespaces must retain their bytes, names and identities
on refusal; ordinary DATA maintenance keeps its existing behavior.

The generic stale-build rejection/retry requirement and its required interaction
remain unchanged. `TestPowerLossCertificationStaleBuildBaseDATAPublicReopen`
uses a fresh DATA producer, the original ordered-root rendezvous, rejection
before acknowledgement, a distinguishable retry and public crash reopen. Its
`stale-build-base-data-current` witness, cut and seed are independently bound;
the historical DATA V1 generation-8 archive remains unchanged with zero current
coverage. The three live inventories are `power_loss_counterexamples.json`,
`power_loss_risk_inventory.json` and `power_loss_witness_contracts.json`.

Construction alone does not establish consumer migration or release acceptance.
The graph's native/history consumer and integration gates remain required.
