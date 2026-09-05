package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The immutable catalog owns only bounded metadata. It never pins a pager or
// reuses old page IDs by control digest: each cold closure reads the aliases
// through a fresh snapshot. Catalog reuse itself follows existing pager,
// system-root and commit-sequence rules, including across collection managers.
func (c *Collection) typedGraphBaseRequirements(expected *typedGraphBaseAlias) (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
	var work rootpublication.StableResourceClosureWork
	if expected == nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, backenddb.ErrClosed
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, expected.meta.Name)
	if err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	if catalog == nil || catalog.typedGraphBase == nil || !collectionMetaValuesEqual(expected.meta, catalog.typedGraphBase.meta) {
		return rootpublication.StableLogicalObligationRequirements{}, work, ErrVectorIndexSnapshotMismatch
	}
	return catalog.typedGraphBase.requirementsAtSnapshot(snap)
}

func (base *typedGraphBaseAlias) requirementsAtSnapshot(snap *backenddb.Snapshot) (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
	var work rootpublication.StableResourceClosureWork
	cfg := base.meta.Options.ColumnStore
	root := base.roots[collectionColumnManifestRootName(base.meta.Name)]
	if root == 0 || cfg == nil || cfg.ActiveManifest == nil || cfg.AssetManager == nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, ErrVectorIndexSnapshotMismatch
	}
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, root)
	if err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, base.meta.Name, "typed graph base closure"); err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	requirements, err := stableColumnManifestDurableRequirementsWithWork(records, cfg.ActiveManifest.Generation, cfg.AssetManager.Namespace, &work)
	return requirements, work, err
}

func (c *Collection) unionTypedGraphBaseRequirements(current rootpublication.StableLogicalObligationRequirements, base *typedGraphBaseAlias) (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
	if base == nil {
		return current, rootpublication.StableResourceClosureWork{}, nil
	}
	captured, work, err := c.typedGraphBaseRequirements(base)
	if err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, work, err
	}
	union, err := rootpublication.MergeStableLogicalObligationRequirements(current, captured)
	return union, work, err
}

func (c *Collection) bindTypedGraphBasePlanClosure(lease *columnPublishPlanLease, base *typedGraphBaseAlias) error {
	if base == nil {
		return nil
	}
	plan := &lease.plan // Not published or shared until this preparation returns.
	if fallback := plan.durableResourceRequirementsFallback; fallback != nil {
		// No eager manifest scan or exact registration on the certified append
		// path. The union is materialized only if existing certification falls back.
		plan.durableResourceRequirementsFallback = func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			current, work, err := fallback()
			if err != nil {
				return current, work, err
			}
			union, baseWork, err := c.unionTypedGraphBaseRequirements(current, base)
			work.Add(baseWork)
			return union, work, err
		}
		return nil
	}
	union, work, err := c.unionTypedGraphBaseRequirements(plan.durableResourceRequirements, base)
	if err != nil {
		return err
	}
	plan.durableResourceRequirements = union
	plan.durableResourceRequirementWork.Add(work)
	// An old current-manifest reference may still belong to the captured base.
	// On this cold exact path let existing exact filtering reconcile ownership;
	// current-only Removed evidence would contradict the complete union.
	plan.durableResourceMutation = rootpublication.StableLogicalObligationMutation{}
	return nil
}

func (c *Collection) typedGraphBaseReachabilityRefs() ([]ColumnAssetRef, error) {
	if c == nil || c.db == nil {
		return nil, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, c.collectionName())
	if err != nil {
		return nil, err
	}
	if catalog == nil || catalog.typedGraphBase == nil {
		return nil, nil
	}
	requirements, _, err := catalog.typedGraphBase.requirementsAtSnapshot(snap)
	if err != nil {
		return nil, err
	}
	refs := make([]ColumnAssetRef, 0, len(requirements.Obligations))
	for _, obligation := range requirements.Obligations {
		refs = append(refs, ColumnAssetRef{Kind: ColumnAssetKind(obligation.Kind), Namespace: obligation.Namespace, Generation: obligation.Generation, PartID: obligation.PartID, FileID: uint32(obligation.FileID), Offset: obligation.Offset, Length: obligation.Length, Checksum: obligation.Checksum})
	}
	return refs, nil
}
