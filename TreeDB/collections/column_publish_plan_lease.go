package collections

import (
	"errors"
	"fmt"
	"os"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var (
	errColumnPublishPlanLeaseConsumed           = errors.New("collections: column publish plan lease already consumed")
	errColumnPublishPlanLeaseNotInstalling      = errors.New("collections: column publish plan lease is not installing")
	errColumnPublishPlanLeaseCleanupQuarantined = errors.New("collections: column publish plan cleanup retained quarantined assets")
)

type columnPublishPlanLeaseState uint8

const (
	columnPublishPlanLeasePrepared columnPublishPlanLeaseState = iota + 1
	columnPublishPlanLeaseInstalling
	columnPublishPlanLeaseCommitted
	columnPublishPlanLeaseAbandoned
	columnPublishPlanLeaseQuarantined
)

// columnPublishPlanLease is the sole owner of one unpublished plan's physical
// assets and stable resources until publication takes them or cleanup proves
// that the unpublished tail can be removed.
type columnPublishPlanLease struct {
	mu sync.Mutex

	collection  *Collection
	plan        ColumnPublishPlan
	state       columnPublishPlanLeaseState
	transferred bool

	preparedLease   *ColumnAssetLifecycleRegistryLease
	pendingLease    *ColumnAssetLifecycleRegistryLease
	quarantineLease *ColumnAssetLifecycleRegistryLease
}

func newColumnPublishPlanLease(collection *Collection, plan ColumnPublishPlan) (_ *columnPublishPlanLease, retErr error) {
	if collection == nil || collection.db == nil {
		plan.releaseStableResources()
		return nil, errCollectionDBNil
	}
	if !plan.Enabled || plan.Collection == "" || plan.AppliedCommandLSN == 0 {
		plan.releaseStableResources()
		return nil, errors.New("collections: column publish plan lease requires an enabled bound plan")
	}
	if collection.meta.Name != "" && plan.Collection != collection.meta.Name {
		plan.releaseStableResources()
		return nil, fmt.Errorf("collections: column publish plan lease collection %q does not match %q", plan.Collection, collection.meta.Name)
	}

	lease := &columnPublishPlanLease{collection: collection, plan: plan, state: columnPublishPlanLeasePrepared}
	refs := columnPublishPlanPreparedRefs(plan)
	if len(refs) == 0 {
		return lease, nil
	}
	preparedLease, err := collection.RegisterColumnAssetPreparedAssets(ColumnAssetPreparedAssetRegistrationOptions{
		Owner:  lease.owner(),
		Source: "column_publish_plan",
		Reason: "unpublished column publish plan",
		Refs:   refs,
	})
	if err == nil {
		lease.preparedLease = preparedLease
		return lease, nil
	}
	retained := refs
	var cleanupErr error
	if !plan.stablePreparedAssets {
		retained, cleanupErr = cleanupUnpublishedColumnPublishAssets(collection.db.ColumnAssetRootDir(), plan.PreparedAssets)
	}
	plan.releaseStableResources()
	if len(retained) != 0 {
		_, quarantineErr := collection.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
			Owner: lease.owner(), Source: "column_publish_plan", Reason: "prepared-plan registration failed", Refs: retained,
		})
		cleanupErr = errors.Join(cleanupErr, errColumnPublishPlanLeaseCleanupQuarantined, quarantineErr)
	}
	return nil, errors.Join(err, cleanupErr)
}

func (l *columnPublishPlanLease) owner() string {
	return fmt.Sprintf("column-publish-plan-%s-%d", l.plan.Collection, l.plan.AppliedCommandLSN)
}

func (l *columnPublishPlanLease) beginInstall(collection string, appliedCommandLSN, baseManifestRootID uint64) (ColumnPublishPlan, error) {
	if l == nil {
		return ColumnPublishPlan{}, errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeasePrepared {
		return ColumnPublishPlan{}, errColumnPublishPlanLeaseConsumed
	}
	if l.plan.Collection != collection || l.plan.AppliedCommandLSN != appliedCommandLSN ||
		l.plan.ManifestRootBaseID != baseManifestRootID || l.plan.RootDelta.BaseRootID != baseManifestRootID {
		return ColumnPublishPlan{}, fmt.Errorf("collections: prebuilt column publish plan binding mismatch collection=%q lsn=%d base=%d", collection, appliedCommandLSN, baseManifestRootID)
	}
	refs := columnPublishPlanPreparedRefs(l.plan)
	if len(refs) != 0 {
		pendingLease, err := l.collection.RegisterColumnAssetPendingPublish(ColumnAssetPendingPublishRegistrationOptions{
			Owner: l.owner(), Source: "column_publish_plan", Reason: "atomic column publish in progress", Refs: refs,
		})
		if err != nil {
			return ColumnPublishPlan{}, err
		}
		l.pendingLease = pendingLease
		if l.preparedLease != nil {
			if err := l.preparedLease.Close(); err != nil {
				_ = pendingLease.Close()
				l.pendingLease = nil
				return ColumnPublishPlan{}, err
			}
			l.preparedLease = nil
		}
	}
	l.state = columnPublishPlanLeaseInstalling
	return cloneColumnPublishPlanForHook(l.plan), nil
}

func (l *columnPublishPlanLease) transferStableResources(ctx backenddb.CommandWALPublishContext) error {
	if l == nil {
		return errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.transferStableResourcesLocked(ctx)
}

func (l *columnPublishPlanLease) transferStableResourcesLocked(ctx backenddb.CommandWALPublishContext) error {
	if l.state != columnPublishPlanLeaseInstalling {
		return errColumnPublishPlanLeaseNotInstalling
	}
	if l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	resources := l.plan.takeStableResources()
	l.transferred = true
	return ctx.RegisterDurableResources(resources)
}

func (l *columnPublishPlanLease) installDurability(ctx backenddb.CommandWALPublishContext) error {
	if l == nil {
		return errors.New("collections: nil prebuilt column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeaseInstalling {
		return errColumnPublishPlanLeaseNotInstalling
	}
	if l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	plan := l.plan
	if plan.durableResourceRequirementsFallback != nil {
		if err := ctx.RegisterDurableLogicalObligationAppendMutation(plan.durableResourceMutation, plan.durableResourceRequirementWork, plan.durableResourceRequirementsFallback); err != nil {
			return fmt.Errorf("collections: register column publish durable append mutation: %w", err)
		}
	} else {
		if err := ctx.RegisterDurableLogicalObligationRequirements(plan.durableResourceRequirements); err != nil {
			return fmt.Errorf("collections: register column publish durable requirements: %w", err)
		}
		if err := ctx.RegisterDurableLogicalObligationMutation(plan.durableResourceMutation); err != nil {
			return fmt.Errorf("collections: register column publish durable mutation: %w", err)
		}
		if err := ctx.RecordDurableLogicalObligationRequirementWork(plan.durableResourceRequirementWork); err != nil {
			return fmt.Errorf("collections: record column publish durable requirement work: %w", err)
		}
	}
	if err := l.transferStableResourcesLocked(ctx); err != nil {
		return fmt.Errorf("collections: register column publish durable resources: %w", err)
	}
	return nil
}

func (l *columnPublishPlanLease) finishCommit() error {
	if l == nil {
		return errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeaseInstalling || !l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	l.state = columnPublishPlanLeaseCommitted
	l.plan.releaseStableResources()
	if l.pendingLease == nil {
		return nil
	}
	err := l.pendingLease.Close()
	l.pendingLease = nil
	return err
}

func (l *columnPublishPlanLease) finishFailure(cause error) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case columnPublishPlanLeasePrepared:
		return l.abandonLocked()
	case columnPublishPlanLeaseInstalling:
		if !l.transferred || (!backenddb.CommitPublicationAccepted(cause) && !errors.Is(cause, backenddb.ErrRecoveryRequired)) {
			return l.abandonLocked()
		}
		return l.quarantineLocked(fmt.Sprintf("ambiguous column publish: %v", cause), columnPublishPlanPreparedRefs(l.plan))
	case columnPublishPlanLeaseQuarantined, columnPublishPlanLeaseAbandoned:
		return nil
	default:
		return errColumnPublishPlanLeaseConsumed
	}
}

func (l *columnPublishPlanLease) Abandon() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == columnPublishPlanLeaseAbandoned {
		return nil
	}
	if l.state != columnPublishPlanLeasePrepared {
		return errColumnPublishPlanLeaseConsumed
	}
	return l.abandonLocked()
}

func (l *columnPublishPlanLease) abandonLocked() error {
	if l.plan.stablePreparedAssets {
		return errors.Join(
			l.quarantineLocked("stable unpublished column assets require reachability cleanup", columnPublishPlanPreparedRefs(l.plan)),
			errColumnPublishPlanLeaseCleanupQuarantined,
		)
	}
	retained, cleanupErr := cleanupUnpublishedColumnPublishAssets(l.collection.db.ColumnAssetRootDir(), l.plan.PreparedAssets)
	if len(retained) != 0 {
		return errors.Join(cleanupErr, l.quarantineLocked("unpublished column publish cleanup was not provably safe", retained), errColumnPublishPlanLeaseCleanupQuarantined)
	}
	l.plan.releaseStableResources()
	l.state = columnPublishPlanLeaseAbandoned
	return errors.Join(cleanupErr, l.closeProtectionLeasesLocked())
}

func (l *columnPublishPlanLease) quarantineLocked(reason string, refs []ColumnAssetRef) error {
	if len(refs) != 0 && l.quarantineLease == nil {
		quarantineLease, err := l.collection.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
			Owner: l.owner(), Source: "column_publish_plan", Reason: reason, Refs: refs,
		})
		if err != nil {
			l.state = columnPublishPlanLeaseQuarantined
			l.plan.releaseStableResources()
			return err
		}
		l.quarantineLease = quarantineLease
	}
	l.state = columnPublishPlanLeaseQuarantined
	l.plan.releaseStableResources()
	return l.closeProtectionLeasesLocked()
}

func (l *columnPublishPlanLease) closeProtectionLeasesLocked() error {
	var errs []error
	if l.preparedLease != nil {
		errs = append(errs, l.preparedLease.Close())
		l.preparedLease = nil
	}
	if l.pendingLease != nil {
		errs = append(errs, l.pendingLease.Close())
		l.pendingLease = nil
	}
	return errors.Join(errs...)
}

func (l *columnPublishPlanLease) stateValue() columnPublishPlanLeaseState {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

func columnPublishPlanPreparedRefs(plan ColumnPublishPlan) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, len(plan.PreparedAssets))
	for i := range plan.PreparedAssets {
		refs[i] = plan.PreparedAssets[i].Ref
	}
	return refs
}

// cleanupUnpublishedColumnPublishAssets reports every ref whose containing
// segment did not end at the proven pre-plan offset after cleanup. Such refs
// must remain protected rather than guessing around a later append.
func cleanupUnpublishedColumnPublishAssets(rootDir string, assets []ColumnPreparedAsset) ([]ColumnAssetRef, error) {
	if len(assets) == 0 {
		return nil, nil
	}
	cleanupErr := cleanupColumnPreparedAssets(rootDir, assets)
	if cleanupErr != nil {
		return columnPublishPlanPreparedRefs(ColumnPublishPlan{PreparedAssets: assets}), cleanupErr
	}
	type target struct {
		truncateTo int64
		refs       []ColumnAssetRef
	}
	targets := make(map[string]*target)
	var inspectErrs []error
	for _, asset := range assets {
		path, err := columnAssetSegmentPath(rootDir, asset.Ref)
		if err != nil {
			inspectErrs = append(inspectErrs, err)
			continue
		}
		entry := targets[path]
		if entry == nil {
			entry = &target{truncateTo: asset.Ref.Offset}
			targets[path] = entry
		}
		if asset.Ref.Offset < entry.truncateTo {
			entry.truncateTo = asset.Ref.Offset
		}
		entry.refs = append(entry.refs, asset.Ref)
	}
	var retained []ColumnAssetRef
	for path, target := range targets {
		info, err := os.Stat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil:
			inspectErrs = append(inspectErrs, err)
			retained = append(retained, target.refs...)
		case info.Size() != target.truncateTo:
			retained = append(retained, target.refs...)
		}
	}
	return retained, errors.Join(inspectErrs...)
}
