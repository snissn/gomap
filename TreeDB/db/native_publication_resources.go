package db

import "github.com/snissn/gomap/TreeDB/internal/rootpublication"

// NativeResourceSelector is an original captured semantic owner's immutable
// inventory. Selection must reject resources outside that inventory; it must
// never consult current collection metadata or the backend visible runtime.
type NativeResourceSelector interface {
	CloneCapturedNativeResource(rootpublication.StableResourceSelector) (*rootpublication.StableResourceSet, error)
}

// NativePublicationResources belongs to the original admitted native plan.
// Its containing allocation and bounded builder allowance must be admitted
// before Initialize. Original Work owns cleanup, including a failed builder.
// Callers serialize construction and never retain a context beyond finalization.
type NativePublicationResources struct {
	builder                  *rootpublication.StableResourceSetBuilder
	resources                *rootpublication.StableResourceSet
	selector                 NativeResourceSelector
	requirements             rootpublication.StableLogicalObligationRequirements
	mutation, appendMutation rootpublication.StableLogicalObligationMutation
	work                     rootpublication.StableResourceClosureWork
	fallback                 func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)
	active                   bool
	failure                  error
}

func (r *NativePublicationResources) Initialize(selector NativeResourceSelector) error {
	if r == nil || r.builder != nil || r.resources != nil || r.failure != nil {
		return rootpublication.ErrResourceOwnership
	}
	r.selector = selector
	r.builder = rootpublication.NewStableResourceSetBuilder()
	return nil
}

func (r *NativePublicationResources) context(lsn uint64) (CommandWALPublishContext, error) {
	if r == nil || r.builder == nil || r.resources != nil || r.failure != nil || r.active || lsn == 0 {
		return CommandWALPublishContext{}, rootpublication.ErrResourceOwnership
	}
	r.active = true
	return CommandWALPublishContext{
		AppliedCommandLSN:                   lsn,
		durableResources:                    r.builder,
		durableResourceRequirements:         &r.requirements,
		durableResourceMutation:             &r.mutation,
		durableResourceAppendMutation:       &r.appendMutation,
		durableResourceRequirementWork:      &r.work,
		durableResourceRequirementsFallback: &r.fallback,
		cloneVisibleStableResource:          r.cloneCapturedResource,
	}, nil
}

func (r *NativePublicationResources) cloneCapturedResource(selector rootpublication.StableResourceSelector) (*rootpublication.StableResourceSet, error) {
	if !r.active || r.selector == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	return r.selector.CloneCapturedNativeResource(selector)
}

func (r *NativePublicationResources) finish() error {
	r.active = false
	if r.fallback != nil {
		requirements, work, err := r.fallback()
		if err != nil {
			r.failure = err
			return err
		}
		merged, err := rootpublication.MergeStableLogicalObligationRequirements(r.requirements, requirements)
		if err != nil {
			r.failure = err
			return err
		}
		r.requirements = merged
		r.work.Add(work)
		r.fallback = nil
	}
	resources, err := r.builder.Freeze()
	if err != nil {
		r.failure = err
		return err
	}
	r.builder, r.resources, r.selector = nil, resources, nil
	return nil
}

// CloneResources returns an owned clone for a captured finite build. Native
// state retains the original set; retries and suffix versions keep their own
// original references. Requirements remain immutable and borrowed with r.
func (r *NativePublicationResources) CloneResources() (*rootpublication.StableResourceSet, rootpublication.StableLogicalObligationRequirements, error) {
	if r == nil || r.builder != nil || r.failure != nil || r.resources == nil {
		return nil, rootpublication.StableLogicalObligationRequirements{}, rootpublication.ErrResourceOwnership
	}
	resources, err := rootpublication.CloneStableResourceSetExcludingKinds(r.resources)
	return resources, r.requirements, err
}

// ReleaseNativeResources runs only from original semantic Work on the existing
// cleanup worker. Failed original fields and their charge remain on this SAME
// carrier; neither Close nor error observation retries physical destruction.
func (r *NativePublicationResources) ReleaseNativeResources() error {
	if r == nil {
		return nil
	}
	if r.failure != nil {
		return r.failure
	}
	if r.builder != nil {
		if err := r.builder.AbandonWithError(); err != nil {
			r.failure = err
			return err
		}
		r.builder = nil
	}
	if r.resources != nil {
		if err := r.resources.ReleaseWithError(); err != nil {
			r.failure = err
			return err
		}
		r.resources = nil
	}
	r.selector, r.fallback = nil, nil
	r.requirements = rootpublication.StableLogicalObligationRequirements{}
	r.mutation, r.appendMutation = rootpublication.StableLogicalObligationMutation{}, rootpublication.StableLogicalObligationMutation{}
	return nil
}

// FinalizeNativeUnderGuard borrows the actual assigned command ownership and
// invokes only the original admitted native planner. It never publishes roots.
func (db *DB) FinalizeNativeUnderGuard(guard *CommandWALStagingGuardV1, intent *CommandWALIntent, native *NativePreparedPublication) (NativeFinalizedPublication, error) {
	if native == nil || native.Finalizer == nil || native.Resources == nil {
		return NativeFinalizedPublication{}, ErrNativeSemanticUnsupported
	}
	lease, err := guard.BorrowStableResourceCaptureLeaseV1(db, intent)
	if err != nil {
		return NativeFinalizedPublication{}, err
	}
	defer lease.Release()
	ctx, err := native.Resources.context(intent.AssignedLSN())
	if err != nil {
		return NativeFinalizedPublication{}, err
	}
	defer func() { native.Resources.active = false }()
	result, err := native.Finalizer.FinalizeNativePublication(ctx)
	if err != nil {
		return result, err
	}
	if result.State == nil {
		return result, ErrNativeSemanticUnsupported
	}
	if err := native.Resources.finish(); err != nil {
		return result, err
	}
	return result, nil
}
