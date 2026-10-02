package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

var (
	ErrFixedPeerVectorProofMissingV1 = errors.New("nativewire: fixed-peer vector mutation proof missing")
	ErrFixedPeerVectorProofStaleV1   = errors.New("nativewire: fixed-peer vector mutation proof stale")
	ErrFixedPeerVectorWrongOwnerV1   = errors.New("nativewire: fixed-peer vector mutation wrong owner")
	ErrFixedPeerVectorUnavailableV1  = errors.New("nativewire: fixed-peer vector runtime unavailable")
	ErrFixedPeerVectorDocumentV1     = errors.New("nativewire: fixed-peer vector document does not match routed vector")
	errFixedPeerVectorSnapshotV1     = errors.New("nativewire: fixed-peer fast and pinned vector search are unavailable")
)

// FixedPeerTCPVectorConfigV1 is shared byte-for-byte by every fixed-peer
// process. Per-process listeners are selected from the node-keyed maps so the
// fixed-peer configuration digest cannot silently diverge across daemons.
type FixedPeerTCPVectorConfigV1 struct {
	Collection      raftplacement.CollectionRefV1
	Catalog         raftplacement.CatalogV1
	Manifest        collections.VectorPartitionManifestV1
	Placement       raftplacement.VectorPartitionPlacementRecordV1
	Identity        raftplacement.VectorPartitionLifecycleIdentityV1
	RouterNodeID    raftcluster.NodeID // immutable mode's sole hosted router; every peer shares this configuration
	PublicAddresses map[raftcluster.NodeID]string
	ShardAddresses  map[raftcluster.GroupID]map[raftcluster.NodeID]string
	RequestBase     VectorPartitionCoordinatorRequestV1
	IndexedThrough  uint64
}

// Keep the validated vector configuration independent of caller-owned maps and
// slices, as the fixed-peer configuration did before its bounded clone path.
func cloneFixedPeerVectorConfigV1(input *FixedPeerTCPVectorConfigV1) *FixedPeerTCPVectorConfigV1 {
	if input == nil {
		return nil
	}
	v := *input
	v.Catalog.Features.Required = slices.Clone(v.Catalog.Features.Required)
	v.Catalog.Groups = slices.Clone(v.Catalog.Groups)
	for i := range v.Catalog.Groups {
		v.Catalog.Groups[i].Members = slices.Clone(v.Catalog.Groups[i].Members)
	}
	v.Catalog.Placements = slices.Clone(v.Catalog.Placements)
	for i := range v.Catalog.Placements {
		v.Catalog.Placements[i].TokenPartitions = slices.Clone(v.Catalog.Placements[i].TokenPartitions)
	}
	v.Manifest.DomainPacks = slices.Clone(v.Manifest.DomainPacks)
	v.Manifest.Placements = slices.Clone(v.Manifest.Placements)
	v.Manifest.Memberships = slices.Clone(v.Manifest.Memberships)
	v.Manifest.OverlapMemberships = slices.Clone(v.Manifest.OverlapMemberships)
	v.Manifest.Representatives = slices.Clone(v.Manifest.Representatives)
	v.Manifest.Assets = slices.Clone(v.Manifest.Assets)
	if v.Manifest.PrepareOrigin != nil {
		origin := *v.Manifest.PrepareOrigin
		v.Manifest.PrepareOrigin = &origin
	}
	if v.Manifest.PagedRootV2 != nil {
		paged := *v.Manifest.PagedRootV2
		paged.SourceOwners = slices.Clone(paged.SourceOwners)
		paged.ANNOwners = slices.Clone(paged.ANNOwners)
		v.Manifest.PagedRootV2 = &paged
	}
	v.Placement.Partitions = slices.Clone(v.Placement.Partitions)
	v.PublicAddresses = maps.Clone(v.PublicAddresses)
	v.ShardAddresses = maps.Clone(v.ShardAddresses)
	for group, addresses := range v.ShardAddresses {
		v.ShardAddresses[group] = maps.Clone(addresses)
	}
	v.RequestBase.Query = slices.Clone(v.RequestBase.Query)
	return &v
}

type fixedPeerVectorCoordinatorRouterSourceV1 struct {
	CollectionVectorPartitionCoordinatorRouterSourceV1
}

func (s fixedPeerVectorCoordinatorRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(ctx context.Context, index string, generation uint64) (VectorPartitionCoordinatorRouterV1, error) {
	if s.Collection == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	router, _, err := s.Collection.OpenPreparedVectorPartitionRouterForReplicatedLiveRecoveryWithContextV1(ctx, index, generation)
	if err != nil {
		return nil, err
	}
	if router == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	return router, nil
}

func (s fixedPeerVectorCoordinatorRouterSourceV1) acquireVectorPartitionCoordinatorReplicatedLivePinV1(ctx context.Context, manifest collections.VectorPartitionManifestV1) (*collections.VectorIndexPartitionLiveSearchPinV1, error) {
	if s.Collection == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	if err := s.Collection.EnsureVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
		return nil, err
	}
	return s.Collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
}

type fixedPeerVectorRuntimeV1 struct {
	parent     *FixedPeerTCPRuntimeV1
	manager    *collections.CollectionManager
	collection *collections.Collection
	dataGroup  raftcluster.GroupID
	boundDB    *backenddb.DB
	listener   net.Listener
	server     *Server
	topology   *VectorPartitionProductionTopologyV1
	source     *CollectionVectorPartitionGenerationSourceV1
	backend    *VectorPartitionPublicBackendV1

	servingGuard func() error
	initDone     chan struct{}
	initCancel   context.CancelFunc
	closed       atomic.Bool
	initMu       sync.Mutex
	mutationMu   sync.Mutex
	closeOnce    sync.Once
	closeErr     error
}

type fixedPeerVectorBuilderV1 struct {
	ready map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1
}

func (b fixedPeerVectorBuilderV1) BuildAndStageVectorPartitionGroupV1(_ context.Context, _ raftplacement.VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	ready, ok := b.ready[group]
	if !ok {
		return raftplacement.VectorPartitionLifecycleGroupReadyV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	return ready, nil
}

type fixedPeerVectorBackendV1 struct{ runtime *fixedPeerVectorRuntimeV1 }

func (b *fixedPeerVectorBackendV1) immutableMutationRefusalV1() error {
	if b != nil && b.runtime != nil && b.runtime.parent != nil && b.runtime.parent.servingVectorConfigV1() != nil &&
		b.runtime.parent.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	return nil
}

func (b *fixedPeerVectorBackendV1) backend(ctx context.Context) (*VectorPartitionPublicBackendV1, error) {
	if b == nil || b.runtime == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return b.runtime.ensureBackendV1(ctx)
}

func (b *fixedPeerVectorBackendV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	if b == nil || b.runtime == nil || b.runtime.parent == nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	return b.runtime.parent.searchVectorPartitionStrictV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) SearchVectorPartitionFastV1(context.Context, public.SearchRequestV1, public.FastSearchOptionsV1) (public.SearchResponseV1, public.FastSearchEvidenceV1, error) {
	return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, publicBackendErrorV1(errFixedPeerVectorSnapshotV1)
}

func (b *fixedPeerVectorBackendV1) PinVectorPartitionSearchSnapshotV1(context.Context, public.PinSearchSnapshotOptionsV1) (public.SearchSnapshotBackendV1, public.FastSearchEvidenceV1, error) {
	return nil, public.FastSearchEvidenceV1{}, publicBackendErrorV1(errFixedPeerVectorSnapshotV1)
}

func (b *fixedPeerVectorBackendV1) InsertVectorPartitionV1(ctx context.Context, request public.InsertRequestV1) (public.InsertResponseV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.InsertResponseV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.InsertResponseV1{}, publicBackendErrorV1(err)
	}
	return backend.InsertVectorPartitionV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) RegisterVectorPartitionV1(ctx context.Context, request public.GenerationRegistrationV1) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RegisterVectorPartitionV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) immutableOwnerV1() bool {
	return b != nil && b.runtime != nil && b.runtime.parent != nil && b.runtime.parent.servingVectorConfigV1() != nil &&
		b.runtime.parent.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) &&
		b.runtime.parent.config.NodeID != b.runtime.parent.servingVectorConfigV1().RouterNodeID
}

func (b *fixedPeerVectorBackendV1) GenerationStatusV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if b.immutableOwnerV1() {
		vector := b.runtime.parent.servingVectorConfigV1()
		if id.Index != vector.Identity.Index.IndexName || id.Generation != vector.Identity.Generation {
			return public.GenerationStatusV1{}, &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: ErrFixedPeerVectorProofStaleV1}
		}
		_, record, guard, err := b.runtime.observeImmutableTopologyV1(ctx)
		if err != nil {
			return public.GenerationStatusV1{}, publicBackendErrorV1(err)
		}
		if err := guard(); err != nil {
			return public.GenerationStatusV1{}, publicBackendErrorV1(err)
		}
		return publicStatusV1(record), nil
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.GenerationStatusV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) PrepareVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.PrepareVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) ActivateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.ActivateVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) InvalidateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1, reason string) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.InvalidateVectorPartitionV1(ctx, id, reason)
}

func (b *fixedPeerVectorBackendV1) RetireVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RetireVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) RequestVectorPartitionRebuildV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RequestVectorPartitionRebuildV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) VectorPartitionCleanupEligibilityV1(ctx context.Context, id public.GenerationIDV1) (public.CleanupEligibilityV1, error) {
	if b.immutableOwnerV1() {
		status, err := b.GenerationStatusV1(ctx, id)
		if err != nil {
			return public.CleanupEligibilityV1{}, err
		}
		return public.CleanupEligibilityV1{Eligible: status.State == public.GenerationCleanableV1, Status: status}, nil
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.CleanupEligibilityV1{}, publicBackendErrorV1(err)
	}
	return backend.VectorPartitionCleanupEligibilityV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) OperationsHealthV1(ctx context.Context) (public.OperationsHealthV1, error) {
	if b.immutableOwnerV1() {
		vector := b.runtime.parent.servingVectorConfigV1()
		id := public.GenerationIDV1{Index: vector.Identity.Index.IndexName, Generation: vector.Identity.Generation}
		topology, _, guard, err := b.runtime.observeImmutableTopologyV1(raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceOperationsHealthV1))
		if err != nil {
			if dbErr := b.runtime.requireCurrentImmutableServingDBV1(); dbErr != nil {
				return public.OperationsHealthV1{Generation: id, Reason: "authority_unavailable"}, dbErr
			}
			b.runtime.initMu.Lock()
			cold := b.runtime.topology == nil || b.runtime.source == nil
			b.runtime.initMu.Unlock()
			if cold {
				return public.OperationsHealthV1{Generation: id, Reason: "topology_unavailable"}, nil
			}
			return public.OperationsHealthV1{Generation: id, Reason: "catalog_unavailable"}, err
		}
		if !vectorPartitionTopologyHealthyV1(topology.Status(), fixedPeerVectorOwnerGroupsV1(vector.Placement)) {
			return public.OperationsHealthV1{Generation: id, Reason: "topology_unavailable"}, nil
		}
		if err := guard(); err != nil {
			return public.OperationsHealthV1{Generation: id, Reason: "authority_unavailable"}, err
		}
		return public.OperationsHealthV1{Ready: true, Generation: id, State: public.GenerationActiveV1, Reason: "ready"}, nil
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.OperationsHealthV1{Generation: public.GenerationIDV1{Index: b.runtime.parent.servingVectorConfigV1().Manifest.IndexName, Generation: b.runtime.parent.servingVectorConfigV1().Manifest.Generation}, Reason: "authority_unavailable"}, err
	}
	return backend.OperationsHealthV1(ctx)
}

func (r *fixedPeerVectorRuntimeV1) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.initMu.Lock()
		r.closed.Store(true)
		topology, source, cancel, done := r.topology, r.source, r.initCancel, r.initDone
		if r.parent != nil && r.parent.servingVectorConfigV1() != nil && r.parent.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
			r.collection, r.topology, r.source, r.backend, r.servingGuard = nil, nil, nil, nil, nil
		}
		r.initMu.Unlock()
		if cancel != nil {
			cancel()
		}
		var errs []error
		// Public/shard requests drain before source close, outside initialization,
		// storage and FSM locks. An in-progress constructor cannot install after Close.
		if r.listener != nil {
			errs = append(errs, r.listener.Close())
		}
		if r.server != nil {
			errs = append(errs, r.server.Close())
		}
		errs = append(errs, topology.Close())
		if source != nil {
			errs = append(errs, source.Close())
		}
		if done != nil {
			<-done
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

func (r *fixedPeerVectorRuntimeV1) start() {
	go func() {
		var retryDelay time.Duration
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				var netErr net.Error
				if !errors.Is(err, net.ErrClosed) && errors.As(err, &netErr) && netErr.Temporary() {
					if retryDelay == 0 {
						retryDelay = 5 * time.Millisecond
					} else {
						retryDelay = min(2*retryDelay, time.Second)
					}
					time.Sleep(retryDelay)
					continue
				}
				return
			}
			retryDelay = 0
			go func() { _ = r.server.ServeConn(context.Background(), conn) }()
		}
	}()
}

func openFixedPeerVectorRuntimeV1(parent *FixedPeerTCPRuntimeV1) (*fixedPeerVectorRuntimeV1, error) {
	if parent == nil || parent.servingVectorConfigV1() == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	var group raftcluster.GroupID
	var data *fixedPeerDataV1
	for _, configured := range parent.config.Groups {
		if candidate := parent.data[configured.ID]; candidate != nil {
			if data != nil {
				return nil, errors.New("nativewire: fixed-peer vector runtime requires exactly one local data group")
			}
			group, data = configured.ID, candidate
		}
	}
	if data == nil || data.db == nil {
		return nil, errors.New("nativewire: fixed-peer vector runtime requires one local data store")
	}
	database := data.db
	if parent.preparedVector != nil {
		_, current, err := data.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(context.Background(), raftcluster.AppliedIndexReadBarrier{NodeID: parent.config.NodeID, GroupID: group, MinAppliedIndex: parent.preparedVector.IndexedThrough}, parent.preparedVector.Collection.Collection)
		if err != nil {
			return nil, err
		}
		database = current
	}
	manager := collections.NewCollectionManager(database)
	var collection *collections.Collection
	var prepared collections.VectorPartitionManifestV1
	if parent.servingVectorConfigV1().Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		// Mutable D1 still requires its complete local source before advertising
		// the public listener. An immutable owner may have only hosted assets;
		// its lazy backend must prove scoped state against ACTIVE catalog authority.
		var err error
		collection, err = manager.OpenCollection(parent.servingVectorConfigV1().Collection.Collection)
		if err != nil {
			return nil, fmt.Errorf("nativewire: open fixed-peer vector collection: %w", err)
		}
		if !collections.VectorPartitionLiveDocumentProofSupportedV1(collection.MetaView()) {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, ErrFixedPeerVectorDocumentV1)
		}
		prepared, err = collection.PreparedVectorPartitionManifestWithContextV1(context.Background(), parent.servingVectorConfigV1().Manifest.IndexName, parent.servingVectorConfigV1().Manifest.Generation)
		if err != nil {
			// A committed document may advance the immutable source. Only the
			// exact persisted live carrier can authorize validation-only recovery.
			if _, recoveryErr := collection.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(context.Background(), parent.servingVectorConfigV1().Manifest); recoveryErr != nil {
				return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err, recoveryErr)
			}
			prepared = parent.servingVectorConfigV1().Manifest
		}
		if !vectorPartitionReplicatedLiveManifestMatchesV1(prepared, parent.servingVectorConfigV1().Manifest) {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("prepared vector manifest mismatch"))
		}
	}
	registry, err := fixedPeerVectorRegistryV1()
	if err != nil {
		return nil, err
	}
	if collection != nil {
		// Replicated fixed-peer startup is validation-only: publishing a missing
		// binding here would create local command-WAL coverage outside Raft. The
		// prepared binding must already be durable before this process advertises
		// readiness.
		if _, err := collection.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(context.Background(), prepared); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("validate vector live binding: %w", err))
		}
	}
	listener, err := net.Listen("tcp", parent.servingVectorConfigV1().PublicAddresses[parent.config.NodeID])
	if err != nil {
		return nil, err
	}
	runtime := &fixedPeerVectorRuntimeV1{parent: parent, manager: manager, collection: collection, dataGroup: group, boundDB: database, listener: listener}
	lazy := &fixedPeerVectorBackendV1{runtime: runtime}
	service, err := public.NewServiceV1(lazy)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	ops, err := public.NewOperationsV1(service, fixedPeerVectorOperationsConfigV1(), lazy.OperationsHealthV1)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	runtime.server = NewServer(ServerOptions{VectorPartitionOperations: ops, VectorPartitionNodeConfigSHA256: parent.client.digest, ConnectionIdleTimeout: parent.config.RequestTimeout})
	runtime.server.vectorPartitionDraining = parent.draining.Load
	runtime.server.registry = registry
	return runtime, nil
}

func fixedPeerVectorRegistryV1() (*iwire.Registry, error) {
	schemas := iwire.MustV1Registry().Schemas()
	schemas = slices.DeleteFunc(schemas, func(schema iwire.CommandSchema) bool {
		switch schema.ID {
		case iwire.CommandVectorSearchFast, iwire.CommandVectorPinSearchSnapshot,
			iwire.CommandVectorSearchPinned, iwire.CommandVectorClosePinnedSnapshot:
			return true
		default:
			return false
		}
	})
	return iwire.NewRegistry(schemas...)
}

// One slot owns all lazy topology/backend work, including authority I/O. Close
// cancels its context and joins it outside initMu; waiters own only their context.
func (r *fixedPeerVectorRuntimeV1) beginInitializationV1(ctx context.Context) (context.Context, context.CancelFunc, chan struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		r.initMu.Lock()
		if r.closed.Load() {
			r.initMu.Unlock()
			return nil, nil, nil, ErrFixedPeerVectorUnavailableV1
		}
		if done := r.initDone; done != nil {
			r.initMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, nil, ctx.Err()
			case <-done:
			}
			continue
		}
		workCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		r.initDone, r.initCancel = done, cancel
		r.initMu.Unlock()
		return workCtx, cancel, done, nil
	}
}

func (r *fixedPeerVectorRuntimeV1) finishInitializationV1(done chan struct{}, cancel context.CancelFunc) {
	cancel()
	r.initMu.Lock()
	if r.initDone == done {
		r.initDone, r.initCancel = nil, nil
		close(done)
	}
	r.initMu.Unlock()
}

func (r *fixedPeerVectorRuntimeV1) ensureBackendV1(ctx context.Context) (*VectorPartitionPublicBackendV1, error) {
	if r == nil || r.parent == nil || r.parent.servingVectorConfigV1() == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	if err := r.parent.requirePreparedVectorCatalogV1(); err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if r.parent.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return r.ensureImmutableBackendV1(ctx)
	}
	if r.collection == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.initMu.Lock()
	if r.closed.Load() {
		r.initMu.Unlock()
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	if r.backend != nil {
		backend := r.backend
		r.initMu.Unlock()
		return backend, nil
	}
	r.initMu.Unlock()
	workCtx, cancel, done, err := r.beginInitializationV1(ctx)
	if err != nil {
		return nil, err
	}
	defer r.finishInitializationV1(done, cancel)
	ctx = workCtx
	r.initMu.Lock()
	err = ctx.Err()
	if err == nil && r.closed.Load() {
		err = ErrFixedPeerVectorUnavailableV1
	}
	backend := r.backend
	r.initMu.Unlock()
	if err != nil {
		return nil, err
	}
	if backend != nil {
		return backend, nil
	}

	vector := r.parent.servingVectorConfigV1()
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := resolved.ValidateVectorPartitionPlacementV1(vector.Placement); err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	owners, ready, err := fixedPeerVectorLifecycleSpecV1(vector)
	if err != nil {
		return nil, err
	}
	owner := owners[0]
	topologyCatalog := resolved
	if r.dataGroup == owner {
		catalog := vector.Catalog
		catalog.Groups = slices.Clone(catalog.Groups)
		for i := range catalog.Groups {
			if catalog.Groups[i].ID == owner {
				catalog.Groups[i].LeaderHint = r.parent.config.NodeID
			}
		}
		topologyCatalog, err = raftplacement.Validate(catalog)
		if err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
	}
	lifecycle := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: r.parent.authority, Committer: r.parent.meta}
	record, exists := r.parent.authority.VectorPartitionLifecycleRecordV1(vector.Identity)
	if !exists || record.State != raftplacement.VectorPartitionLifecycleActiveV1 {
		if err := r.parent.ensureVectorLifecycleV1(ctx); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
		record, exists = r.parent.authority.VectorPartitionLifecycleRecordV1(vector.Identity)
		if !exists {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("vector lifecycle record did not replicate locally"))
		}
	}
	if record.State != raftplacement.VectorPartitionLifecycleActiveV1 || record.Identity != vector.Identity ||
		!reflect.DeepEqual(record.RequiredGroups, owners) || len(record.ReadyGroups) != 1 || record.ReadyGroups[0] != ready || record.ReadySetDigest == "" {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("vector lifecycle identity or ready set mismatch"))
	}

	authority, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(r.parent.authority, r.parent)
	if err != nil {
		return nil, err
	}
	endpoints := make(map[raftcluster.GroupID]string, len(owners))
	nodeEndpoints := make(map[raftcluster.GroupID]map[raftcluster.NodeID]string, len(owners))
	for _, groupID := range owners {
		group, ok := resolved.Group(groupID)
		nodeID := group.LeaderHint
		if r.dataGroup == groupID {
			// A local owner serves through its own proved ReadIndex source; the
			// initialization catalog need not contain a historical leader hint.
			nodeID = r.parent.config.NodeID
		}
		if !ok || nodeID == "" || vector.ShardAddresses[groupID][nodeID] == "" {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("owner group %q has no configured serving endpoint", groupID))
		}
		endpoints[groupID] = vector.ShardAddresses[groupID][nodeID]
		nodeEndpoints[groupID] = make(map[raftcluster.NodeID]string, len(vector.ShardAddresses[groupID]))
		for node, endpoint := range vector.ShardAddresses[groupID] {
			if group, ok := resolved.Group(groupID); !ok || !slices.Contains(group.Members, node) {
				continue
			}
			nodeEndpoints[groupID][node] = endpoint
		}
	}

	topologyOptions := VectorPartitionProductionTopologyOptionsV1{
		ConstructionContext: ctx,
		Catalog:             topologyCatalog,
		Placement:           vector.Placement,
		RouterSource:        fixedPeerVectorCoordinatorRouterSourceV1{CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: r.collection}},
		ReplicatedLifecycle: authority,
		Endpoints:           endpoints,
		NodeEndpoints:       nodeEndpoints,
		CoordinatorLimits:   DefaultVectorPartitionCoordinatorLimitsV1(),
		ShardLimits:         DefaultVectorPartitionShardSearchLimitsV1(),
		ShardIdleTimeout:    r.parent.config.RequestTimeout,
	}
	var source *CollectionVectorPartitionGenerationSourceV1
	var shardListener net.Listener
	if r.dataGroup == owner {
		readCoordinator, readErr := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
			GroupID: owner, NodeID: r.parent.config.NodeID, ReadIndexProvider: r.parent.data[owner].provider, AppliedIndexWaiter: r.parent.data[owner].fsm,
		}})
		if readErr != nil {
			return nil, readErr
		}
		source, err = NewCollectionVectorPartitionGenerationSourceForReplicatedLiveLifecycleV1(r.collection, vector.Collection, authority, vector.Manifest, record.ReadySetDigest)
		if err != nil {
			return nil, err
		}
		shardService, serviceErr := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: topologyCatalog, Placement: vector.Placement, LocalNodeID: r.parent.config.NodeID, LocalGroupID: owner,
			ReadCoordinator: readCoordinator, GenerationSource: source, Limits: DefaultVectorPartitionShardSearchLimitsV1(),
		})
		if serviceErr != nil {
			_ = source.Close()
			return nil, serviceErr
		}
		if r.parent.preparedVector != nil {
			shardService.postSearchGuard = r.parent.requirePreparedVectorCurrentDBV1
		}
		shardListener, err = net.Listen("tcp", vector.ShardAddresses[owner][r.parent.config.NodeID])
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		topologyOptions.Shards = []VectorPartitionProductionShardV1{{
			GroupID: owner, Listener: shardListener, Service: shardService, EndpointIdentity: r.parent.client.digest,
		}}
	}
	topology, err := NewVectorPartitionProductionTopologyV1(topologyOptions)
	if err != nil {
		if shardListener != nil {
			_ = shardListener.Close()
		}
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	backend, err = NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: vector.RequestBase, Lifecycle: lifecycle, ReadFence: r.parent,
		Identity: vector.Identity, RequiredGroups: owners, Builder: fixedPeerVectorBuilderV1{ready: map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1{owner: ready}}, MutationEpoch: record.MutationEpoch,
		MutationSubmitter: r.parent,
	})
	if err != nil {
		_ = topology.Close()
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	r.initMu.Lock()
	err = ctx.Err()
	if err == nil && r.closed.Load() {
		err = ErrFixedPeerVectorUnavailableV1
	}
	if err == nil {
		r.source, r.topology, r.backend = source, topology, backend
	}
	r.initMu.Unlock()
	if err != nil {
		err = errors.Join(err, topology.Close())
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		return nil, err
	}
	return backend, nil
}

func fixedPeerVectorLifecycleSpecV1(vector *FixedPeerTCPVectorConfigV1) ([]raftcluster.GroupID, raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	if len(owners) != 1 {
		return nil, raftplacement.VectorPartitionLifecycleGroupReadyV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("fixed-peer routed vector mutation requires exactly one owner group"))
	}
	return owners, raftplacement.VectorPartitionLifecycleGroupReadyV1{
		GroupID: owners[0], AppliedIndex: vector.IndexedThrough,
		AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(owners[0]), vector.Manifest),
	}, nil
}

func (r *FixedPeerTCPRuntimeV1) ensureVectorLifecycleLeaderV1(ctx context.Context) (raftplacement.CatalogMetaStatusV1, error) {
	if r == nil || r.vector == nil || r.meta == nil || r.servingVectorConfigV1() == nil {
		return raftplacement.CatalogMetaStatusV1{}, ErrFixedPeerVectorUnavailableV1
	}
	if err := r.requirePreparedVectorCatalogV1(); err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	status := r.meta.RuntimeStatusV1()
	if status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return raftplacement.CatalogMetaStatusV1{}, raftcluster.ErrNotLeader
	}
	if r.config.VectorInitialization != nil {
		if err := r.validatePreparedVectorAllVotersV1(ctx); err != nil {
			return raftplacement.CatalogMetaStatusV1{}, err
		}
	}
	owners, ready, err := fixedPeerVectorLifecycleSpecV1(r.servingVectorConfigV1())
	if err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	lifecycle := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: r.authority, Committer: r.meta}
	record, err := lifecycle.BeginBuildV1(ctx, r.servingVectorConfigV1().Identity, owners, 0, 1)
	if err == nil && (record.State == raftplacement.VectorPartitionLifecycleBuildingV1 || record.State == raftplacement.VectorPartitionLifecycleStagedV1) {
		record, err = lifecycle.RecordGroupReadyV1(ctx, r.servingVectorConfigV1().Identity, ready)
	}
	if err == nil && record.State == raftplacement.VectorPartitionLifecycleStagedV1 {
		record, err = lifecycle.PrepareV1(ctx, r.servingVectorConfigV1().Identity)
	}
	if err == nil && record.State == raftplacement.VectorPartitionLifecyclePreparedV1 {
		record, err = lifecycle.ActivateV1(ctx, r.servingVectorConfigV1().Identity)
	}
	if err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	if record.State != raftplacement.VectorPartitionLifecycleActiveV1 {
		return raftplacement.CatalogMetaStatusV1{}, raftplacement.ErrVectorPartitionLifecycleState
	}
	catalog, ok := r.authority.Status()
	if !ok {
		return catalog, raftplacement.ErrCatalogMetaUnavailable
	}
	return catalog, nil
}

func (r *FixedPeerTCPRuntimeV1) ensureVectorLifecycleV1(ctx context.Context) error {
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return err
	}
	if r == nil || r.vector == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	leader, err := r.client.leader(ctx, r.config.Catalog)
	if err != nil {
		return err
	}
	var target raftplacement.CatalogMetaStatusV1
	if leader == r.config.NodeID {
		target, err = r.ensureVectorLifecycleLeaderV1(ctx)
	} else {
		var reply fixedPeerReplyV1
		reply, err = r.client.call(ctx, leader, "vector-lifecycle", fixedPeerRequestV1{}, true)
		target = reply.Catalog
	}
	if err != nil {
		return err
	}
	return r.waitForCatalogStatusV1(ctx, target)
}

// waitForCatalogStatusV1 waits only for this node to apply an already
// authenticated catalog status. It never treats a different catalog identity as
// follower lag.
func (r *FixedPeerTCPRuntimeV1) waitForCatalogStatusV1(ctx context.Context, target raftplacement.CatalogMetaStatusV1) error {
	if target.Epoch == 0 || target.Digest == "" || target.AppliedIndex == 0 {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		local, ok := r.authority.Status()
		if ok && local.AppliedIndex >= target.AppliedIndex {
			if local.Epoch != target.Epoch || local.Digest != target.Digest {
				return raftplacement.ErrCatalogMetaUnavailable
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fixedPeerVectorOwnerGroupsV1(placement raftplacement.VectorPartitionPlacementRecordV1) []raftcluster.GroupID {
	seen := make(map[raftcluster.GroupID]bool)
	for _, partition := range placement.Partitions {
		seen[partition.GroupID] = true
	}
	out := make([]raftcluster.GroupID, 0, len(seen))
	for group := range seen {
		out = append(out, group)
	}
	slices.Sort(out)
	return out
}

func fixedPeerVectorOperationsConfigV1() public.OperationsConfigV1 {
	config := public.ConservativeOperationsConfigV1()
	config.Enabled = true
	return config
}

// LinearizableCatalogMetaAppliedIndexV1 makes remote catalog fencing available
// to ingress nodes without pretending they can mint an owner-local no-log
// proof. Fixed-peer strict searches use a fresh request-scoped live pin.
func (r *FixedPeerTCPRuntimeV1) LinearizableCatalogMetaAppliedIndexV1(ctx context.Context) (uint64, error) {
	status, err := r.catalogFence(ctx)
	return status.AppliedIndex, err
}

func validateFixedPeerVectorConfigV1(config FixedPeerTCPConfigV1, localGroups map[raftcluster.GroupID]bool) error {
	vector := config.Vector
	if vector == nil {
		return nil
	}
	if vector.Collection.Database == "" || vector.Collection.Catalog == "" || vector.Collection.Collection == "" ||
		vector.Manifest.State != "ready" || vector.Manifest.Collection != vector.Collection.Collection ||
		vector.Manifest.IndexName == "" || vector.Manifest.Generation == 0 || vector.Manifest.IntegrityDigest == "" ||
		vector.Placement.Collection != vector.Collection || vector.Placement.IndexName != vector.Manifest.IndexName ||
		vector.Placement.PartitionGeneration != vector.Manifest.Generation || vector.Identity.Index.Collection != vector.Collection ||
		vector.Identity.Index.IndexName != vector.Manifest.IndexName || vector.Identity.Index.CatalogEpoch == 0 ||
		vector.Identity.Index.CatalogDigest == "" || vector.Identity.Generation != vector.Manifest.Generation || vector.IndexedThrough == 0 {
		return errors.New("incomplete vector runtime identity")
	}
	if len(vector.PublicAddresses) != len(config.Nodes) {
		return errors.New("vector public address coverage differs from fixed peers")
	}
	for _, node := range config.Nodes {
		address := vector.PublicAddresses[node.ID]
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil || resolved.IP == nil || resolved.IP.IsUnspecified() || resolved.Port == 0 {
			return fmt.Errorf("invalid vector public address for node %q", node.ID)
		}
	}
	owners := make(map[raftcluster.GroupID]bool)
	for _, partition := range vector.Placement.Partitions {
		owners[partition.GroupID] = true
	}
	immutable := vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{})
	standby := fixedPeerImmutableVectorStandbyV1(config) && len(localGroups) == 0
	if len(owners) == 0 || (!immutable && len(owners) != 1) {
		return errors.New("fixed-peer vector runtime requires exactly one owner group")
	}
	localDataGroups := 0
	localOwner := false
	for _, group := range config.Groups {
		if localGroups[group.ID] {
			localDataGroups++
			localOwner = localOwner || owners[group.ID]
		}
	}
	if !standby && localDataGroups != 1 {
		return errors.New("fixed-peer vector runtime requires exactly one local data group")
	}
	// Only a credentialed immutable ANN owner may consume catalog decisions.
	// Source holders, routers and mutable runtimes retain local voting authority.
	consumerOwner := immutable && config.Credentials != nil && localOwner && config.NodeID != vector.RouterNodeID
	if !standby && !localGroups[config.Catalog.ID] && !consumerOwner {
		return errors.New("fixed-peer vector runtime requires local catalog authority")
	}
	for group := range owners {
		var fixed *FixedPeerTCPGroupV1
		for i := range config.Groups {
			if config.Groups[i].ID == group {
				fixed = &config.Groups[i]
				break
			}
		}
		if fixed == nil || len(vector.ShardAddresses[group]) < len(fixed.Peers) {
			return fmt.Errorf("vector shard address coverage is incomplete for group %q", group)
		}
		members := make(map[raftcluster.NodeID]bool, len(fixed.Peers))
		for _, peer := range fixed.Peers {
			members[peer.ID] = true
		}
		for node, address := range vector.ShardAddresses[group] {
			if members[node] {
				continue
			}
			candidate := config
			candidate.NodeID = node
			candidate.RaftListen = nil
			known := false
			assigned := node == vector.RouterNodeID
			for _, peer := range config.Catalog.Peers {
				assigned = assigned || peer.ID == node
			}
			for _, configuredGroup := range config.Groups {
				for _, peer := range configuredGroup.Peers {
					assigned = assigned || peer.ID == node
				}
			}
			for _, configured := range config.Nodes {
				known = known || configured.ID == node
			}
			if !immutable || config.Credentials == nil || !known || assigned || !fixedPeerImmutableVectorStandbyV1(candidate) || !peerPrivateEndpointV1(address) {
				return fmt.Errorf("invalid replacement shard address for node %q", node)
			}
		}
		for _, peer := range fixed.Peers {
			address := vector.ShardAddresses[group][peer.ID]
			resolved, err := net.ResolveTCPAddr("tcp", address)
			if err != nil || resolved.IP == nil || resolved.IP.IsUnspecified() || resolved.Port == 0 {
				return fmt.Errorf("invalid vector shard address for node %q", peer.ID)
			}
		}
		if localGroups[group] && vector.ShardAddresses[group][config.NodeID] == "" {
			return fmt.Errorf("vector shard address is missing for local owner group %q", group)
		}
	}
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return fmt.Errorf("invalid vector catalog: %w", err)
	}
	if immutable {
		if vector.RouterNodeID == "" {
			return errors.New("immutable vector runtime requires a router node")
		}
		placed, ok := resolved.Placement(vector.Collection)
		if !ok || placed.Mode != raftplacement.PlacementModeCollectionV1 {
			return errors.New("immutable vector source requires collection placement")
		}
		if owners[placed.GroupID] {
			return errors.New("immutable vector source group must be separate from owner groups")
		}
		routerGroup := raftcluster.GroupID("")
		for _, group := range vector.Catalog.Groups {
			if slices.Contains(group.Members, vector.RouterNodeID) {
				if routerGroup != "" {
					return errors.New("immutable vector router belongs to multiple data groups")
				}
				routerGroup = group.ID
			}
		}
		if routerGroup == "" || owners[routerGroup] || routerGroup == placed.GroupID {
			return errors.New("immutable vector router must have a separate nonowner data group")
		}
	} else if vector.RouterNodeID != "" {
		return errors.New("mutable vector runtime cannot select an immutable router node")
	}
	if err := resolved.ValidateVectorPartitionPlacementV1(vector.Placement); err != nil {
		return fmt.Errorf("invalid vector placement: %w", err)
	}
	if err := validateFixedPeerCatalogV1(config, vector.Catalog); err != nil {
		return err
	}
	for owner := range owners {
		group, ok := resolved.Group(owner)
		if !ok || group.LeaderHint == "" || !slices.Contains(group.Members, group.LeaderHint) {
			return errors.New("vector owner catalog leader hint must be a member")
		}
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(vector.Identity.Index.CatalogEpoch, vector.Catalog)
	if err != nil {
		return fmt.Errorf("invalid vector catalog record: %w", err)
	}
	if record.Digest != vector.Identity.Index.CatalogDigest {
		return errors.New("vector catalog digest differs from lifecycle identity")
	}
	if !immutable {
		lifecycleOwners, ready, err := fixedPeerVectorLifecycleSpecV1(vector)
		if err != nil {
			return err
		}
		if _, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(vector.Identity, lifecycleOwners, []raftplacement.VectorPartitionLifecycleGroupReadyV1{ready}); err != nil {
			return fmt.Errorf("invalid vector lifecycle identity: %w", err)
		}
	}
	manifest, placement := vector.Manifest, vector.Placement
	source := raftplacement.VectorPartitionLifecycleSourceIdentityV1{
		Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum,
		SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount,
	}
	if vector.Identity.Index.IndexDefinitionDigest != manifest.IndexDefinitionDigest || vector.Identity.Source != source ||
		placement.IndexDefinitionDigest != manifest.IndexDefinitionDigest || placement.SourceGeneration != manifest.SourceGeneration ||
		placement.SourceChecksum != manifest.SourceChecksum || placement.SourceSchemaHash != manifest.SourceSchemaHash ||
		placement.SourceRowCount != manifest.SourceRowCount || placement.PartitionCount != manifest.PartitionCount ||
		len(placement.Partitions) != len(manifest.Placements) {
		return errors.New("vector identity or placement differs from manifest")
	}
	for i, partition := range placement.Partitions {
		if partition.PartitionID != manifest.Placements[i].PartitionID || string(partition.GroupID) != manifest.Placements[i].GroupID {
			return errors.New("vector partition mapping differs from manifest")
		}
	}
	// Only defaults retained by coordinatorRequestV1 belong in startup preflight.
	request, limits := vector.RequestBase, DefaultVectorPartitionCoordinatorLimitsV1()
	if err := validateVectorPartitionPublicRequestIdentityV1(request, limits.MaxIdentityBytes); err != nil {
		return err
	}
	if request.Database != placement.Collection.Database || request.Catalog != placement.Collection.Catalog ||
		request.Collection != placement.Collection.Collection || request.IndexDefinitionDigest != placement.IndexDefinitionDigest ||
		!isVectorPartitionShardSearchDigestV1(request.IndexDefinitionDigest) {
		return errors.New("vector request base identity differs from placement or has invalid digest")
	}
	for _, identity := range []string{request.Database, request.Catalog, request.Collection, placement.IndexName, request.IndexDefinitionDigest} {
		if len(identity) > limits.MaxIdentityBytes {
			return errors.New("vector request base identity exceeds coordinator limit")
		}
	}
	if request.RouterScoreBudget < 1 || request.RouterScoreBudget > min(limits.MaxRouterScoreCalls, collections.MaxVectorPartitionRouterScoreBudgetV3) ||
		request.LocalScoreBudget < 0 || request.LocalScoreBudget > limits.MaxLocalScoreCalls ||
		(request.RouterMode != collections.VectorPartitionRouterModeExactV1 && request.RouterMode != collections.VectorPartitionRouterModeApproxV1) ||
		(request.RouterMode == collections.VectorPartitionRouterModeExactV1 && request.RouterScoreBudget < len(manifest.Representatives)) ||
		request.StatsMode != VectorPartitionShardSearchStatsBasicV1 {
		return errors.New("invalid vector request base router or stats defaults")
	}
	if err := manifest.Validate(collections.DefaultVectorPartitionManifestLimits()); err != nil {
		return fmt.Errorf("invalid vector manifest: %w", err)
	}
	if immutable {
		encoded, err := collections.EncodeVectorPartitionManifestV1(manifest)
		if err != nil {
			return fmt.Errorf("invalid immutable vector manifest: %w", err)
		}
		placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(encoded)) != vector.Identity.Immutable.ManifestDigest ||
			placementDigest != vector.Identity.Immutable.PlacementDigest {
			return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
	}
	return nil
}

// A preauthorized Nodes-only spare retains the exact immutable config but has
// no Raft membership or local vector assets until a later replacement phase.
func fixedPeerImmutableVectorStandbyV1(config FixedPeerTCPConfigV1) bool {
	return config.Credentials != nil && config.Vector != nil && len(config.RaftListen) == 0 &&
		config.Vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{})
}

// searchVectorPartitionStrictV1 keeps mutable search on the single owner whose
// Raft barrier and fresh live pin authorize it. Immutable search is coordinated
// at the public ingress and may dispatch to several independently hosted owners.
func (r *FixedPeerTCPRuntimeV1) searchVectorPartitionStrictV1(ctx context.Context, request public.SearchRequestV1) (response public.SearchResponseV1, resultErr error) {
	defer func() {
		if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
			response = public.SearchResponseV1{}
			resultErr = publicBackendErrorV1(err)
		}
	}()
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(err)
	}
	if r == nil || r.vector == nil || r.servingVectorConfigV1() == nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	if r.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		if len(request.VisibilityToken) != 0 {
			if err := r.requireSplitVectorVisibilityV1(ctx, request); err != nil {
				return public.SearchResponseV1{}, publicBackendErrorV1(err)
			}
		}
		if r.config.NodeID != r.servingVectorConfigV1().RouterNodeID {
			// Owners expose status and the authenticated shard listener, but do not
			// host the router asset required by public strict search.
			return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
		}
		backend, err := r.vector.ensureImmutableBackendV1(ctx)
		if err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		r.vector.initMu.Lock()
		guard := r.vector.servingGuard
		current := r.vector.backend == backend && guard != nil
		r.vector.initMu.Unlock()
		if !current {
			return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorProofStaleV1)
		}
		response, err := backend.SearchVectorPartitionV1(ctx, request)
		if err != nil {
			return public.SearchResponseV1{}, err
		}
		// The ACTIVE grant checked on entry can be invalidated while remote
		// owners search. Fence it again before exposing any merged result.
		if _, err := r.immutableActiveVectorRecordV1(ctx, fixedPeerVectorOwnerGroupsV1(r.servingVectorConfigV1().Placement)); err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		if err := guard(); err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		return response, nil
	}
	owners := fixedPeerVectorOwnerGroupsV1(r.servingVectorConfigV1().Placement)
	if len(owners) != 1 {
		return public.SearchResponseV1{}, publicBackendErrorV1(errors.New("fixed-peer strict search requires exactly one owner group"))
	}
	var group *FixedPeerTCPGroupV1
	for i := range r.config.Groups {
		if r.config.Groups[i].ID == owners[0] {
			group = &r.config.Groups[i]
			break
		}
	}
	if group == nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorWrongOwnerV1)
	}
	leader, err := r.client.leader(ctx, *group)
	if err != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(err)
	}
	if leader == r.config.NodeID {
		if err := r.requireSplitVectorVisibilityV1(ctx, request); err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		backend, err := r.vector.ensureBackendV1(ctx)
		if err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		searchRequest := request
		searchRequest.VisibilityToken = nil // runtime holds the token fence; generic backend cannot consume it
		response, err := backend.SearchVectorPartitionV1(ctx, searchRequest)
		if err != nil {
			return public.SearchResponseV1{}, err
		}
		if err := r.requireSplitVectorVisibilityV1(ctx, request); err != nil {
			return public.SearchResponseV1{}, publicBackendErrorV1(err)
		}
		return response, nil
	}
	reply, err := r.client.call(ctx, leader, "vector-search", fixedPeerRequestV1{VectorSearch: &request}, false)
	if err != nil {
		return public.SearchResponseV1{}, fixedPeerVectorPublicErrorV1(err)
	}
	if reply.VectorSearch == nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	// Retain the ingress response fence: authority may change while the
	// owner's already-fenced response travels back to this process.
	if err := r.requireSplitVectorVisibilityV1(ctx, request); err != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(err)
	}
	return *reply.VectorSearch, nil
}

func fixedPeerVectorPublicErrorV1(err error) error {
	var publicErr *public.ErrorV1
	if errors.As(err, &publicErr) {
		return err
	}
	var remote *fixedPeerRemoteErrorV1
	if errors.As(err, &remote) {
		code := public.ErrorCodeV1(remote.code)
		switch code {
		case public.ErrorInvalidRequestV1, public.ErrorGenerationMismatchV1, public.ErrorUnavailableV1,
			public.ErrorCanceledV1, public.ErrorDeadlineExceededV1, public.ErrorCommitAmbiguousV1, public.ErrorFailedV1:
			return &public.ErrorV1{Code: code, Err: errors.New(remote.message)}
		}
	}
	return publicBackendErrorV1(err)
}

// SubmitVectorPartitionInsertV1 performs exactly one fixed-peer hop to the
// configured owner leader. The receiving owner repeats the complete proof
// validation inside its serialized preflight-to-consensus boundary.
func (r *FixedPeerTCPRuntimeV1) SubmitVectorPartitionInsertV1(ctx context.Context, request VectorPartitionRoutedInsertV1) (public.InsertResponseV1, error) {
	if r == nil || r.vector == nil || r.servingVectorConfigV1() == nil || r.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || request.OwnerGroup == "" {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	routeGroup := request.OwnerGroup
	if r.authority != nil {
		source, err := r.authority.RouteDocumentToken(ctx, request.CatalogProof, r.servingVectorConfigV1().Collection, raftplacement.DocumentIDTokenV1(request.Request.ID))
		if err != nil {
			return public.InsertResponseV1{}, err
		}
		if source.GroupID() != request.OwnerGroup {
			canonical, target, err := r.splitVectorGroupsV1(ctx)
			if err != nil || canonical != source.GroupID() || target != request.OwnerGroup {
				return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
			}
			request.SourceGroup = canonical
			routeGroup = canonical
		}
	}
	var group *FixedPeerTCPGroupV1
	for i := range r.config.Groups {
		if r.config.Groups[i].ID == routeGroup {
			group = &r.config.Groups[i]
			break
		}
	}
	if group == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	leader, err := r.client.leader(ctx, *group)
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	request.Forwarded = leader != r.config.NodeID
	reply, err := r.client.call(ctx, leader, "vector-forward", fixedPeerRequestV1{VectorInsert: &request}, true)
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	if reply.VectorInsert == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	return *reply.VectorInsert, nil
}

func (r *FixedPeerTCPRuntimeV1) applyVectorInsertV1(ctx context.Context, request VectorPartitionRoutedInsertV1) (public.InsertResponseV1, error) {
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return public.InsertResponseV1{}, err
	}
	if r == nil || r.vector == nil || r.servingVectorConfigV1() == nil || r.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	if request.SourceGroup != "" {
		return r.applySplitVectorSourceInsertV1(ctx, request)
	}
	r.vector.mutationMu.Lock()
	defer r.vector.mutationMu.Unlock()
	if err := r.validateVectorInsertOwnerV1(ctx, request); err != nil {
		return public.InsertResponseV1{}, err
	}
	data := r.data[request.OwnerGroup]
	if data == nil || data.fsm == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	catalogVersion, ok, err := data.fsm.CurrentCatalogVersion(ctx)
	if err != nil || !ok {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	originalGuard, originalDigest, known, err := data.fsm.AppliedIdempotencyGuardV1(ctx, request.Request.IdempotencyKey)
	if err != nil {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if known {
		catalogVersion = originalGuard
	}
	format, err := fixedPeerVectorDocumentFormatV1(r.vector.collection)
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	entry, key, err := fixedPeerVectorInsertEntryV1(r.servingVectorConfigV1().Collection.Collection, format, catalogVersion, request)
	if err != nil {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	if known {
		// The fixed-peer FSM and submitter decode with the default single-group
		// scope; route names are metadata, not their digest scope.
		digest := raftentry.CommandDigestV1ForBytes(entry, raftentry.DecodeOptions{})
		if digest != originalDigest {
			return public.InsertResponseV1{}, &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: errors.New("idempotency key conflicts with the original vector insert")}
		}
	}
	base, ok := r.localRegistry.Lookup(request.OwnerGroup)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	submitter, ok := base.(raftcluster.CommandSubmitterWithPreCommitV1)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	resolved, err := raftplacement.Validate(r.servingVectorConfigV1().Catalog)
	if err != nil {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	group, ok := resolved.Group(request.OwnerGroup)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	members := make([]string, len(group.Members))
	for i := range group.Members {
		members[i] = string(group.Members[i])
	}
	metadata := raftentry.RequestMetadataV1{
		RequestID:                 binary.BigEndian.Uint64(key[:8]),
		AckPolicy:                 iwire.AckRaftCommitted,
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      r.servingVectorConfigV1().Collection.Database,
		ClusterRouteCatalog:       r.servingVectorConfigV1().Collection.Catalog,
		ClusterRouteCollection:    r.servingVectorConfigV1().Collection.Collection,
		ClusterRouteShape:         "vector_partition_exact_id",
		ClusterRouteGroupID:       string(request.OwnerGroup),
		ClusterRouteMembers:       members,
		ClusterRouteLeaderHint:    string(group.LeaderHint),
		ClusterRoutePlacementMode: "vector_partition",
		ClusterRouteKey:           "_id",
		ClusterRoutePartitionID:   fmt.Sprint(request.PartitionID),
		CatalogMetaEpoch:          request.CatalogProof.Epoch,
		CatalogMetaDigest:         request.CatalogProof.Digest,
	}
	if !request.Request.Deadline.IsZero() {
		metadata.DeadlineUnixNanos = request.Request.Deadline.UnixNano()
	}
	result, err := submitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, func(commitCtx context.Context) error {
		return r.validateVectorInsertOwnerV1(commitCtx, request)
	})
	if err = fixedPeerVectorSubmitErrorV1(result, err); err != nil {
		return public.InsertResponseV1{}, err
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable || !result.CommittedApplied ||
		!result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != request.OwnerGroup ||
		result.Evidence.NodeID != r.config.NodeID || result.Evidence.LeaderID != r.config.NodeID ||
		result.Evidence.Term != result.CommittedEntry.Term || result.Evidence.Index != result.CommittedEntry.Index ||
		result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 ||
		(result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied) {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(raftcluster.ErrCommitNotProven)
	}
	ownerStatus, err := data.provider.RuntimeStatusV1(ctx)
	if err != nil || ownerStatus.State != "Leader" || ownerStatus.LeaderID != r.config.NodeID || ownerStatus.RaftAppliedIndex < result.CommittedEntry.Index {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err, errors.New("owner apply proof is incomplete")))
	}
	live, err := r.vector.collection.ProveVectorPartitionLiveDocumentV1(ctx, r.servingVectorConfigV1().Manifest, request.Request.ID, request.Request.Document)
	if err != nil {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	if live.Generation != request.Identity.Generation {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("live visibility generation does not match request identity")))
	}
	visibleID := string(request.Request.ID)
	forwards := uint64(0)
	if request.Forwarded {
		forwards = 1
	}
	return public.InsertResponseV1{
		Generation: request.Request.Generation, PartitionID: request.PartitionID, OwnerGroup: string(request.OwnerGroup),
		CommitTerm: result.CommittedEntry.Term, CommitIndex: result.CommittedEntry.Index, AppliedIndex: ownerStatus.RaftAppliedIndex,
		ProductionConsensus: true, LiveRevision: live.Revision, VisibilityGeneration: request.Request.Generation, VisibleID: visibleID,
		Counters: public.MutationCountersV1{Routes: 1, Forwards: forwards, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1},
	}, nil
}

func fixedPeerVectorPostCommitAmbiguousV1(err error) error {
	if err == nil {
		err = errors.New("post-commit mutation proof is incomplete")
	}
	return errors.Join(raftcluster.ErrCommitAmbiguous, err)
}

func fixedPeerVectorSubmitErrorV1(result raftcluster.SubmitResultV1, err error) error {
	if err != nil && (result.CommittedEntry.Term != 0 || result.CommittedEntry.Index != 0 || result.Evidence.ProvesProductionConsensus()) {
		return fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	if !errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		if code, ok := raftfsm.ErrorCodeOf(err); ok && code == raftentry.ErrorRejectedConflictV1 {
			return &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: err}
		}
	}
	return err
}

func (r *FixedPeerTCPRuntimeV1) validateVectorInsertOwnerV1(ctx context.Context, request VectorPartitionRoutedInsertV1) error {
	if request.CatalogProof.Epoch == 0 || request.CatalogProof.Digest == "" || request.ReadySetDigest == "" || request.RouterModelDigest == "" {
		return ErrFixedPeerVectorProofMissingV1
	}
	if !utf8.Valid(request.Request.ID) {
		return ErrFixedPeerVectorDocumentV1
	}
	if err := public.ValidateInsertRequestV1(ctx, request.Request); err != nil {
		return err
	}
	vector := r.servingVectorConfigV1()
	if vector == nil || r.vector == nil || vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) ||
		r.vector.collection == nil || request.Identity != vector.Identity ||
		request.Request.Generation.Index != vector.Identity.Index.IndexName || request.Request.Generation.Generation != vector.Identity.Generation ||
		request.CatalogProof.Epoch != vector.Identity.Index.CatalogEpoch || request.CatalogProof.Digest != vector.Identity.Index.CatalogDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if r.vector.dataGroup != request.OwnerGroup || r.data[request.OwnerGroup] == nil {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	status, err := r.data[request.OwnerGroup].provider.RuntimeStatusV1(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, raftcluster.ErrNotLeader)
	}
	catalog, err := r.catalogFence(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if catalog.Epoch != request.CatalogProof.Epoch || catalog.Digest != request.CatalogProof.Digest {
		return ErrFixedPeerVectorProofStaleV1
	}
	source, err := r.authority.RouteDocumentToken(ctx, request.CatalogProof, vector.Collection, raftplacement.DocumentIDTokenV1(request.Request.ID))
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if source.GroupID() != request.OwnerGroup {
		return ErrFixedPeerVectorUnavailableV1
	}
	record, exists := r.authority.VectorPartitionLifecycleRecordV1(request.Identity)
	if !exists || record.ReadySetDigest != request.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := record.CanSearch(raftplacement.VectorPartitionLifecycleSearchProofV1{Identity: request.Identity, ReadySetDigest: request.ReadySetDigest}); err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	format, err := fixedPeerVectorDocumentFormatV1(r.vector.collection)
	if err != nil {
		return err
	}
	decoded, err := r.vector.collection.ValidatedVectorFromDocumentV1(vector.Manifest.IndexName, format, request.Request.Document)
	if err != nil || !sameFloat32BitsV1(decoded, request.Request.Vector) {
		return errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	coordinator := backend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, request.Request.Generation.Index, request.Request.Generation.Generation)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	defer lease.Close()
	routerStatus := lease.session.router.Status()
	readySetDigest, err := coordinator.validateReplicatedLifecycle(ctx, routerStatus)
	if err != nil || readySetDigest != request.ReadySetDigest || routerStatus.ModelDigest != request.RouterModelDigest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	selection, err := lease.session.router.SearchWithContextV1(ctx, decoded, collections.VectorPartitionRouterSearchOptionsV3{
		Mode: collections.VectorPartitionRouterModeExactV1, ScoreBudget: int(routerStatus.Representatives), PartitionProbes: 1,
	})
	if err != nil || len(selection.Partitions) != 1 {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	partitionID, owner, err := vectorPartitionMutationOwnerV1(routerStatus.Manifest, coordinator.placement, selection.Partitions[0].PartitionID)
	if err != nil || partitionID != request.PartitionID || owner != request.OwnerGroup {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	return nil
}

func fixedPeerVectorInsertEntryV1(collection string, format collections.DocumentFormat, catalogVersion uint64, request VectorPartitionRoutedInsertV1) ([]byte, [sha256.Size]byte, error) {
	key := sha256.Sum256(request.Request.IdempotencyKey)
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: slices.Clone(request.Request.IdempotencyKey)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		collectionNameRef(collection),
		documentFormatSection(format),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, request.Request.ID)},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, request.Request.Document)},
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, key, err
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	return entry, key, err
}

func fixedPeerVectorDocumentFormatV1(collection *collections.Collection) (collections.DocumentFormat, error) {
	format := collection.MetaView().Options.DocumentFormat
	if format == collections.DocumentFormatDefault {
		format = collections.DocumentFormatJSON
	}
	if format != collections.DocumentFormatJSON {
		return format, ErrFixedPeerVectorDocumentV1
	}
	return format, nil
}

func sameFloat32BitsV1(left, right []float32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if math.Float32bits(left[i]) != math.Float32bits(right[i]) {
			return false
		}
	}
	return true
}
