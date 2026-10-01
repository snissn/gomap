package nativewire

import (
	"context"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// One fixed durable pending slot is retried by one owned goroutine. There is
// no in-memory delivery queue and no goroutine per operation or per tick.
type fixedPeerSplitRetryV1 struct {
	cancel    context.CancelFunc
	done      chan struct{}
	lastError error // protected by parent.splitRetryMu; never clears durable intent
}

// Start after successful construction; cancel and join before closing the
// collection DB.
func (r *FixedPeerTCPRuntimeV1) startSplitVectorRetryV1() {
	if r == nil || r.vector == nil || r.client == nil || r.client.peerTransport == nil || r.config.Vector == nil || r.config.Credentials == nil {
		return
	}
	vector := r.config.Vector
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil || vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || vector.Identity.SourceFormat != 0 {
		return
	}
	placement, ok := resolved.Placement(vector.Collection)
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	if !ok || placement.Mode != raftplacement.PlacementModeCollectionV1 || len(owners) != 1 || owners[0] == placement.GroupID || r.vector.dataGroup != placement.GroupID {
		return
	}
	r.splitRetryMu.Lock()
	if r.splitRetry != nil || r.draining.Load() || r.closed.Load() {
		r.splitRetryMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &fixedPeerSplitRetryV1{cancel: cancel, done: make(chan struct{})}
	r.splitRetry = worker
	r.splitRetryMu.Unlock()
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			tick, cancelTick := context.WithTimeout(ctx, r.config.RequestTimeout)
			err := r.retrySplitVectorPendingV1(tick)
			cancelTick()
			r.splitRetryMu.Lock()
			worker.lastError = err
			r.splitRetryMu.Unlock()
		}
	}()
}

func (r *FixedPeerTCPRuntimeV1) stopSplitVectorRetryV1() {
	if r == nil {
		return
	}
	r.splitRetryMu.Lock()
	worker := r.splitRetry
	if worker != nil {
		worker.cancel()
	}
	r.splitRetryMu.Unlock()
	// Never hold the lifetime mutex while a request or DB lease drains.
	if worker != nil {
		<-worker.done
	}
}

func (r *FixedPeerTCPRuntimeV1) retrySplitVectorPendingV1(ctx context.Context) error {
	if r == nil || r.vector == nil || r.config.Vector == nil || r.vector.collection == nil || r.draining.Load() || r.closed.Load() {
		return ErrFixedPeerVectorUnavailableV1
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	vector := r.config.Vector
	// Start eligibility already binds this runtime-owned fixed source/target
	// identity. An empty durable slot needs no authority RPC, byte reservation
	// or mutation lock. Local DB identity remains a guard, never future authority.
	source := r.vector.dataGroup
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	if len(owners) != 1 || owners[0] == source {
		return ErrFixedPeerVectorUnavailableV1
	}
	data := r.localDataV1(source)
	if data == nil || data.fsm == nil || !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorUnavailableV1
	}
	identity := commitlog.SplitVectorInsertV1{Collection: vector.Collection.Collection, Index: vector.Manifest.IndexName, Generation: vector.Identity.Generation,
		SourceGroup: string(source), TargetGroup: string(owners[0]), CatalogEpoch: vector.Identity.Index.CatalogEpoch, CatalogDigest: vector.Identity.Index.CatalogDigest}
	pending, err := r.vector.collection.PendingVectorPartitionSplitInsertV1(identity)
	if err != nil {
		return err
	}
	if !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorUnavailableV1
	}
	if pending == nil {
		return nil
	}
	currentSource, target, err := r.splitVectorGroupsV1(ctx)
	if err != nil {
		return err
	}
	if currentSource != source || target != owners[0] {
		return ErrFixedPeerVectorProofStaleV1
	}
	// Hold bounded decoded-slot/JSON/token scratch through retirement. The
	// existing nested network/proposal admission still charges its own copies.
	work, err := r.client.peerTransport.admission.request(ctx, "control-write", 2<<20, peerRequestIngressV1)
	if err != nil {
		return err
	}
	defer work.release()
	// A busy producer owns this durable intent; skip the tick so Close never
	// waits behind foreground mutation work before draining its requests.
	if !r.vector.mutationMu.TryLock() {
		return nil
	}
	defer r.vector.mutationMu.Unlock()
	// A producer may have completed the observed slot before this lock.
	pending, err = r.vector.collection.PendingVectorPartitionSplitInsertV1(identity)
	if err != nil {
		return err
	}
	if !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorUnavailableV1
	}
	if pending == nil {
		return nil
	}
	if err := r.validateSplitVectorInsertV1(work.ctx, *pending, source); err != nil {
		return err
	}
	_, err = r.finishSplitVectorPendingV1(work.ctx, *pending, false, 0)
	return err
}
