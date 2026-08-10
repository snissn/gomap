package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const vectorPartitionServingSnapshotRefreshTimeoutV1 = time.Second

// VectorPartitionServingSnapshotPublisherOptionsV1 binds the immutable local
// assets to the replicated serving identity. GenerationSources contains only
// groups owned by this process; every source is pinned and every local
// partition is opened before publication.
type VectorPartitionServingSnapshotPublisherOptionsV1 struct {
	Coordinator                *VectorPartitionCoordinatorV1
	Authority                  *LinearizableCatalogVectorPartitionLifecycleAuthorityV1
	GenerationSources          map[raftcluster.GroupID]VectorPartitionGenerationSourceV1
	TopologyDigest             string
	AuthorizationOverlayDigest string
	IndexedThrough             uint64
}

// VectorPartitionServingSnapshotIdentityV1 is the immutable identity exposed
// by one published snapshot. Proof renewal may extend admission for this exact
// identity, but it never mutates the identity or its assets.
type VectorPartitionServingSnapshotIdentityV1 struct {
	SnapshotDigest             string                                               `json:"snapshot_digest"`
	Lifecycle                  raftplacement.VectorPartitionLifecycleIdentityV1     `json:"lifecycle"`
	CatalogEpoch               uint64                                               `json:"catalog_epoch"`
	CatalogDigest              string                                               `json:"catalog_digest"`
	CatalogAppliedIndex        uint64                                               `json:"catalog_applied_index"`
	CatalogCommitIndex         uint64                                               `json:"catalog_commit_index"`
	CatalogRaftAppliedIndex    uint64                                               `json:"catalog_raft_applied_index"`
	ProofNodeID                raftcluster.NodeID                                   `json:"proof_node_id"`
	ProofGroupID               raftcluster.GroupID                                  `json:"proof_group_id"`
	ProofLeaderTerm            uint64                                               `json:"proof_leader_term"`
	LifecycleRevision          uint64                                               `json:"lifecycle_revision"`
	ReadySetDigest             string                                               `json:"ready_set_digest"`
	TopologyDigest             string                                               `json:"topology_digest"`
	ManifestIntegrityDigest    string                                               `json:"manifest_integrity_digest"`
	RouterModelDigest          string                                               `json:"router_model_digest"`
	AuthorizationOverlayDigest string                                               `json:"authorization_overlay_digest"`
	IndexedThrough             uint64                                               `json:"indexed_through"`
	PublishedAtUnixNano        int64                                                `json:"published_at_unix_nano"`
	ReadyGroups                []raftplacement.VectorPartitionLifecycleGroupReadyV1 `json:"ready_groups"`
	LocalGroups                []raftcluster.GroupID                                `json:"local_groups"`
}

type VectorPartitionServingSnapshotPublisherStatsV1 struct {
	Builds, BuildFailures                      uint64
	Publications, Replacements, Invalidations  uint64
	ProofRefreshes, ProofRefreshFailures       uint64
	Acquisitions, AcquisitionRejections        uint64
	Releases, SnapshotCloses                   uint64
	RouterPins, GenerationPins, PartitionOpens uint64
	CurrentPins, CurrentSnapshots              uint64
}

type vectorPartitionServingSnapshotV1 struct {
	identity    VectorPartitionServingSnapshotIdentityV1
	publishedAt time.Time
	proof       vectorPartitionServingAuthorityProofV1
	router      *vectorPartitionCoordinatorRouterLeaseV1
	generations map[raftcluster.GroupID]VectorPartitionPinnedGenerationV1
	partitions  map[raftcluster.GroupID]map[uint32]*VectorPartitionPartitionSearchLeaseV1
	refs        uint64
	retired     bool
	closeOnce   sync.Once
	closeErr    error
}

type VectorPartitionServingSnapshotPublisherV1 struct {
	opts VectorPartitionServingSnapshotPublisherOptionsV1

	mu      sync.Mutex
	current *vectorPartitionServingSnapshotV1
	closed  bool
	stats   VectorPartitionServingSnapshotPublisherStatsV1

	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
	closeErr  error
}

type VectorPartitionServingSnapshotLeaseV1 struct {
	publisher *VectorPartitionServingSnapshotPublisherV1
	snapshot  *vectorPartitionServingSnapshotV1
	closeOnce sync.Once
	closeErr  error
}

func NewVectorPartitionServingSnapshotPublisherV1(opts VectorPartitionServingSnapshotPublisherOptionsV1) (*VectorPartitionServingSnapshotPublisherV1, error) {
	if opts.Coordinator == nil || opts.Authority == nil || len(opts.GenerationSources) == 0 || opts.IndexedThrough == 0 ||
		!isVectorPartitionShardSearchDigestV1(opts.TopologyDigest) || !isVectorPartitionShardSearchDigestV1(opts.AuthorizationOverlayDigest) {
		return nil, fmt.Errorf("%w: incomplete serving snapshot publisher", ErrVectorPartitionShardSearchAssetsUnavailable)
	}
	groups := make(map[raftcluster.GroupID]bool, len(opts.Coordinator.placement.Partitions))
	for _, partition := range opts.Coordinator.placement.Partitions {
		groups[partition.GroupID] = true
	}
	sources := make(map[raftcluster.GroupID]VectorPartitionGenerationSourceV1, len(opts.GenerationSources))
	for group, source := range opts.GenerationSources {
		if group == "" || source == nil || !groups[group] {
			return nil, fmt.Errorf("%w: serving snapshot source group %q", ErrVectorPartitionShardSearchAssetsUnavailable, group)
		}
		sources[group] = source
	}
	opts.GenerationSources = sources
	ctx, cancel := context.WithCancel(context.Background())
	return &VectorPartitionServingSnapshotPublisherV1{
		opts: opts, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1),
	}, nil
}

// PublishV1 prepares every immutable asset off path, recaptures the exact
// authority after physical opens, then swaps one pointer under the publisher
// lock. The previous snapshot drains through existing leases.
func (p *VectorPartitionServingSnapshotPublisherV1) PublishV1(ctx context.Context) error {
	if p == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("%w: serving snapshot publisher closed", ErrVectorPartitionShardSearchAssetsUnavailable)
	}
	p.stats.Builds++
	p.mu.Unlock()

	next, counts, err := p.buildSnapshotV1(ctx)
	p.mu.Lock()
	p.stats.RouterPins += counts.routerPins
	p.stats.GenerationPins += counts.generationPins
	p.stats.PartitionOpens += counts.partitionOpens
	if err != nil {
		p.stats.BuildFailures++
		p.mu.Unlock()
		return err
	}
	if p.closed {
		p.mu.Unlock()
		return errors.Join(fmt.Errorf("%w: serving snapshot publisher closed", ErrVectorPartitionShardSearchAssetsUnavailable), next.close())
	}
	previous := p.current
	p.current = next
	p.stats.Publications++
	p.stats.CurrentSnapshots++
	if previous != nil {
		p.stats.Replacements++
		p.stats.CurrentSnapshots--
		previous.retired = true
	}
	closePrevious := previous != nil && previous.refs == 0
	p.mu.Unlock()
	if closePrevious {
		p.recordSnapshotCloseV1(previous.close())
	}
	p.startRefreshLoopV1()
	p.wakeRefreshV1()
	return nil
}

type vectorPartitionServingSnapshotBuildCountsV1 struct {
	routerPins, generationPins, partitionOpens uint64
}

func (p *VectorPartitionServingSnapshotPublisherV1) buildSnapshotV1(ctx context.Context) (_ *vectorPartitionServingSnapshotV1, counts vectorPartitionServingSnapshotBuildCountsV1, err error) {
	placement := p.opts.Coordinator.placement
	before, err := p.opts.Authority.captureVectorPartitionServingAuthorityV1(
		raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceUnknownV1),
		placement.Collection, placement.IndexName, placement.PartitionGeneration, placement.IndexDefinitionDigest,
		placement.SourceGeneration, placement.SourceChecksum, placement.SourceSchemaHash, placement.SourceRowCount,
	)
	if err != nil {
		return nil, counts, err
	}
	snapshot := &vectorPartitionServingSnapshotV1{
		generations: make(map[raftcluster.GroupID]VectorPartitionPinnedGenerationV1, len(p.opts.GenerationSources)),
		partitions:  make(map[raftcluster.GroupID]map[uint32]*VectorPartitionPartitionSearchLeaseV1, len(p.opts.GenerationSources)),
	}
	fail := func(err error) (*vectorPartitionServingSnapshotV1, vectorPartitionServingSnapshotBuildCountsV1, error) {
		return nil, counts, errors.Join(err, snapshot.close())
	}

	snapshot.router, err = p.opts.Coordinator.acquireRouterSessionV1(ctx, placement.IndexName, placement.PartitionGeneration)
	if err != nil {
		return fail(err)
	}
	counts.routerPins++
	routerStatus := snapshot.router.session.router.Status()
	if err := p.opts.Coordinator.validateRouterStatus(routerStatus); err != nil {
		p.opts.Coordinator.retireRouterSessionV1(snapshot.router)
		return fail(err)
	}
	if !isVectorPartitionShardSearchDigestV1(routerStatus.Manifest.IntegrityDigest) ||
		routerStatus.Manifest.ReadySetDigest != before.authority.Record.ReadySetDigest {
		return fail(ErrVectorPartitionCoordinatorGenerationMismatch)
	}
	if err := p.opts.Coordinator.recordRouterSessionIdentityV1(snapshot.router, routerStatus, before.authority.Record.ReadySetDigest); err != nil {
		p.opts.Coordinator.retireRouterSessionV1(snapshot.router)
		return fail(err)
	}

	localGroups := make([]raftcluster.GroupID, 0, len(p.opts.GenerationSources))
	for group := range p.opts.GenerationSources {
		localGroups = append(localGroups, group)
	}
	sort.Slice(localGroups, func(i, j int) bool { return localGroups[i] < localGroups[j] })
	for _, group := range localGroups {
		generation, pinErr := p.opts.GenerationSources[group].PinVectorPartitionGenerationV1(ctx, placement.IndexName, placement.PartitionGeneration)
		if pinErr != nil {
			return fail(pinErr)
		}
		counts.generationPins++
		snapshot.generations[group] = generation
		manifest := generation.Manifest()
		if err := validateVectorPartitionServingSnapshotManifestV1(placement, group, manifest, routerStatus.Manifest, before.authority); err != nil {
			return fail(err)
		}
		snapshot.partitions[group] = make(map[uint32]*VectorPartitionPartitionSearchLeaseV1)
		for _, partition := range placement.Partitions {
			if partition.GroupID != group {
				continue
			}
			lease, openErr := generation.OpenPartition(ctx, partition.PartitionID)
			if openErr != nil {
				return fail(openErr)
			}
			if lease == nil || lease.Searcher == nil {
				return fail(ErrVectorPartitionShardSearchAssetsUnavailable)
			}
			counts.partitionOpens++
			snapshot.partitions[group][partition.PartitionID] = lease
		}
		if len(snapshot.partitions[group]) == 0 {
			return fail(fmt.Errorf("%w: group %q owns no partitions", ErrVectorPartitionShardSearchAssetsUnavailable, group))
		}
	}

	after, err := p.opts.Authority.captureVectorPartitionServingAuthorityV1(
		raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceUnknownV1),
		placement.Collection, placement.IndexName, placement.PartitionGeneration, placement.IndexDefinitionDigest,
		placement.SourceGeneration, placement.SourceChecksum, placement.SourceSchemaHash, placement.SourceRowCount,
	)
	if err != nil {
		return fail(err)
	}
	if !sameVectorPartitionServingAuthorityProofV1(before, after) {
		return fail(ErrVectorPartitionShardSearchGenerationMismatch)
	}
	publishedAt := time.Now()
	identity := VectorPartitionServingSnapshotIdentityV1{
		Lifecycle:    after.authority.Identity,
		CatalogEpoch: after.authority.Catalog.Epoch, CatalogDigest: after.authority.Catalog.Digest,
		CatalogAppliedIndex: after.read.CatalogAppliedIndex, CatalogCommitIndex: after.read.CommitIndex,
		CatalogRaftAppliedIndex: after.read.RaftAppliedIndex, ProofNodeID: after.read.NodeID,
		ProofGroupID: after.read.GroupID, ProofLeaderTerm: after.read.LeaderTerm,
		LifecycleRevision: after.authority.Record.Revision, ReadySetDigest: after.authority.Record.ReadySetDigest,
		TopologyDigest: p.opts.TopologyDigest, ManifestIntegrityDigest: routerStatus.Manifest.IntegrityDigest,
		RouterModelDigest: routerStatus.ModelDigest, AuthorizationOverlayDigest: p.opts.AuthorizationOverlayDigest,
		IndexedThrough: p.opts.IndexedThrough, PublishedAtUnixNano: publishedAt.UnixNano(),
		ReadyGroups: slices.Clone(after.authority.Record.ReadyGroups), LocalGroups: slices.Clone(localGroups),
	}
	identity.SnapshotDigest, err = vectorPartitionServingSnapshotDigestV1(identity)
	if err != nil {
		return fail(err)
	}
	snapshot.identity, snapshot.publishedAt, snapshot.proof = identity, publishedAt, after
	if err := p.opts.Authority.validateVectorPartitionServingAuthorityV1(after); err != nil {
		return fail(err)
	}
	return snapshot, counts, nil
}

func validateVectorPartitionServingSnapshotManifestV1(
	placement raftplacement.VectorPartitionPlacementRecordV1,
	localGroup raftcluster.GroupID,
	manifest VectorPartitionPinnedManifestV1,
	routerManifest collections.VectorPartitionManifestV1,
	authority raftplacement.VectorPartitionServingAuthoritySnapshotV1,
) error {
	if manifest.State != "ready" || manifest.Collection != placement.Collection.Collection || manifest.IndexName != placement.IndexName ||
		manifest.IndexDefinitionDigest != placement.IndexDefinitionDigest || manifest.IntegrityDigest != routerManifest.IntegrityDigest ||
		!isVectorPartitionShardSearchDigestV1(manifest.IntegrityDigest) || manifest.SourceGeneration != placement.SourceGeneration ||
		manifest.SourceChecksum != placement.SourceChecksum || manifest.SourceSchemaHash != placement.SourceSchemaHash ||
		manifest.SourceRowCount != placement.SourceRowCount || manifest.Generation != placement.PartitionGeneration ||
		manifest.RouterGeneration != placement.PartitionGeneration || manifest.ReadySetDigest != authority.Record.ReadySetDigest ||
		manifest.PartitionCount != placement.PartitionCount || len(manifest.Placements) != len(placement.Partitions) ||
		!slices.Contains(authority.Record.RequiredGroups, localGroup) {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	ready := false
	for _, group := range authority.Record.ReadyGroups {
		if group.GroupID == localGroup && group.AppliedIndex != 0 && isVectorPartitionShardSearchDigestV1(group.AssetSetDigest) {
			ready = true
			break
		}
	}
	if !ready {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	for i := range placement.Partitions {
		if manifest.Placements[i].PartitionID != placement.Partitions[i].PartitionID || manifest.Placements[i].GroupID != string(placement.Partitions[i].GroupID) {
			return ErrVectorPartitionCoordinatorRouteMismatch
		}
	}
	return nil
}

func sameVectorPartitionServingAuthorityProofV1(a, b vectorPartitionServingAuthorityProofV1) bool {
	return a.read.NodeID == b.read.NodeID && a.read.GroupID == b.read.GroupID &&
		a.read.LeaderTerm == b.read.LeaderTerm && a.read.CatalogAppliedIndex == b.read.CatalogAppliedIndex &&
		reflect.DeepEqual(a.authority, b.authority)
}

func vectorPartitionServingSnapshotDigestV1(identity VectorPartitionServingSnapshotIdentityV1) (string, error) {
	identity.SnapshotDigest = ""
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (p *VectorPartitionServingSnapshotPublisherV1) AcquireV1() (*VectorPartitionServingSnapshotLeaseV1, error) {
	if p == nil {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.current == nil {
		p.stats.AcquisitionRejections++
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	if err := p.opts.Authority.validateVectorPartitionServingAuthorityV1(p.current.proof); err != nil {
		p.stats.AcquisitionRejections++
		return nil, err
	}
	p.current.refs++
	p.stats.Acquisitions++
	p.stats.CurrentPins++
	return &VectorPartitionServingSnapshotLeaseV1{publisher: p, snapshot: p.current}, nil
}

func (l *VectorPartitionServingSnapshotLeaseV1) IdentityV1() VectorPartitionServingSnapshotIdentityV1 {
	if l == nil || l.snapshot == nil {
		return VectorPartitionServingSnapshotIdentityV1{}
	}
	identity := l.snapshot.identity
	identity.ReadyGroups = slices.Clone(identity.ReadyGroups)
	identity.LocalGroups = slices.Clone(identity.LocalGroups)
	return identity
}

func (l *VectorPartitionServingSnapshotLeaseV1) Close() error {
	if l == nil || l.publisher == nil || l.snapshot == nil {
		return nil
	}
	l.closeOnce.Do(func() { l.closeErr = l.publisher.releaseV1(l.snapshot) })
	return l.closeErr
}

func (p *VectorPartitionServingSnapshotPublisherV1) releaseV1(snapshot *vectorPartitionServingSnapshotV1) error {
	p.mu.Lock()
	if snapshot.refs > 0 {
		snapshot.refs--
		p.stats.Releases++
		if p.stats.CurrentPins > 0 {
			p.stats.CurrentPins--
		}
	}
	closeSnapshot := snapshot.retired && snapshot.refs == 0
	p.mu.Unlock()
	if closeSnapshot {
		err := snapshot.close()
		p.recordSnapshotCloseV1(err)
		return err
	}
	return nil
}

func (p *VectorPartitionServingSnapshotPublisherV1) RefreshProofV1(ctx context.Context) error {
	if p == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	p.mu.Lock()
	snapshot := p.current
	closed := p.closed
	p.mu.Unlock()
	if closed || snapshot == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	placement := p.opts.Coordinator.placement
	fresh, err := p.opts.Authority.captureVectorPartitionServingAuthorityV1(
		raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceUnknownV1),
		placement.Collection, placement.IndexName, placement.PartitionGeneration, placement.IndexDefinitionDigest,
		placement.SourceGeneration, placement.SourceChecksum, placement.SourceSchemaHash, placement.SourceRowCount,
	)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil || p.closed || p.current != snapshot || !sameVectorPartitionServingAuthorityProofV1(snapshot.proof, fresh) {
		p.stats.ProofRefreshFailures++
		if err != nil {
			return err
		}
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	snapshot.proof = fresh
	p.stats.ProofRefreshes++
	return nil
}

func (p *VectorPartitionServingSnapshotPublisherV1) InvalidateV1() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	current := p.current
	p.current = nil
	if current != nil {
		current.retired = true
		p.stats.Invalidations++
		if p.stats.CurrentSnapshots > 0 {
			p.stats.CurrentSnapshots--
		}
	}
	closeSnapshot := current != nil && current.refs == 0
	p.mu.Unlock()
	p.wakeRefreshV1()
	if closeSnapshot {
		err := current.close()
		p.recordSnapshotCloseV1(err)
		return err
	}
	return nil
}

func (p *VectorPartitionServingSnapshotPublisherV1) StatsV1() VectorPartitionServingSnapshotPublisherStatsV1 {
	if p == nil {
		return VectorPartitionServingSnapshotPublisherStatsV1{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *VectorPartitionServingSnapshotPublisherV1) startRefreshLoopV1() {
	p.startOnce.Do(func() {
		p.wg.Add(1)
		go p.refreshLoopV1()
	})
}

func (p *VectorPartitionServingSnapshotPublisherV1) refreshLoopV1() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		snapshot := p.current
		var proof raftcluster.CatalogMetaReadProofV1
		if snapshot != nil {
			proof = snapshot.proof.read
		}
		p.mu.Unlock()
		if snapshot == nil {
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		delay := time.Duration(proof.ValidThroughUnixNano-proof.IssuedAtUnixNano) / 2
		if delay < 10*time.Millisecond {
			delay = 10 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return
		case <-p.wake:
			timer.Stop()
			continue
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(p.ctx, vectorPartitionServingSnapshotRefreshTimeoutV1)
		_ = p.RefreshProofV1(ctx)
		cancel()
	}
}

func (p *VectorPartitionServingSnapshotPublisherV1) wakeRefreshV1() {
	if p == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *VectorPartitionServingSnapshotPublisherV1) recordSnapshotCloseV1(err error) {
	p.mu.Lock()
	p.stats.SnapshotCloses++
	p.closeErr = errors.Join(p.closeErr, err)
	p.mu.Unlock()
}

func (p *VectorPartitionServingSnapshotPublisherV1) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		current := p.current
		p.current = nil
		if current != nil {
			current.retired = true
			if p.stats.CurrentSnapshots > 0 {
				p.stats.CurrentSnapshots--
			}
		}
		closeSnapshot := current != nil && current.refs == 0
		p.mu.Unlock()
		p.cancel()
		p.wg.Wait()
		if closeSnapshot {
			p.recordSnapshotCloseV1(current.close())
		}
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeErr
}

func (s *vectorPartitionServingSnapshotV1) close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		groups := make([]raftcluster.GroupID, 0, len(s.generations))
		for group := range s.generations {
			groups = append(groups, group)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
		for _, group := range groups {
			partitions := make([]uint32, 0, len(s.partitions[group]))
			for partition := range s.partitions[group] {
				partitions = append(partitions, partition)
			}
			slices.Sort(partitions)
			for _, partition := range partitions {
				s.closeErr = errors.Join(s.closeErr, s.partitions[group][partition].Close())
			}
		}
		for _, group := range groups {
			if generation := s.generations[group]; generation != nil {
				s.closeErr = errors.Join(s.closeErr, generation.Close())
			}
		}
		if s.router != nil {
			s.closeErr = errors.Join(s.closeErr, s.router.Close())
		}
	})
	return s.closeErr
}
