package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	vectorIndexPartitionLiveMaxMutatedIDsV1 = 128 << 10
	// The live overlay is serialized inside the vector-index metadata record.
	// Bound retained graph state plus conservative snapshot/JSON copy headroom
	// before accepting a document mutation.
	vectorIndexPartitionLiveMaxBytesV1 = 256 << 20
)

var (
	ErrVectorIndexPartitionLiveUnavailableV1 = errors.New("collections: vector partition live state unavailable")
	ErrVectorIndexPartitionLiveMismatchV1    = errors.New("collections: vector partition live state mismatch")
	ErrVectorIndexPartitionLiveCapacityV1    = errors.New("collections: vector partition live mutation capacity exceeded")
)

var vectorPartitionLiveBeforeBindingPublicationHookForTest struct {
	mu sync.Mutex
	fn func()
}

var vectorPartitionLiveBeforeBindingTransitionHookForTest struct {
	mu sync.Mutex
	fn func()
}

var vectorPartitionLiveInsertDocumentBeforePublicationHookForTest struct {
	mu sync.Mutex
	fn func()
}

func setVectorPartitionLiveInsertDocumentBeforePublicationHookForTest(fn func()) func() {
	vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Lock()
	previous := vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.fn
	vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.fn = fn
	vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Unlock()
	return func() {
		vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Lock()
		vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.fn = previous
		vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Unlock()
	}
}

func runVectorPartitionLiveInsertDocumentBeforePublicationHookForTest() {
	vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Lock()
	fn := vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.fn
	vectorPartitionLiveInsertDocumentBeforePublicationHookForTest.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func setVectorPartitionLiveBeforeBindingTransitionHookForTest(fn func()) func() {
	vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Lock()
	previous := vectorPartitionLiveBeforeBindingTransitionHookForTest.fn
	vectorPartitionLiveBeforeBindingTransitionHookForTest.fn = fn
	vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Unlock()
	return func() {
		vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Lock()
		vectorPartitionLiveBeforeBindingTransitionHookForTest.fn = previous
		vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Unlock()
	}
}

func runVectorPartitionLiveBeforeBindingTransitionHookForTest() {
	vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Lock()
	fn := vectorPartitionLiveBeforeBindingTransitionHookForTest.fn
	vectorPartitionLiveBeforeBindingTransitionHookForTest.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func setVectorPartitionLiveBeforeBindingPublicationHookForTest(fn func()) func() {
	vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Lock()
	previous := vectorPartitionLiveBeforeBindingPublicationHookForTest.fn
	vectorPartitionLiveBeforeBindingPublicationHookForTest.fn = fn
	vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Unlock()
	return func() {
		vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Lock()
		vectorPartitionLiveBeforeBindingPublicationHookForTest.fn = previous
		vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Unlock()
	}
}

func runVectorPartitionLiveBeforeBindingPublicationHookForTest() {
	vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Lock()
	fn := vectorPartitionLiveBeforeBindingPublicationHookForTest.fn
	vectorPartitionLiveBeforeBindingPublicationHookForTest.mu.Unlock()
	if fn != nil {
		fn()
	}
}

type vectorPartitionLiveRepresentativeV1 struct {
	domain uint32
	vector []float32
}

type vectorPartitionLiveOwnerV1 struct {
	domain  uint32
	deleted bool
}

type vectorIndexPartitionLiveUndoCurrentNodeV2 struct {
	nodeID int
	exists bool
}

// vectorIndexPartitionLiveDomainUndoV2 is an attempt-local undo log used only
// while a grouped document/carrier publication owns mutation admission. It
// retains the immutable published search view and copies only graph rows that
// the attempted HNSW mutation changes.
type vectorIndexPartitionLiveDomainUndoV2 struct {
	nodesLen                          int
	vectorRowsLen                     int
	nodes                             map[int]vectorIndexNode
	currentNodes                      map[string]vectorIndexPartitionLiveUndoCurrentNodeV2
	entry                             int
	maxLevel                          int
	mutationSeq                       uint64
	persistedEpoch                    uint64
	fullSnapshotBaseEpoch             uint64
	persistedBytesDisk                int64
	persistedSnapshotDirty            bool
	dirtyMeta                         bool
	dirtyNodes                        map[int]struct{}
	dirtyDocs                         map[string]struct{}
	searchViewDirty                   map[int]struct{}
	searchViewCurrent                 bool
	searchViewAcknowledged            bool
	searchViewAcknowledgedMutationSeq uint64
	searchViewAcknowledgedGeneration  uint64
	searchViewPersisted               *vectorIndexSearchPersistedMetadata
}

func cloneVectorIndexIntSetV2(in map[int]struct{}) map[int]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[int]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
}

func cloneVectorIndexStringSetV2(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
}

func (idx *VectorIndex) beginPartitionLiveDomainMutationV2Locked() error {
	if idx.partitionLiveMutationUndo != nil {
		return nil
	}
	// Partition-live domains are built incrementally and never use the frozen
	// contiguous-row construction layout. Refuse an unexpected layout rather
	// than copying the whole domain merely to make rollback possible.
	if len(idx.vectorRows) != 0 {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	var searchViewPersisted *vectorIndexSearchPersistedMetadata
	if view := idx.searchView.Load(); view != nil {
		searchViewPersisted = view.persisted.Load()
	}
	idx.partitionLiveMutationUndo = &vectorIndexPartitionLiveDomainUndoV2{
		nodesLen:                          len(idx.nodes),
		vectorRowsLen:                     len(idx.vectorRows),
		nodes:                             make(map[int]vectorIndexNode),
		currentNodes:                      make(map[string]vectorIndexPartitionLiveUndoCurrentNodeV2),
		entry:                             idx.entry,
		maxLevel:                          idx.maxLevel,
		mutationSeq:                       idx.mutationSeq,
		persistedEpoch:                    idx.persistedEpoch,
		fullSnapshotBaseEpoch:             idx.fullSnapshotBaseEpoch,
		persistedBytesDisk:                idx.persistedBytesDisk,
		persistedSnapshotDirty:            idx.persistedSnapshotDirty,
		dirtyMeta:                         idx.dirtyMeta,
		dirtyNodes:                        cloneVectorIndexIntSetV2(idx.dirtyNodes),
		dirtyDocs:                         cloneVectorIndexStringSetV2(idx.dirtyDocs),
		searchViewDirty:                   cloneVectorIndexIntSetV2(idx.searchViewDirty),
		searchViewCurrent:                 idx.searchViewCurrent.Load(),
		searchViewAcknowledged:            idx.searchViewAcknowledged,
		searchViewAcknowledgedMutationSeq: idx.searchViewAcknowledgedMutationSeq,
		searchViewAcknowledgedGeneration:  idx.searchViewAcknowledgedGeneration,
		searchViewPersisted:               searchViewPersisted,
	}
	return nil
}

func (idx *VectorIndex) capturePartitionLiveDomainNodeV2Locked(nodeID int) {
	undo := idx.partitionLiveMutationUndo
	if undo == nil || nodeID < 0 || nodeID >= undo.nodesLen {
		return
	}
	if _, captured := undo.nodes[nodeID]; captured {
		return
	}
	undo.nodes[nodeID] = cloneVectorIndexSearchNode(idx.nodes[nodeID])
}

func (idx *VectorIndex) capturePartitionLiveDomainCurrentNodeV2Locked(documentID []byte) {
	undo := idx.partitionLiveMutationUndo
	if undo == nil {
		return
	}
	key := string(documentID)
	if _, captured := undo.currentNodes[key]; captured {
		return
	}
	nodeID, exists := idx.currentNode[key]
	undo.currentNodes[key] = vectorIndexPartitionLiveUndoCurrentNodeV2{nodeID: nodeID, exists: exists}
}

func (idx *VectorIndex) rollbackPartitionLiveDomainMutationV2Locked() {
	undo := idx.partitionLiveMutationUndo
	if undo == nil {
		return
	}
	for nodeID, node := range undo.nodes {
		if nodeID >= 0 && nodeID < len(idx.nodes) {
			idx.nodes[nodeID] = node
		}
	}
	if undo.nodesLen < len(idx.nodes) {
		clear(idx.nodes[undo.nodesLen:])
		idx.nodes = idx.nodes[:undo.nodesLen]
	}
	if undo.vectorRowsLen < len(idx.vectorRows) {
		clear(idx.vectorRows[undo.vectorRowsLen:])
		idx.vectorRows = idx.vectorRows[:undo.vectorRowsLen]
	}
	for id, current := range undo.currentNodes {
		if current.exists {
			idx.currentNode[id] = current.nodeID
		} else {
			delete(idx.currentNode, id)
		}
	}
	idx.entry = undo.entry
	idx.maxLevel = undo.maxLevel
	idx.mutationSeq = undo.mutationSeq
	idx.persistedEpoch = undo.persistedEpoch
	idx.fullSnapshotBaseEpoch = undo.fullSnapshotBaseEpoch
	idx.persistedBytesDisk = undo.persistedBytesDisk
	idx.persistedSnapshotDirty = undo.persistedSnapshotDirty
	idx.dirtyMeta = undo.dirtyMeta
	idx.dirtyNodes = undo.dirtyNodes
	idx.dirtyDocs = undo.dirtyDocs
	idx.searchViewDirty = undo.searchViewDirty
	idx.searchViewCurrent.Store(undo.searchViewCurrent)
	idx.searchViewAcknowledged = undo.searchViewAcknowledged
	idx.searchViewAcknowledgedMutationSeq = undo.searchViewAcknowledgedMutationSeq
	idx.searchViewAcknowledgedGeneration = undo.searchViewAcknowledgedGeneration
	if view := idx.searchView.Load(); view != nil {
		view.persisted.Store(undo.searchViewPersisted)
	}
	idx.partitionLiveMutationUndo = nil
}

func (idx *VectorIndex) commitPartitionLiveDomainMutationV2Locked() {
	if idx.partitionLiveMutationUndo == nil {
		return
	}
	idx.partitionLiveMutationUndo = nil
	idx.acknowledgeSearchViewStateLocked()
	idx.publishSearchViewLocked(false)
}

// vectorIndexPartitionLiveStateV1 is deliberately owned by the registered
// VectorIndex. The immutable partition generation remains the base; this is
// only its bounded, durable live overlay.
type vectorIndexPartitionLiveStateV1 struct {
	indexDefinitionDigest string
	source                VectorPartitionSourceIdentityV1
	generation            uint64
	revision              uint64
	coverage              uint64
	packDomains           []uint32
	representatives       []vectorPartitionLiveRepresentativeV1
	domains               map[uint32]*VectorIndex
	owners                map[string]vectorPartitionLiveOwnerV1
	ownerCount            int
	ownerEpoch            uint64
	domainEpochHighWater  uint64
	domainEpochs          map[uint32]uint64
	dirtyOwners           map[string]struct{}
	dirtyDomains          map[uint32]struct{}
	fullDomains           map[uint32]struct{}
	stagedOwners          map[string]vectorPartitionLiveOwnerV1
	sharedDomains         map[uint32]struct{}
	domainTransactions    map[uint32]*VectorIndex
	ownerSource           *VectorIndex
	cutovers              uint64
	cutoverPublications   uint64
	nodeCapacity          int
	byteCapacity          uint64
	nodeIDBytes           uint64
	ownerIDBytes          uint64
	bindingDurable        bool
	invalid               bool
}

type vectorIndexPartitionLivePersistV1 struct {
	Version               int                                            `json:"version"`
	IndexDefinitionDigest string                                         `json:"index_definition_digest"`
	Source                VectorPartitionSourceIdentityV1                `json:"source"`
	Generation            uint64                                         `json:"generation"`
	Revision              uint64                                         `json:"revision"`
	Coverage              uint64                                         `json:"coverage"`
	Cutovers              uint64                                         `json:"cutovers"`
	PackDomains           []uint32                                       `json:"pack_domains"`
	Representatives       []vectorIndexPartitionLivePersistRepV1         `json:"representatives"`
	OwnerCount            uint64                                         `json:"owner_count,omitempty"`
	OwnerEpoch            uint64                                         `json:"owner_epoch,omitempty"`
	DomainEpochHighWater  uint64                                         `json:"domain_epoch_high_water,omitempty"`
	DomainEpochs          []vectorIndexPartitionLivePersistDomainEpochV2 `json:"domain_epochs,omitempty"`
	Owners                []vectorIndexPartitionLivePersistOwnerV1       `json:"owners,omitempty"`
	Domains               []vectorIndexPartitionLivePersistDomainV1      `json:"domains,omitempty"`
}

type vectorIndexPartitionLivePersistRepV1 struct {
	Domain uint32    `json:"domain"`
	Vector []float32 `json:"vector"`
}
type vectorIndexPartitionLivePersistOwnerV1 struct {
	ID      string `json:"id"`
	Domain  uint32 `json:"domain"`
	Deleted bool   `json:"deleted"`
}
type vectorIndexPartitionLivePersistDomainV1 struct {
	Domain   uint32                     `json:"domain"`
	Epoch    uint64                     `json:"epoch,omitempty"`
	Snapshot vectorIndexPersistSnapshot `json:"snapshot"`
}

type vectorIndexPartitionLivePersistDomainEpochV2 struct {
	Domain uint32 `json:"domain"`
	Epoch  uint64 `json:"epoch"`
}

type VectorIndexPartitionLiveStatusV1 struct {
	Generation uint64
	Revision   uint64
	Coverage   uint64
	MutatedIDs int
	LiveIDs    int
	Cutovers   uint64
}

type VectorIndexPartitionLiveSearchPinV1 struct {
	status             VectorIndexPartitionLiveStatusV1
	packDomains        []uint32
	excludedStableIDs  map[string]struct{}
	domains            map[uint32]vectorIndexPartitionLiveDomainPinV1
	releasePublication func()
	releaseOnce        sync.Once
}

type vectorPartitionLiveSearchPinKeyV1 struct {
	index      string
	generation uint64
	revision   uint64
	coverage   uint64
	commitSeq  uint64
	systemRoot uint64
}

type vectorIndexPartitionLiveDomainPinV1 struct {
	index            *VectorIndex
	view             *vectorIndexSearchView
	maxStableIDBytes int
}

func vectorPartitionLiveSourceV1(m VectorPartitionManifestV1) VectorPartitionSourceIdentityV1 {
	return VectorPartitionSourceIdentityV1{Generation: m.SourceGeneration, Checksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash, RowCount: m.SourceRowCount}
}

func vectorPartitionLiveRoutingIdentityMatchesV1(packDomains []uint32, manifest VectorPartitionManifestV1) bool {
	if manifest.DomainCount == 0 || uint64(len(packDomains)) != uint64(manifest.PartitionCount) || len(manifest.DomainPacks) != len(packDomains) {
		return false
	}
	var lastDomain, lastPack uint32
	for i, mapping := range manifest.DomainPacks {
		if mapping.DomainID >= manifest.DomainCount || uint64(mapping.PackID) >= uint64(len(packDomains)) || packDomains[mapping.PackID] != mapping.DomainID ||
			(i == 0 && mapping.DomainID != 0) ||
			(i > 0 && (mapping.DomainID < lastDomain || mapping.DomainID > lastDomain+1 || mapping.DomainID == lastDomain && mapping.PackID <= lastPack)) {
			return false
		}
		lastDomain, lastPack = mapping.DomainID, mapping.PackID
	}
	return lastDomain == manifest.DomainCount-1
}

func vectorPartitionLiveManifestRoutingIdentityMatchesV1(authoritative, candidate VectorPartitionManifestV1) bool {
	if authoritative.DomainCount != candidate.DomainCount || authoritative.PartitionCount != candidate.PartitionCount || len(authoritative.DomainPacks) != len(candidate.DomainPacks) {
		return false
	}
	for i := range authoritative.DomainPacks {
		if authoritative.DomainPacks[i] != candidate.DomainPacks[i] {
			return false
		}
	}
	return true
}

func vectorPartitionLiveRepresentativeLessV1(a, b vectorPartitionLiveRepresentativeV1) bool {
	if a.domain != b.domain {
		return a.domain < b.domain
	}
	for i := 0; i < len(a.vector) && i < len(b.vector); i++ {
		left, right := math.Float32bits(a.vector[i]), math.Float32bits(b.vector[i])
		if left != right {
			return left < right
		}
	}
	return len(a.vector) < len(b.vector)
}

func (idx *VectorIndex) bindVectorPartitionLiveV1(manifest VectorPartitionManifestV1, coverage uint64, representatives []vectorPartitionLiveRepresentativeV1) error {
	if idx == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	if manifest.IndexName != idx.name || manifest.Generation == 0 || coverage == 0 || manifest.DomainCount == 0 || len(manifest.DomainPacks) != int(manifest.PartitionCount) || len(representatives) == 0 {
		return fmt.Errorf("%w: incomplete binding", ErrVectorIndexPartitionLiveMismatchV1)
	}
	packDomains := make([]uint32, manifest.PartitionCount)
	seenPacks := make([]bool, manifest.PartitionCount)
	seenDomains := make([]bool, manifest.DomainCount)
	for _, mapping := range manifest.DomainPacks {
		if mapping.PackID >= manifest.PartitionCount || mapping.DomainID >= manifest.DomainCount || seenPacks[mapping.PackID] {
			return fmt.Errorf("%w: invalid domain pack mapping", ErrVectorIndexPartitionLiveMismatchV1)
		}
		seenPacks[mapping.PackID] = true
		seenDomains[mapping.DomainID] = true
		packDomains[mapping.PackID] = mapping.DomainID
	}
	for _, seen := range seenPacks {
		if !seen {
			return fmt.Errorf("%w: incomplete pack mapping", ErrVectorIndexPartitionLiveMismatchV1)
		}
	}
	clonedReps := make([]vectorPartitionLiveRepresentativeV1, len(representatives))
	representedDomains := make([]bool, manifest.DomainCount)
	for i, rep := range representatives {
		if rep.domain >= manifest.DomainCount || len(rep.vector) != idx.dimensions || !seenDomains[rep.domain] {
			return fmt.Errorf("%w: invalid representative", ErrVectorIndexPartitionLiveMismatchV1)
		}
		if err := validateFloat32Vector(rep.vector); err != nil {
			return err
		}
		clonedReps[i] = vectorPartitionLiveRepresentativeV1{domain: rep.domain, vector: append([]float32(nil), rep.vector...)}
		representedDomains[rep.domain] = true
	}
	for _, represented := range representedDomains {
		if !represented {
			return fmt.Errorf("%w: missing domain representative", ErrVectorIndexPartitionLiveMismatchV1)
		}
	}
	sort.Slice(clonedReps, func(i, j int) bool { return vectorPartitionLiveRepresentativeLessV1(clonedReps[i], clonedReps[j]) })

	idx.mu.Lock()
	defer idx.mu.Unlock()
	source := vectorPartitionLiveSourceV1(manifest)
	ownerEpoch := uint64(1)
	domainEpochHighWater := uint64(0)
	domainEpochs := make(map[uint32]uint64)
	if live := idx.partitionLive; live != nil {
		if live.indexDefinitionDigest == manifest.IndexDefinitionDigest && live.source == source && live.generation == manifest.Generation {
			if len(live.packDomains) != len(packDomains) {
				return ErrVectorIndexPartitionLiveMismatchV1
			}
			for i := range packDomains {
				if live.packDomains[i] != packDomains[i] {
					return ErrVectorIndexPartitionLiveMismatchV1
				}
			}
			if len(live.representatives) != len(clonedReps) {
				return ErrVectorIndexPartitionLiveMismatchV1
			}
			for i := range clonedReps {
				if live.representatives[i].domain != clonedReps[i].domain || len(live.representatives[i].vector) != len(clonedReps[i].vector) {
					return ErrVectorIndexPartitionLiveMismatchV1
				}
				for dimension := range clonedReps[i].vector {
					if math.Float32bits(live.representatives[i].vector[dimension]) != math.Float32bits(clonedReps[i].vector[dimension]) {
						return ErrVectorIndexPartitionLiveMismatchV1
					}
				}
			}
			return nil
		}
		// A newly published immutable generation retires the old overlay.  It
		// may only cut over when it is newer and its base source is the exact
		// current collection source. Existing pins own immutable delta views,
		// so replacing the registered pointer does not invalidate them.
		if manifest.Generation <= live.generation || !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != coverage {
			return ErrVectorIndexPartitionLiveMismatchV1
		}
		if live.ownerEpoch == ^uint64(0) {
			return ErrVectorIndexPartitionLiveCapacityV1
		}
		ownerEpoch = live.ownerEpoch + 1
		domainEpochHighWater = live.domainEpochHighWater
	}
	if idx.collection != nil && (!idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != coverage) {
		return fmt.Errorf("%w: live coverage=%d current=%d", ErrVectorIndexPartitionLiveMismatchV1, coverage, idx.sourceDocumentGeneration)
	}
	if !idx.vectorPartitionLiveBindingBytesFitV1(packDomains, clonedReps) {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	idx.partitionLive = &vectorIndexPartitionLiveStateV1{
		indexDefinitionDigest: manifest.IndexDefinitionDigest, source: source, generation: manifest.Generation,
		coverage: coverage, packDomains: packDomains, representatives: clonedReps,
		domains: make(map[uint32]*VectorIndex), owners: make(map[string]vectorPartitionLiveOwnerV1),
		ownerEpoch: ownerEpoch, domainEpochHighWater: domainEpochHighWater, domainEpochs: domainEpochs, dirtyOwners: make(map[string]struct{}),
		dirtyDomains: make(map[uint32]struct{}), fullDomains: make(map[uint32]struct{}),
		nodeCapacity: vectorIndexPartitionLiveMaxMutatedIDsV1, byteCapacity: vectorIndexPartitionLiveMaxBytesV1, bindingDurable: idx.collection == nil,
	}
	idx.mutationSeq++
	idx.markVectorMetaDirtyLocked()
	return nil
}

func (idx *VectorIndex) invalidateVectorPartitionLiveLocked() {
	if idx.partitionLive != nil {
		idx.partitionLive.invalid = true
		idx.mutationSeq++
		idx.markVectorMetaDirtyLocked()
	}
}

func (idx *VectorIndex) newPartitionLiveDomainIndexV1() (*VectorIndex, error) {
	delta, err := idx.newPartitionLiveDomainIndexUnpublishedV1()
	if err != nil {
		return nil, err
	}
	delta.acknowledgeSearchViewStateLocked()
	delta.publishSearchViewLocked(true)
	return delta, nil
}

func (idx *VectorIndex) newPartitionLiveDomainIndexUnpublishedV1() (*VectorIndex, error) {
	delta, err := newVectorIndex(nil, VectorIndexOptions{Name: idx.name, Field: idx.field, Metric: idx.metric, Encoding: idx.encoding, Dimensions: idx.dimensions, M: idx.m, EfConstruction: idx.efConstruction, EfSearch: idx.efSearch, RebuildDeletedRatio: idx.rebuildDeletedRatio})
	if err != nil {
		return nil, err
	}
	delta.sourceDocumentRootsValid = true
	delta.sourceDocumentGeneration = idx.sourceDocumentGeneration
	delta.trackSearchViewDirty = true
	delta.nativePersistent = true
	return delta, nil
}

func (idx *VectorIndex) partitionLiveRouteLocked(vector []float32) (uint32, error) {
	if len(vector) != idx.dimensions || idx.partitionLive == nil {
		return 0, ErrVectorIndexPartitionLiveUnavailableV1
	}
	bestDomain, bestScore, found := uint32(0), float32(-math.MaxFloat32), false
	for _, rep := range idx.partitionLive.representatives {
		score, err := CanonicalVectorPartitionCosineScoreV1(vector, rep.vector)
		if err != nil {
			return 0, err
		}
		if !found || score > bestScore || score == bestScore && rep.domain < bestDomain {
			bestDomain, bestScore, found = rep.domain, score, true
		}
	}
	if !found {
		return 0, ErrVectorIndexPartitionLiveUnavailableV1
	}
	return bestDomain, nil
}

func (live *vectorIndexPartitionLiveStateV1) ownerV1(id string) (vectorPartitionLiveOwnerV1, bool) {
	if live == nil {
		return vectorPartitionLiveOwnerV1{}, false
	}
	if live.stagedOwners != nil {
		if owner, ok := live.stagedOwners[id]; ok {
			return owner, true
		}
	}
	if live.ownerSource != nil {
		live.ownerSource.mu.RLock()
		owner, ok := live.owners[id]
		live.ownerSource.mu.RUnlock()
		return owner, ok
	}
	owner, ok := live.owners[id]
	return owner, ok
}

func (live *vectorIndexPartitionLiveStateV1) ownerCountV1() int {
	if live == nil {
		return 0
	}
	return live.ownerCount
}

func (live *vectorIndexPartitionLiveStateV1) setOwnerV1(id string, owner vectorPartitionLiveOwnerV1) {
	if live.stagedOwners != nil {
		live.stagedOwners[id] = owner
	} else {
		live.owners[id] = owner
	}
	if live.dirtyOwners == nil {
		live.dirtyOwners = make(map[string]struct{})
	}
	live.dirtyOwners[id] = struct{}{}
}

func (live *vectorIndexPartitionLiveStateV1) rangeOwnersV1(fn func(string, vectorPartitionLiveOwnerV1) bool) {
	if live == nil {
		return
	}
	if live.ownerSource != nil {
		live.ownerSource.mu.RLock()
		defer live.ownerSource.mu.RUnlock()
	}
	for id, owner := range live.owners {
		if staged, ok := live.stagedOwners[id]; ok {
			owner = staged
		}
		if !fn(id, owner) {
			return
		}
	}
	for id, owner := range live.stagedOwners {
		if _, exists := live.owners[id]; exists {
			continue
		}
		if !fn(id, owner) {
			return
		}
	}
}

func (idx *VectorIndex) ensurePartitionLiveDomainWritableLocked(domain uint32) (*VectorIndex, error) {
	live := idx.partitionLive
	if live == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	delta := live.domains[domain]
	if delta == nil {
		var err error
		delta, err = idx.newPartitionLiveDomainIndexV1()
		if err != nil {
			return nil, err
		}
		if live.domainEpochHighWater == ^uint64(0) {
			return nil, ErrVectorIndexPartitionLiveCapacityV1
		}
		live.domainEpochHighWater++
		live.domains[domain] = delta
		live.domainEpochs[domain] = live.domainEpochHighWater
		if live.fullDomains == nil {
			live.fullDomains = make(map[uint32]struct{})
		}
		live.fullDomains[domain] = struct{}{}
	} else if _, shared := live.sharedDomains[domain]; shared {
		if live.domainTransactions == nil {
			live.domainTransactions = make(map[uint32]*VectorIndex)
		}
		if live.domainTransactions[domain] == nil {
			delta.mu.Lock()
			err := delta.beginPartitionLiveDomainMutationV2Locked()
			delta.mu.Unlock()
			if err != nil {
				return nil, err
			}
			live.domainTransactions[domain] = delta
		}
	}
	if live.dirtyDomains == nil {
		live.dirtyDomains = make(map[uint32]struct{})
	}
	live.dirtyDomains[domain] = struct{}{}
	return delta, nil
}

func (idx *VectorIndex) reconcileVectorPartitionMutationLocked(documentID []byte, vector []float32) error {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil
	}
	key := string(documentID)
	old, existed := live.ownerV1(key)
	if !existed && live.ownerCountV1() >= vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if vector == nil {
		if existed && !old.deleted {
			delta, err := idx.ensurePartitionLiveDomainWritableLocked(old.domain)
			if err != nil {
				return err
			}
			if delta != nil {
				delta.mu.Lock()
				delta.tombstoneDocumentIDLocked(documentID)
				if delta.partitionLiveMutationUndo == nil {
					delta.publishSearchViewLocked(false)
				}
				delta.mu.Unlock()
			}
		}
		live.setOwnerV1(key, vectorPartitionLiveOwnerV1{deleted: true})
	} else {
		domain, err := idx.partitionLiveRouteLocked(vector)
		if err != nil {
			return err
		}
		if existed && !old.deleted && old.domain == domain {
			if delta := live.domains[domain]; delta != nil {
				delta.mu.RLock()
				unchanged := delta.currentVectorMatchesLocked(documentID, vector)
				delta.mu.RUnlock()
				if unchanged {
					live.revision++
					idx.mutationSeq++
					if idx.persistedEpoch != 0 {
						idx.persistedSnapshotDirty = true
					}
					idx.markVectorMetaDirtyLocked()
					return nil
				}
			}
		}
		delta, err := idx.ensurePartitionLiveDomainWritableLocked(domain)
		if err != nil {
			return err
		}
		delta.mu.Lock()
		beforeNodes := len(delta.nodes)
		err = delta.insertVectorLocked(documentID, vector)
		if err == nil && delta.partitionLiveMutationUndo == nil {
			delta.acknowledgeSearchViewStateLocked()
			delta.publishSearchViewLocked(false)
		}
		afterNodes := len(delta.nodes)
		delta.mu.Unlock()
		if err != nil {
			return err
		}
		if existed && !old.deleted && old.domain != domain {
			oldDelta, err := idx.ensurePartitionLiveDomainWritableLocked(old.domain)
			if err != nil {
				return err
			}
			if oldDelta != nil {
				oldDelta.mu.Lock()
				oldDelta.tombstoneDocumentIDLocked(documentID)
				if oldDelta.partitionLiveMutationUndo == nil {
					oldDelta.publishSearchViewLocked(false)
				}
				oldDelta.mu.Unlock()
			}
		}
		live.setOwnerV1(key, vectorPartitionLiveOwnerV1{domain: domain})
		if afterNodes > beforeNodes {
			live.nodeIDBytes += uint64(len(documentID))
		}
	}
	if !existed {
		live.ownerCount++
		live.ownerIDBytes += uint64(len(documentID))
	}
	live.revision++
	idx.mutationSeq++
	if idx.persistedEpoch != 0 {
		idx.persistedSnapshotDirty = true
	}
	idx.markVectorMetaDirtyLocked()
	return nil
}

func (idx *VectorIndex) partitionLiveNodeCountLocked() int {
	if idx.partitionLive == nil {
		return 0
	}
	total := 0
	for _, domain := range idx.partitionLive.domains {
		if domain == nil || len(domain.nodes) > vectorIndexPartitionLiveMaxMutatedIDsV1-total {
			return vectorIndexPartitionLiveMaxMutatedIDsV1
		}
		total += len(domain.nodes)
	}
	return total
}

func (idx *VectorIndex) partitionLiveNodeCapacityLocked() int {
	if idx.partitionLive == nil || idx.partitionLive.nodeCapacity <= 0 {
		return vectorIndexPartitionLiveMaxMutatedIDsV1
	}
	return idx.partitionLive.nodeCapacity
}

func (idx *VectorIndex) partitionLiveByteCapacityLocked() uint64 {
	if idx.partitionLive == nil || idx.partitionLive.byteCapacity == 0 {
		return vectorIndexPartitionLiveMaxBytesV1
	}
	return idx.partitionLive.byteCapacity
}

func vectorPartitionLiveAddBytesV1(total *uint64, value uint64) bool {
	if value > ^uint64(0)-*total {
		return false
	}
	*total += value
	return true
}

func (idx *VectorIndex) vectorPartitionLiveNodeBytesV1() (uint64, bool) {
	dimensions, m := uint64(idx.dimensions), uint64(maxInt(idx.m, 1))
	// Runtime node/vector data, all 33 bounded HNSW layers, and conservative
	// slice/fixed overhead. The final multiplier in the state estimate covers
	// persisted snapshots, JSON expansion, and transient copies.
	if dimensions > (^uint64(0)-1024)/5 || m > (^uint64(0)-1024)/(34*8) {
		return 0, false
	}
	return 1024 + dimensions*5 + m*34*8, true
}

func (idx *VectorIndex) vectorPartitionLiveBindingBytesFitV1(packDomains []uint32, reps []vectorPartitionLiveRepresentativeV1) bool {
	total := uint64(1024 + len(packDomains)*4)
	for _, rep := range reps {
		if !vectorPartitionLiveAddBytesV1(&total, uint64(len(rep.vector))*4+64) {
			return false
		}
	}
	return total <= vectorIndexPartitionLiveMaxBytesV1/8
}

func (idx *VectorIndex) partitionLiveBytesFitLocked(additionalNodes, additionalOwners int, additionalNodeIDBytes, additionalOwnerIDBytes uint64) bool {
	live := idx.partitionLive
	if live == nil || additionalNodes < 0 || additionalOwners < 0 {
		return live == nil
	}
	nodeBytes, ok := idx.vectorPartitionLiveNodeBytesV1()
	if !ok {
		return false
	}
	nodes := idx.partitionLiveNodeCountLocked()
	if additionalNodes > math.MaxInt-nodes || additionalOwners > math.MaxInt-live.ownerCountV1() {
		return false
	}
	total := uint64(1024 + len(live.packDomains)*(4+128) + (live.ownerCountV1()+additionalOwners)*128)
	for _, rep := range live.representatives {
		if !vectorPartitionLiveAddBytesV1(&total, uint64(len(rep.vector))*4+64) {
			return false
		}
	}
	if uint64(nodes+additionalNodes) > ^uint64(0)/nodeBytes || !vectorPartitionLiveAddBytesV1(&total, uint64(nodes+additionalNodes)*nodeBytes) ||
		!vectorPartitionLiveAddBytesV1(&total, live.nodeIDBytes) || !vectorPartitionLiveAddBytesV1(&total, additionalNodeIDBytes) ||
		!vectorPartitionLiveAddBytesV1(&total, live.ownerIDBytes) || !vectorPartitionLiveAddBytesV1(&total, additionalOwnerIDBytes) ||
		total > ^uint64(0)/8 {
		return false
	}
	return total*8 <= idx.partitionLiveByteCapacityLocked()
}

func (idx *VectorIndex) vectorPartitionLivePersistBytesFitV1(persisted *vectorIndexPartitionLivePersistV1) bool {
	if persisted == nil {
		return true
	}
	nodeBytes, ok := idx.vectorPartitionLiveNodeBytesV1()
	if !ok {
		return false
	}
	total := uint64(1024 + len(persisted.PackDomains)*4 + len(persisted.Domains)*128 + len(persisted.Owners)*128)
	for _, rep := range persisted.Representatives {
		if !vectorPartitionLiveAddBytesV1(&total, uint64(len(rep.Vector))*4+64) {
			return false
		}
	}
	nodes := uint64(0)
	for _, domain := range persisted.Domains {
		if uint64(len(domain.Snapshot.Nodes)) > ^uint64(0)-nodes {
			return false
		}
		nodes += uint64(len(domain.Snapshot.Nodes))
		for _, node := range domain.Snapshot.Nodes {
			if !vectorPartitionLiveAddBytesV1(&total, uint64(len(node.DocumentID))) {
				return false
			}
		}
	}
	if nodes > ^uint64(0)/nodeBytes || !vectorPartitionLiveAddBytesV1(&total, nodes*nodeBytes) {
		return false
	}
	for _, owner := range persisted.Owners {
		if !vectorPartitionLiveAddBytesV1(&total, uint64(len(owner.ID))) {
			return false
		}
	}
	return total <= vectorIndexPartitionLiveMaxBytesV1/8
}

func (idx *VectorIndex) partitionLiveActiveOwnerCountLocked() int {
	if idx.partitionLive == nil {
		return 0
	}
	active := 0
	idx.partitionLive.rangeOwnersV1(func(_ string, owner vectorPartitionLiveOwnerV1) bool {
		if !owner.deleted {
			active++
		}
		return true
	})
	return active
}

// cutoverVectorPartitionLiveLocked compacts each logical domain from current
// live owners. The parent lock makes the map swap atomic; existing pins keep
// their old indexes and immutable search views until Release.
func (idx *VectorIndex) cutoverVectorPartitionLiveLocked() error {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	if idx.partitionLiveNodeCountLocked() <= idx.partitionLiveActiveOwnerCountLocked() {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	ownerIDs := make([]string, 0, live.ownerCountV1())
	live.rangeOwnersV1(func(id string, owner vectorPartitionLiveOwnerV1) bool {
		if !owner.deleted {
			ownerIDs = append(ownerIDs, id)
		}
		return true
	})
	sort.Strings(ownerIDs)
	fresh := make(map[uint32]*VectorIndex, len(live.domains))
	for _, id := range ownerIDs {
		owner, _ := live.ownerV1(id)
		old := live.domains[owner.domain]
		if old == nil {
			return ErrVectorIndexPartitionLiveUnavailableV1
		}
		old.mu.RLock()
		nodeID, ok := old.currentNode[id]
		if !ok || nodeID < 0 || nodeID >= len(old.nodes) || old.nodes[nodeID].deleted {
			old.mu.RUnlock()
			return ErrVectorIndexPartitionLiveUnavailableV1
		}
		vector := make([]float32, old.nodes[nodeID].vectorDimensions())
		for dimension := range vector {
			vector[dimension] = old.nodes[nodeID].vectorValueAt(dimension)
		}
		old.mu.RUnlock()

		delta := fresh[owner.domain]
		var err error
		if delta == nil {
			delta, err = idx.newPartitionLiveDomainIndexUnpublishedV1()
			if err != nil {
				return err
			}
			fresh[owner.domain] = delta
		}
		delta.mu.Lock()
		err = delta.insertVectorLocked([]byte(id), vector)
		delta.mu.Unlock()
		if err != nil {
			return err
		}
	}
	domains := make([]uint32, 0, len(fresh))
	for domain := range fresh {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	if uint64(len(domains)) > ^uint64(0)-live.domainEpochHighWater {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	for _, domain := range domains {
		delta := fresh[domain]
		delta.mu.Lock()
		delta.acknowledgeSearchViewStateLocked()
		delta.publishSearchViewLocked(true)
		delta.mu.Unlock()
		live.cutoverPublications++
	}
	live.domains = fresh
	live.domainEpochs = make(map[uint32]uint64, len(domains))
	live.dirtyDomains = make(map[uint32]struct{}, len(domains))
	live.fullDomains = make(map[uint32]struct{}, len(domains))
	for _, domain := range domains {
		live.domainEpochHighWater++
		live.domainEpochs[domain] = live.domainEpochHighWater
		live.dirtyDomains[domain] = struct{}{}
		live.fullDomains[domain] = struct{}{}
	}
	clear(live.sharedDomains)
	live.nodeIDBytes = 0
	for _, id := range ownerIDs {
		live.nodeIDBytes += uint64(len(id))
	}
	live.cutovers++
	idx.mutationSeq++
	idx.markVectorMetaDirtyLocked()
	return nil
}

func (idx *VectorIndex) ensureVectorPartitionLiveCapacityLocked(additionalNodes int) error {
	if idx.partitionLive == nil || additionalNodes <= 0 {
		return nil
	}
	capacity := idx.partitionLiveNodeCapacityLocked()
	nodes := idx.partitionLiveNodeCountLocked()
	if additionalNodes <= capacity && nodes <= capacity-additionalNodes {
		return nil
	}
	active := idx.partitionLiveActiveOwnerCountLocked()
	if additionalNodes > capacity || active > capacity-additionalNodes {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if err := idx.cutoverVectorPartitionLiveLocked(); err != nil {
		return err
	}
	nodes = idx.partitionLiveNodeCountLocked()
	if additionalNodes > capacity || nodes > capacity-additionalNodes {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	return nil
}

func (idx *VectorIndex) ensureVectorPartitionLiveByteCapacityLocked(additionalNodes, additionalOwners int, additionalNodeIDBytes, additionalOwnerIDBytes uint64) error {
	if idx.partitionLive == nil || idx.partitionLiveBytesFitLocked(additionalNodes, additionalOwners, additionalNodeIDBytes, additionalOwnerIDBytes) {
		return nil
	}
	if idx.partitionLiveNodeCountLocked() <= idx.partitionLiveActiveOwnerCountLocked() {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if err := idx.cutoverVectorPartitionLiveLocked(); err != nil {
		return err
	}
	if !idx.partitionLiveBytesFitLocked(additionalNodes, additionalOwners, additionalNodeIDBytes, additionalOwnerIDBytes) {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	return nil
}

func (idx *VectorIndex) preflightVectorPartitionMutationLocked(documentID []byte, vector []float32) error {
	if idx.partitionLive == nil {
		return nil
	}
	_, exists := idx.partitionLive.ownerV1(string(documentID))
	if !exists && idx.partitionLive.ownerCountV1() >= vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if vector != nil {
		domain, err := idx.partitionLiveRouteLocked(vector)
		if err != nil {
			return err
		}
		additionalNodes := 1
		additionalNodeIDBytes := uint64(len(documentID))
		if owner, ok := idx.partitionLive.ownerV1(string(documentID)); ok && !owner.deleted && owner.domain == domain {
			if delta := idx.partitionLive.domains[domain]; delta != nil {
				delta.mu.RLock()
				unchanged := delta.currentVectorMatchesLocked(documentID, vector)
				delta.mu.RUnlock()
				if unchanged {
					additionalNodes = 0
					additionalNodeIDBytes = 0
				}
			}
		}
		if err := idx.ensureVectorPartitionLiveCapacityLocked(additionalNodes); err != nil {
			return err
		}
		ownerBytes := uint64(0)
		if !exists {
			ownerBytes = uint64(len(documentID))
		}
		additionalOwners := 0
		if !exists {
			additionalOwners = 1
		}
		if err := idx.ensureVectorPartitionLiveByteCapacityLocked(additionalNodes, additionalOwners, additionalNodeIDBytes, ownerBytes); err != nil {
			return err
		}
		return nil
	}
	if !exists {
		return idx.ensureVectorPartitionLiveByteCapacityLocked(0, 1, 0, uint64(len(documentID)))
	}
	return nil
}

func (idx *VectorIndex) preflightVectorPartitionMutationBatchLocked(documentIDs [][]byte, vectors [][]float32) error {
	if idx.partitionLive == nil {
		return nil
	}
	owners := idx.partitionLive.ownerCountV1()
	additionalNodes := 0
	additionalNodeIDBytes := uint64(0)
	additionalOwnerIDBytes := uint64(0)
	newOwners := make(map[string]struct{})
	type preflightOwner struct {
		active    bool
		domain    uint32
		vector    []float32
		fromBatch bool
	}
	batchOwners := make(map[string]preflightOwner)
	for i, documentID := range documentIDs {
		key := string(documentID)
		if _, exists := idx.partitionLive.ownerV1(key); !exists {
			if _, duplicate := newOwners[key]; !duplicate {
				newOwners[key] = struct{}{}
				owners++
				additionalOwnerIDBytes += uint64(len(documentID))
			}
		}
		if owners > vectorIndexPartitionLiveMaxMutatedIDsV1 {
			return ErrVectorIndexPartitionLiveCapacityV1
		}
		owner, seen := batchOwners[key]
		if !seen {
			if current, ok := idx.partitionLive.ownerV1(key); ok {
				owner.active = !current.deleted
				owner.domain = current.domain
			}
		}
		if vectors[i] == nil {
			batchOwners[key] = preflightOwner{fromBatch: true}
			continue
		}
		domain, err := idx.partitionLiveRouteLocked(vectors[i])
		if err != nil {
			return err
		}
		unchanged := false
		if owner.active && owner.domain == domain {
			if owner.fromBatch {
				node := vectorIndexNode{vector: owner.vector}
				if idx.encoding == VectorIndexEncodingInt8 {
					node.vector = nil
					node.quantized, node.quantScale = quantizeVectorIndexInt8(owner.vector)
				}
				unchanged = idx.vectorIndexNodeMatchesSourceVectorLocked(&node, vectors[i])
			} else if delta := idx.partitionLive.domains[domain]; delta != nil {
				delta.mu.RLock()
				unchanged = delta.currentVectorMatchesLocked(documentID, vectors[i])
				delta.mu.RUnlock()
			}
		}
		if !unchanged {
			additionalNodes++
			additionalNodeIDBytes += uint64(len(documentID))
		}
		batchOwners[key] = preflightOwner{active: true, domain: domain, vector: vectors[i], fromBatch: true}
	}
	if err := idx.ensureVectorPartitionLiveCapacityLocked(additionalNodes); err != nil {
		return err
	}
	return idx.ensureVectorPartitionLiveByteCapacityLocked(additionalNodes, len(newOwners), additionalNodeIDBytes, additionalOwnerIDBytes)
}

func (idx *VectorIndex) acquireVectorPartitionLiveSearchPinV1(manifest VectorPartitionManifestV1) (*VectorIndexPartitionLiveSearchPinV1, error) {
	if idx == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.RLock()
	live := idx.partitionLive
	if live == nil || live.indexDefinitionDigest != manifest.IndexDefinitionDigest || live.source != vectorPartitionLiveSourceV1(manifest) || live.generation != manifest.Generation ||
		!vectorPartitionLiveRoutingIdentityMatchesV1(live.packDomains, manifest) {
		idx.mu.RUnlock()
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	if live.invalid || !live.bindingDurable {
		idx.mu.RUnlock()
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	// Once collection reconciliation advances the current source state, only a
	// matching durable overlay coverage is authority for the old immutable base.
	if idx.collection != nil && (!idx.sourceDocumentRootsValid || live.coverage != idx.sourceDocumentGeneration) {
		idx.mu.RUnlock()
		return nil, fmt.Errorf("%w: live coverage=%d current=%d", ErrVectorIndexPartitionLiveMismatchV1, live.coverage, idx.sourceDocumentGeneration)
	}
	pin := &VectorIndexPartitionLiveSearchPinV1{
		status:            VectorIndexPartitionLiveStatusV1{Generation: live.generation, Revision: live.revision, Coverage: live.coverage, MutatedIDs: live.ownerCountV1(), Cutovers: live.cutovers},
		packDomains:       append([]uint32(nil), live.packDomains...),
		excludedStableIDs: make(map[string]struct{}, live.ownerCountV1()),
		domains:           make(map[uint32]vectorIndexPartitionLiveDomainPinV1, len(live.domains)),
	}
	maxStableIDBytes := make(map[uint32]int, len(live.domains))
	live.rangeOwnersV1(func(id string, owner vectorPartitionLiveOwnerV1) bool {
		pin.excludedStableIDs[id] = struct{}{}
		if !owner.deleted {
			pin.status.LiveIDs++
			maxStableIDBytes[owner.domain] = maxInt(maxStableIDBytes[owner.domain], len(id))
		}
		return true
	})
	for domain, delta := range live.domains {
		view := delta.acquireSearchView()
		if view == nil {
			idx.mu.RUnlock()
			pin.Release()
			return nil, ErrVectorIndexPartitionLiveUnavailableV1
		}
		pin.domains[domain] = vectorIndexPartitionLiveDomainPinV1{index: delta, view: view, maxStableIDBytes: maxStableIDBytes[domain]}
	}
	idx.mu.RUnlock()
	return pin, nil
}

func (idx *VectorIndex) partitionLiveMetaPersistLockedV2() *vectorIndexPartitionLivePersistV1 {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil
	}
	persisted := &vectorIndexPartitionLivePersistV1{
		Version: 3, IndexDefinitionDigest: live.indexDefinitionDigest, Source: live.source,
		Generation: live.generation, Revision: live.revision, Coverage: live.coverage, Cutovers: live.cutovers,
		OwnerCount:           uint64(live.ownerCount),
		OwnerEpoch:           live.ownerEpoch,
		DomainEpochHighWater: live.domainEpochHighWater,
		PackDomains:          append([]uint32(nil), live.packDomains...),
		Representatives:      make([]vectorIndexPartitionLivePersistRepV1, len(live.representatives)),
	}
	for i, rep := range live.representatives {
		persisted.Representatives[i] = vectorIndexPartitionLivePersistRepV1{Domain: rep.domain, Vector: append([]float32(nil), rep.vector...)}
	}
	domains := make([]uint32, 0, len(live.domains))
	for domain := range live.domains {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	for _, domain := range domains {
		persisted.DomainEpochs = append(persisted.DomainEpochs, vectorIndexPartitionLivePersistDomainEpochV2{Domain: domain, Epoch: live.domainEpochs[domain]})
	}
	return persisted
}

func (idx *VectorIndex) partitionLiveSnapshotRecordsLockedV2() ([]vectorIndexPartitionLivePersistOwnerV1, []vectorIndexPartitionLivePersistDomainV1) {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil, nil
	}
	owners := make([]vectorIndexPartitionLivePersistOwnerV1, 0, live.ownerCountV1())
	live.rangeOwnersV1(func(id string, owner vectorPartitionLiveOwnerV1) bool {
		owners = append(owners, vectorIndexPartitionLivePersistOwnerV1{ID: id, Domain: owner.domain, Deleted: owner.deleted})
		return true
	})
	sort.Slice(owners, func(i, j int) bool { return owners[i].ID < owners[j].ID })
	domains := make([]vectorIndexPartitionLivePersistDomainV1, 0, len(live.domains))
	domainIDs := make([]uint32, 0, len(live.domains))
	for domain := range live.domains {
		domainIDs = append(domainIDs, domain)
	}
	sort.Slice(domainIDs, func(i, j int) bool { return domainIDs[i] < domainIDs[j] })
	for _, domain := range domainIDs {
		snapshot, _ := live.domains[domain].persistSnapshot()
		domains = append(domains, vectorIndexPartitionLivePersistDomainV1{Domain: domain, Epoch: live.domainEpochs[domain], Snapshot: snapshot})
	}
	return owners, domains
}

func (idx *VectorIndex) acknowledgePartitionLivePersistenceLockedV2() {
	live := idx.partitionLive
	if live == nil {
		return
	}
	for domain := range live.dirtyDomains {
		delta := live.domains[domain]
		if delta == nil {
			continue
		}
		bytesDisk, mutationSeq := delta.nativePersistedBytesAndMutationSequence()
		delta.recordPersistedSnapshot(live.domainEpochs[domain], bytesDisk, mutationSeq)
	}
	clear(live.dirtyOwners)
	clear(live.dirtyDomains)
	clear(live.fullDomains)
}

func (idx *VectorIndex) materializePartitionLiveStagedOwnersV2() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	live := idx.partitionLive
	if live == nil || live.stagedOwners == nil {
		return
	}
	if live.ownerSource != nil {
		live.ownerSource.mu.Lock()
		defer live.ownerSource.mu.Unlock()
	}
	for id, owner := range live.stagedOwners {
		live.owners[id] = owner
	}
	live.stagedOwners = nil
	live.sharedDomains = nil
	live.ownerSource = nil
}

func (idx *VectorIndex) clonePartitionLiveReplayStateV2(before *VectorIndex, beforeSeq, baseRoot uint64) string {
	if idx == nil || before == nil || baseRoot == 0 {
		return "invalid replay clone"
	}
	before.mu.RLock()
	if before.mutationSeq != beforeSeq || before.partitionLive == nil || before.partitionLive.invalid {
		before.mu.RUnlock()
		return "carrier changed"
	}
	sourceGeneration := before.sourceDocumentGeneration
	sourceValid := before.sourceDocumentRootsValid
	sourceState := before.sourceDocumentState
	sourceStateValid := before.sourceDocumentStateValid
	persistedBytesDisk := before.persistedBytesDisk
	old := before.partitionLive
	live := &vectorIndexPartitionLiveStateV1{
		indexDefinitionDigest: old.indexDefinitionDigest, source: old.source, generation: old.generation,
		revision: old.revision, coverage: old.coverage, cutovers: old.cutovers,
		cutoverPublications: old.cutoverPublications, nodeCapacity: old.nodeCapacity, byteCapacity: old.byteCapacity,
		nodeIDBytes: old.nodeIDBytes, ownerIDBytes: old.ownerIDBytes, bindingDurable: old.bindingDurable,
		packDomains:     append([]uint32(nil), old.packDomains...),
		representatives: make([]vectorPartitionLiveRepresentativeV1, len(old.representatives)),
		domains:         make(map[uint32]*VectorIndex, len(old.domains)), owners: old.owners,
		ownerCount: old.ownerCount, ownerEpoch: old.ownerEpoch, domainEpochHighWater: old.domainEpochHighWater, domainEpochs: make(map[uint32]uint64, len(old.domainEpochs)),
		dirtyOwners: make(map[string]struct{}, len(old.dirtyOwners)), dirtyDomains: make(map[uint32]struct{}, len(old.dirtyDomains)), fullDomains: make(map[uint32]struct{}, len(old.fullDomains)),
		stagedOwners: make(map[string]vectorPartitionLiveOwnerV1), sharedDomains: make(map[uint32]struct{}, len(old.domains)), ownerSource: before,
	}
	for i, rep := range old.representatives {
		live.representatives[i] = vectorPartitionLiveRepresentativeV1{domain: rep.domain, vector: append([]float32(nil), rep.vector...)}
	}
	for domain, delta := range old.domains {
		live.domains[domain] = delta
		live.sharedDomains[domain] = struct{}{}
	}
	for domain, epoch := range old.domainEpochs {
		live.domainEpochs[domain] = epoch
	}
	for id := range old.dirtyOwners {
		live.dirtyOwners[id] = struct{}{}
	}
	for domain := range old.dirtyDomains {
		live.dirtyDomains[domain] = struct{}{}
	}
	for domain := range old.fullDomains {
		live.fullDomains[domain] = struct{}{}
	}
	before.mu.RUnlock()

	idx.mu.Lock()
	idx.partitionLive = live
	idx.sourceDocumentGeneration = sourceGeneration
	idx.sourceDocumentRootsValid = sourceValid
	idx.sourceDocumentState = sourceState
	idx.sourceDocumentStateValid = sourceStateValid
	idx.persistedEpoch = baseRoot
	idx.persistedBytesDisk = persistedBytesDisk
	idx.persistedSnapshotDirty = false
	idx.mutationSeq = beforeSeq
	idx.dirtyMeta = false
	clear(idx.dirtyNodes)
	clear(idx.dirtyDocs)
	idx.mu.Unlock()
	return ""
}

func (idx *VectorIndex) restorePartitionLiveV1(persisted *vectorIndexPartitionLivePersistV1, sourceCoverage uint64) (*vectorIndexPartitionLiveStateV1, string) {
	if persisted == nil {
		return nil, ""
	}
	return idx.restorePartitionLiveSnapshotV2(persisted, persisted.Owners, persisted.Domains, sourceCoverage)
}

func (idx *VectorIndex) restorePartitionLiveSnapshotV2(persisted *vectorIndexPartitionLivePersistV1, owners []vectorIndexPartitionLivePersistOwnerV1, domains []vectorIndexPartitionLivePersistDomainV1, sourceCoverage uint64) (*vectorIndexPartitionLiveStateV1, string) {
	if persisted == nil {
		return nil, ""
	}
	if persisted.Version == 1 {
		owners = persisted.Owners
		domains = persisted.Domains
	}
	if (persisted.Version != 1 && persisted.Version != 3) || persisted.IndexDefinitionDigest == "" || persisted.Generation == 0 || persisted.Source.Generation == 0 || persisted.Coverage != sourceCoverage || len(persisted.PackDomains) == 0 || len(persisted.Representatives) == 0 || len(owners) > vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return nil, "invalid_partition_live_meta"
	}
	domainEpochHighWater := persisted.DomainEpochHighWater
	if persisted.Version == 3 {
		if persisted.OwnerEpoch == 0 || persisted.OwnerCount != uint64(len(owners)) || len(persisted.Owners) != 0 || len(persisted.Domains) != 0 {
			return nil, "invalid_partition_live_meta"
		}
		descriptors := make(map[uint32]uint64, len(persisted.DomainEpochs))
		for _, descriptor := range persisted.DomainEpochs {
			if descriptor.Epoch == 0 {
				return nil, "invalid_partition_live_domain"
			}
			if _, duplicate := descriptors[descriptor.Domain]; duplicate {
				return nil, "invalid_partition_live_domain"
			}
			descriptors[descriptor.Domain] = descriptor.Epoch
		}
		for _, epoch := range descriptors {
			if epoch > domainEpochHighWater {
				return nil, "invalid_partition_live_domain"
			}
		}
		if len(descriptors) != len(domains) {
			return nil, "invalid_partition_live_domain"
		}
		for _, domain := range domains {
			if descriptors[domain.Domain] != domain.Epoch || domain.Epoch == 0 {
				return nil, "invalid_partition_live_domain"
			}
		}
	} else {
		for _, domain := range domains {
			epoch := domain.Epoch
			if epoch == 0 {
				epoch = 1
			}
			if epoch > domainEpochHighWater {
				domainEpochHighWater = epoch
			}
		}
	}
	capacitySnapshot := *persisted
	capacitySnapshot.Owners = owners
	capacitySnapshot.Domains = domains
	if !idx.vectorPartitionLivePersistBytesFitV1(&capacitySnapshot) {
		return nil, "invalid_partition_live_meta"
	}
	mappedDomains := make(map[uint32]struct{}, len(persisted.PackDomains))
	for _, domain := range persisted.PackDomains {
		mappedDomains[domain] = struct{}{}
	}
	ownerEpoch := persisted.OwnerEpoch
	if ownerEpoch == 0 {
		ownerEpoch = 1
	}
	live := &vectorIndexPartitionLiveStateV1{indexDefinitionDigest: persisted.IndexDefinitionDigest, source: persisted.Source, generation: persisted.Generation, revision: persisted.Revision, coverage: persisted.Coverage, cutovers: persisted.Cutovers, nodeCapacity: vectorIndexPartitionLiveMaxMutatedIDsV1, byteCapacity: vectorIndexPartitionLiveMaxBytesV1, bindingDurable: true, packDomains: append([]uint32(nil), persisted.PackDomains...), representatives: make([]vectorPartitionLiveRepresentativeV1, len(persisted.Representatives)), domains: make(map[uint32]*VectorIndex), owners: make(map[string]vectorPartitionLiveOwnerV1, len(owners)), ownerCount: len(owners), ownerEpoch: ownerEpoch, domainEpochHighWater: domainEpochHighWater, domainEpochs: make(map[uint32]uint64), dirtyOwners: make(map[string]struct{}), dirtyDomains: make(map[uint32]struct{}), fullDomains: make(map[uint32]struct{})}
	representedDomains := make(map[uint32]struct{}, len(persisted.Representatives))
	for i, rep := range persisted.Representatives {
		if len(rep.Vector) != idx.dimensions {
			return nil, "invalid_partition_live_representative"
		}
		if _, mapped := mappedDomains[rep.Domain]; !mapped {
			return nil, "invalid_partition_live_representative"
		}
		if i > 0 && vectorPartitionLiveRepresentativeLessV1(
			vectorPartitionLiveRepresentativeV1{domain: rep.Domain, vector: rep.Vector},
			vectorPartitionLiveRepresentativeV1{domain: persisted.Representatives[i-1].Domain, vector: persisted.Representatives[i-1].Vector},
		) {
			return nil, "invalid_partition_live_representative"
		}
		if err := validateFloat32Vector(rep.Vector); err != nil {
			return nil, "invalid_partition_live_representative"
		}
		representedDomains[rep.Domain] = struct{}{}
		live.representatives[i] = vectorPartitionLiveRepresentativeV1{domain: rep.Domain, vector: append([]float32(nil), rep.Vector...)}
	}
	if len(representedDomains) != len(mappedDomains) {
		return nil, "invalid_partition_live_representative"
	}
	for _, domain := range domains {
		if _, mapped := mappedDomains[domain.Domain]; !mapped {
			return nil, "invalid_partition_live_domain"
		}
		if _, duplicate := live.domains[domain.Domain]; duplicate {
			return nil, "invalid_partition_live_domain"
		}
		if domain.Snapshot.Meta.PartitionLive != nil {
			return nil, "invalid_partition_live_domain"
		}
		delta, err := idx.newPartitionLiveDomainIndexV1()
		if err != nil {
			return nil, "invalid_partition_live_domain"
		}
		if reason := delta.loadPersistSnapshot(domain.Snapshot); reason != "" {
			return nil, "invalid_partition_live_" + reason
		}
		delta.setNativePersistent(true)
		delta.mu.Lock()
		delta.parallelReciprocalLinks = false
		delta.mu.Unlock()
		domainEpoch := domain.Epoch
		if domainEpoch == 0 {
			domainEpoch = 1
		}
		delta.recordLoadedSnapshot(domainEpoch, 0)
		for _, node := range domain.Snapshot.Nodes {
			live.nodeIDBytes += uint64(len(node.DocumentID))
		}
		live.domains[domain.Domain] = delta
		live.domainEpochs[domain.Domain] = domainEpoch
	}
	totalNodes := 0
	for _, delta := range live.domains {
		if delta == nil || len(delta.nodes) > vectorIndexPartitionLiveMaxMutatedIDsV1-totalNodes {
			return nil, "invalid_partition_live_capacity"
		}
		totalNodes += len(delta.nodes)
	}
	previousID := ""
	activeOwners := 0
	for i, owner := range owners {
		if owner.ID == "" || i > 0 && owner.ID <= previousID {
			return nil, "invalid_partition_live_owner"
		}
		previousID = owner.ID
		live.ownerIDBytes += uint64(len(owner.ID))
		if !owner.Deleted {
			activeOwners++
			delta := live.domains[owner.Domain]
			if delta == nil {
				return nil, "invalid_partition_live_owner_domain"
			}
			delta.mu.RLock()
			node, ok := delta.currentNode[owner.ID]
			valid := ok && node >= 0 && node < len(delta.nodes) && !delta.nodes[node].deleted
			delta.mu.RUnlock()
			if !valid {
				return nil, "invalid_partition_live_owner_node"
			}
		}
		live.owners[owner.ID] = vectorPartitionLiveOwnerV1{domain: owner.Domain, deleted: owner.Deleted}
	}
	activeRows := 0
	for domain, delta := range live.domains {
		delta.mu.RLock()
		for id, node := range delta.currentNode {
			owner, ok := live.owners[id]
			if !ok || owner.deleted || owner.domain != domain || node < 0 || node >= len(delta.nodes) || delta.nodes[node].deleted {
				delta.mu.RUnlock()
				return nil, "invalid_partition_live_owner_node"
			}
			activeRows++
		}
		delta.mu.RUnlock()
	}
	if activeRows != activeOwners {
		return nil, "invalid_partition_live_owner_node"
	}
	if persisted.Version == 1 {
		for id := range live.owners {
			live.dirtyOwners[id] = struct{}{}
		}
		for domain, delta := range live.domains {
			live.dirtyDomains[domain] = struct{}{}
			live.fullDomains[domain] = struct{}{}
			delta.mu.Lock()
			delta.dirtyMeta = true
			for nodeID := range delta.nodes {
				delta.markVectorNodeDirtyLocked(nodeID)
			}
			for id := range delta.currentNode {
				delta.markVectorDocDirtyLocked([]byte(id))
			}
			delta.mu.Unlock()
		}
	}
	return live, ""
}

// EnsureVectorPartitionLiveBindingV1 binds the ready immutable generation to
// the same registered VectorIndex that production collection reconciliation
// updates. It does not alter partition lifecycle authority.
func (c *Collection) EnsureVectorPartitionLiveBindingV1(ctx context.Context, manifest VectorPartitionManifestV1) error {
	return c.ensureVectorPartitionLiveBindingV1(ctx, manifest, nil)
}

func (idx *VectorIndex) vectorPartitionLiveBindingCurrentV1(manifest VectorPartitionManifestV1) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	return idx.sourceDocumentRootsValid && live != nil && !live.invalid && live.bindingDurable &&
		live.indexDefinitionDigest == manifest.IndexDefinitionDigest && live.source == vectorPartitionLiveSourceV1(manifest) &&
		live.generation == manifest.Generation && live.coverage == idx.sourceDocumentGeneration &&
		vectorPartitionLiveRoutingIdentityMatchesV1(live.packDomains, manifest)
}

func (c *Collection) vectorPartitionLiveWarmBindingCurrentV1(manifest VectorPartitionManifestV1) bool {
	if c == nil || c.db == nil {
		return false
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	if idx == nil || !idx.vectorPartitionLiveBindingCurrentV1(manifest) {
		return false
	}
	state, ok := c.db.StateToken()
	return ok && c.validateVectorPartitionLiveAuthorityStateV1(manifest.IndexName, manifest.Generation, state) == nil
}

func (c *Collection) vectorPartitionLiveCoordinatorBindingPinnedV1(manifest VectorPartitionManifestV1) bool {
	if c == nil {
		return false
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return false
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	key, ok := idx.vectorPartitionLiveSearchPinKeyV1(manifest)
	return ok && coord.hasPartitionLiveSearchPin(key)
}

func (c *Collection) validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(index string, generation, commitSeq, systemRoot uint64) error {
	if c == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	idx := c.registeredVectorIndex(index)
	if idx == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.RLock()
	live := idx.partitionLive
	if live == nil || live.invalid || !live.bindingDurable || live.generation != generation ||
		!idx.sourceDocumentRootsValid || !idx.sourceDocumentStateValid || live.coverage != idx.sourceDocumentGeneration ||
		idx.sourceDocumentState.CommitSeq != commitSeq || idx.sourceDocumentState.SystemRootPageID != systemRoot {
		idx.mu.RUnlock()
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	key := vectorPartitionLiveSearchPinKeyV1{
		index: index, generation: generation, revision: live.revision, coverage: live.coverage,
		commitSeq: commitSeq, systemRoot: systemRoot,
	}
	idx.mu.RUnlock()
	if !coord.hasPartitionLiveSearchPin(key) {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	return nil
}

func (idx *VectorIndex) vectorPartitionLiveSearchPinKeyV1(manifest VectorPartitionManifestV1) (vectorPartitionLiveSearchPinKeyV1, bool) {
	if idx == nil {
		return vectorPartitionLiveSearchPinKeyV1{}, false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	if live == nil || live.invalid || !live.bindingDurable || live.indexDefinitionDigest != manifest.IndexDefinitionDigest ||
		live.source != vectorPartitionLiveSourceV1(manifest) || live.generation != manifest.Generation ||
		!vectorPartitionLiveRoutingIdentityMatchesV1(live.packDomains, manifest) ||
		!idx.sourceDocumentRootsValid || !idx.sourceDocumentStateValid || live.coverage != idx.sourceDocumentGeneration {
		return vectorPartitionLiveSearchPinKeyV1{}, false
	}
	return vectorPartitionLiveSearchPinKeyV1{
		index: manifest.IndexName, generation: live.generation, revision: live.revision, coverage: live.coverage,
		commitSeq: idx.sourceDocumentState.CommitSeq, systemRoot: idx.sourceDocumentState.SystemRootPageID,
	}, true
}

func (c *Collection) validateCurrentVectorPartitionLiveBindingV1(ctx context.Context, manifest VectorPartitionManifestV1) error {
	currentGeneration, currentState, err := c.vectorPartitionLiveCurrentDocumentStateV1(ctx)
	if err != nil {
		return err
	}
	// The immutable source tuple remains bound to the manifest. A newer
	// ColumnGraph source is admissible only when this exact durable carrier
	// proves coverage of the coherent current collection document state.
	return c.validateAndRecordVectorPartitionLiveAuthorityStateV1(manifest, currentGeneration, currentState)
}

func (c *Collection) vectorPartitionLiveCurrentDocumentStateV1(ctx context.Context) (uint64, backenddb.StateToken, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, backenddb.StateToken{}, err
	}
	if c == nil || c.db == nil {
		return 0, backenddb.StateToken{}, ErrVectorIndexPartitionLiveUnavailableV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	documentGeneration, err := vectorIndexDocumentGenerationForCollection(snap, c.name)
	if err != nil {
		return 0, backenddb.StateToken{}, err
	}
	state, ok := snap.StateToken()
	if !ok {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	return documentGeneration, state, ctx.Err()
}

// loadVectorPartitionLiveIndexForServingV1 restores only a durable registered
// runtime. It never scans collection rows or invokes an automatic graph
// rebuild; missing, stale, or corrupt coverage is reported to the caller.
func (c *Collection) loadVectorPartitionLiveIndexForServingV1(def VectorIndexDefinition) (*VectorIndex, VectorIndexLoadStatus, error) {
	if idx := c.registeredVectorIndex(def.Name); idx != nil {
		if idx.validateNativeSnapshotDefinition(def) != "" || !idx.hasValidSourceDocumentRoots() {
			return nil, VectorIndexLoadStatus{ExactFallbackReason: vectorIndexFallbackStaleDocumentRoot}, ErrVectorIndexPartitionLiveMismatchV1
		}
		return idx, VectorIndexLoadStatus{Loaded: true}, nil
	}
	return c.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
}

// loadVectorPartitionLiveCarriersForReplayV1 restores checkpointed carriers
// before a later document command is applied. It is deliberately load-only:
// command replay must never scan collection rows or rebuild a ColumnGraph.
func (c *Collection) loadVectorPartitionLiveCarriersForReplayV1(catalog *collectionCatalog) error {
	if c == nil || catalog == nil {
		return nil
	}
	unlockLoad := c.lockNativeVectorIndexLoad()
	defer unlockLoad()
	for _, def := range catalog.meta.VectorIndexes {
		if def.Strategy != VectorIndexStrategyColumnGraph || c.registeredVectorIndex(def.Name) != nil {
			continue
		}
		rootName := collectionVectorIndexRootName(catalog.meta.Name, def.Name)
		if catalog.rootID(rootName) == 0 && len(catalog.overlayRootIDs(rootName)) == 0 {
			continue
		}
		index, status, err := c.loadVectorPartitionLiveIndexForServingV1(def)
		if err != nil {
			return err
		}
		if index == nil || !index.isPartitionLiveCarrier() {
			return fmt.Errorf("%w: partition live carrier %q replay load failed: %s", ErrVectorIndexPartitionLiveUnavailableV1, def.Name, status.ExactFallbackReason)
		}
	}
	return nil
}

func (c *Collection) vectorPartitionLiveCurrentStateV1(ctx context.Context, index string) (VectorPartitionSourceIdentityV1, uint64, backenddb.StateToken, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionSourceIdentityV1{}, 0, backenddb.StateToken{}, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorPartitionSourceIdentityV1{}, 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	source, err := c.vectorPartitionSourceIdentityAtSnapshotV1(index, snap)
	if err != nil {
		return VectorPartitionSourceIdentityV1{}, 0, backenddb.StateToken{}, err
	}
	documentGeneration, err := vectorIndexDocumentGenerationForCollection(snap, c.name)
	if err != nil {
		return VectorPartitionSourceIdentityV1{}, 0, backenddb.StateToken{}, err
	}
	state, ok := snap.StateToken()
	if !ok {
		return VectorPartitionSourceIdentityV1{}, 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	return source, documentGeneration, state, ctx.Err()
}

func (c *Collection) ensureVectorPartitionLiveBindingV1(ctx context.Context, manifest VectorPartitionManifestV1, replay *backenddb.CommandWALIntent) error {
	if c == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	if replay == nil && c.vectorPartitionLiveWarmBindingCurrentV1(manifest) {
		return nil
	}
	if replay == nil && c.vectorPartitionLiveCoordinatorBindingPinnedV1(manifest) {
		return nil
	}

	// Mutations take native-vector admission before loading/reconciling runtime
	// state. Use the same order and hold exclusive admission until a new binding
	// is durably published and marked admitted.
	unlockAdmission := c.lockNativeVectorAdmissionWrite()
	defer unlockAdmission()
	unlockLoad := c.lockNativeVectorIndexLoad()
	if replay == nil && c.vectorPartitionLiveWarmBindingCurrentV1(manifest) {
		unlockLoad()
		return nil
	}

	idx, needsBindingPublication, err := func() (*VectorIndex, bool, error) {
		unlockMutation := c.lockMutation()
		defer unlockMutation.Unlock()
		if idx := c.registeredVectorIndex(manifest.IndexName); idx != nil && idx.vectorPartitionLiveBindingCurrentV1(manifest) {
			if err := c.validateCurrentVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
				return nil, false, err
			}
			return idx, replay != nil, nil
		}

		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return nil, false, backenddb.ErrClosed
		}
		catalog, err := c.catalogForSnapshot(snap)
		if err != nil {
			_ = snap.Close()
			return nil, false, err
		}
		if catalog == nil {
			_ = snap.Close()
			return nil, false, errCollectionNotFound
		}
		def, ok := findVectorIndex(catalog.meta.VectorIndexes, manifest.IndexName)
		closeErr := snap.Close()
		if closeErr != nil {
			return nil, false, closeErr
		}
		if !ok || VectorIndexDefinitionDigestV1(def) != manifest.IndexDefinitionDigest {
			return nil, false, ErrVectorIndexPartitionLiveMismatchV1
		}
		idx, loadStatus, err := c.loadVectorPartitionLiveIndexForServingV1(def)
		if err != nil {
			return nil, false, err
		}
		if idx != nil && idx.vectorPartitionLiveBindingCurrentV1(manifest) {
			if err := c.validateCurrentVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
				return nil, false, err
			}
			return idx, replay != nil, nil
		}

		currentSource, currentGeneration, currentState, err := c.vectorPartitionLiveCurrentStateV1(ctx, manifest.IndexName)
		if err != nil {
			return nil, false, err
		}
		if currentSource != vectorPartitionLiveSourceV1(manifest) {
			return nil, false, fmt.Errorf("%w: immutable source identity is not current", ErrVectorIndexPartitionLiveMismatchV1)
		}
		if idx == nil {
			if def.Strategy != VectorIndexStrategyColumnGraph || loadStatus.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot {
				return nil, false, fmt.Errorf("%w: durable live carrier load failed: %s", ErrVectorIndexPartitionLiveUnavailableV1, loadStatus.ExactFallbackReason)
			}
			idx, err = newVectorIndex(c, vectorIndexOptionsFromDefinition(def))
			if err != nil {
				return nil, false, err
			}
			idx.setPartitionLiveCarrier(true)
			idx.recordSourceDocumentState(currentGeneration, currentState)
		} else {
			idx.mu.RLock()
			coverageCurrent := idx.sourceDocumentRootsValid && idx.sourceDocumentGeneration == currentGeneration
			idx.mu.RUnlock()
			if !coverageCurrent {
				return nil, false, ErrVectorIndexPartitionLiveMismatchV1
			}
		}

		router, status, err := c.OpenPreparedVectorPartitionRouterForGenerationWithContextV1(ctx, manifest.IndexName, manifest.Generation)
		if err != nil {
			return nil, false, err
		}
		defer router.Close()
		if status.Generation != manifest.Generation {
			return nil, false, ErrVectorIndexPartitionLiveMismatchV1
		}
		if !vectorPartitionLiveManifestRoutingIdentityMatchesV1(router.manifest, manifest) {
			return nil, false, ErrVectorIndexPartitionLiveMismatchV1
		}
		representatives, err := router.partitionLiveRepresentativesV1()
		if err != nil {
			return nil, false, err
		}
		coord := c.collectionSchemaCoordinator()
		if coord == nil {
			return nil, false, ErrVectorIndexPartitionLiveUnavailableV1
		}
		runVectorPartitionLiveBeforeBindingTransitionHookForTest()
		coord.partitionLivePublishMu.Lock()
		err = idx.bindVectorPartitionLiveV1(manifest, currentGeneration, representatives)
		coord.partitionLivePublishMu.Unlock()
		if err != nil {
			return nil, false, err
		}
		idx.mu.RLock()
		live := idx.partitionLive
		needsPublication := live != nil && !live.bindingDurable
		idx.mu.RUnlock()
		return idx, needsPublication, nil
	}()
	// Collection mutations acquire admission before loading registered vector
	// runtimes. Never hold the load mutex while durable publication acquires
	// admission, or concurrent first binding and mutation can deadlock.
	unlockLoad()
	if err != nil {
		return err
	}
	if needsBindingPublication {
		rollbackRegistration := c.registeredVectorIndex(manifest.IndexName) == nil
		if rollbackRegistration {
			c.registerVectorIndexCurrentCatalog(idx)
			defer func() {
				if rollbackRegistration && c.registeredVectorIndex(manifest.IndexName) == idx {
					c.UnregisterVectorIndex(manifest.IndexName)
				}
			}()
		}
		runVectorPartitionLiveBeforeBindingPublicationHookForTest()
		_, err := idx.saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntent(replay)
		if err != nil {
			return err
		}
		unlockLoad = c.lockNativeVectorIndexLoad()
		defer unlockLoad()
		rootID, err := c.currentNativeVectorIndexRootID(manifest.IndexName)
		if err != nil {
			return err
		}
		idx.mu.Lock()
		live := idx.partitionLive
		dirty := idx.dirtyMeta || (idx.mutationSeq != 0 && (idx.persistedEpoch == 0 || idx.persistedSnapshotDirty))
		if rootID == 0 || idx.persistedEpoch != rootID || dirty ||
			live == nil || live.invalid || live.indexDefinitionDigest != manifest.IndexDefinitionDigest || live.source != vectorPartitionLiveSourceV1(manifest) || live.generation != manifest.Generation || live.coverage != idx.sourceDocumentGeneration ||
			!vectorPartitionLiveRoutingIdentityMatchesV1(live.packDomains, manifest) {
			idx.mu.Unlock()
			return fmt.Errorf("%w: durable binding publication did not commit", ErrVectorIndexPartitionLiveUnavailableV1)
		}
		live.bindingDurable = true
		idx.mu.Unlock()
		rollbackRegistration = false
	}
	return nil
}

func (c *Collection) validateAndRecordVectorPartitionLiveAuthorityStateV1(manifest VectorPartitionManifestV1, currentGeneration uint64, state backenddb.StateToken) error {
	if c == nil || currentGeneration == 0 {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	if idx == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	live := idx.partitionLive
	if live == nil || live.invalid || !live.bindingDurable ||
		live.indexDefinitionDigest != manifest.IndexDefinitionDigest || live.source != vectorPartitionLiveSourceV1(manifest) ||
		live.generation != manifest.Generation || live.coverage != currentGeneration ||
		!idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != currentGeneration {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	// Coverage was validated before this process-local authority cache update;
	// never advance the durable live revision or coverage here.
	idx.sourceDocumentState = state
	idx.sourceDocumentStateValid = true
	return nil
}

func (c *Collection) validateVectorPartitionLiveAuthorityStateV1(index string, generation uint64, state backenddb.StateToken) error {
	if c == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx := c.registeredVectorIndex(index)
	if idx == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	if live == nil || live.invalid || !live.bindingDurable || live.generation != generation ||
		!idx.sourceDocumentRootsValid || !idx.sourceDocumentStateValid || live.coverage != idx.sourceDocumentGeneration ||
		idx.sourceDocumentState != state {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	return nil
}

func (c *Collection) AcquireVectorPartitionLiveSearchPinV1(manifest VectorPartitionManifestV1) (*VectorIndexPartitionLiveSearchPinV1, error) {
	if c == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	if idx == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	return idx.acquireVectorPartitionLiveSearchPinV1(manifest)
}

// AcquireVectorPartitionLiveCoordinatorSearchPinV1 holds publication at one
// live identity through coordinator planning and every local shard dispatch.
// Shards acquire ordinary pins while this pin is held; they must not queue a
// second RWMutex reader behind a waiting writer because that would deadlock the
// coordinator waiting for the shard.
func (c *Collection) AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest VectorPartitionManifestV1) (*VectorIndexPartitionLiveSearchPinV1, error) {
	if c == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord.partitionLivePublishMu.RLock()
	pin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		coord.partitionLivePublishMu.RUnlock()
		return nil, err
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	key, ok := idx.vectorPartitionLiveSearchPinKeyV1(manifest)
	if !ok || key.revision != pin.status.Revision || key.coverage != pin.status.Coverage {
		pin.Release()
		coord.partitionLivePublishMu.RUnlock()
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	coord.registerPartitionLiveSearchPin(key)
	pin.releasePublication = func() {
		coord.unregisterPartitionLiveSearchPin(key)
		coord.partitionLivePublishMu.RUnlock()
	}
	return pin, nil
}

func (p *VectorIndexPartitionLiveSearchPinV1) Release() {
	if p == nil {
		return
	}
	p.releaseOnce.Do(func() {
		for domain, pin := range p.domains {
			if pin.view != nil {
				pin.index.releaseSearchView(pin.view)
			}
			delete(p.domains, domain)
		}
		p.excludedStableIDs = nil
		if p.releasePublication != nil {
			p.releasePublication()
			p.releasePublication = nil
		}
	})
}
func (p *VectorIndexPartitionLiveSearchPinV1) StatusV1() VectorIndexPartitionLiveStatusV1 {
	if p == nil {
		return VectorIndexPartitionLiveStatusV1{}
	}
	return p.status
}
func (p *VectorIndexPartitionLiveSearchPinV1) ExcludesBaseIDV1(id string) bool {
	if p == nil {
		return false
	}
	_, ok := p.excludedStableIDs[id]
	return ok
}
func (p *VectorIndexPartitionLiveSearchPinV1) ExcludedStableIDsV1() map[string]struct{} {
	if p == nil {
		return nil
	}
	// The pin owns this immutable set. Callers may read it until Release but
	// must not mutate or retain it beyond the pin lifetime.
	return p.excludedStableIDs
}
func (p *VectorIndexPartitionLiveSearchPinV1) DomainForPackV1(pack uint32) (uint32, bool) {
	if p == nil || int(pack) >= len(p.packDomains) {
		return 0, false
	}
	return p.packDomains[pack], true
}
func (p *VectorIndexPartitionLiveSearchPinV1) DomainDeltaCountV1(domain uint32) int {
	if p == nil {
		return 0
	}
	pin, ok := p.domains[domain]
	if !ok || pin.view == nil {
		return 0
	}
	return pin.view.liveDocs
}

func (p *VectorIndexPartitionLiveSearchPinV1) DomainSearchPreflightV1(domain uint32, opts VectorPartitionSearchOptionsV1) (uint64, uint64, error) {
	if p == nil || opts.TopK <= 0 || opts.EfSearch <= 0 || opts.MaxScoreCalls < 0 || opts.MaxStableIDBytes <= 0 {
		return 0, 0, ErrVectorIndexPartitionLiveUnavailableV1
	}
	pin, ok := p.domains[domain]
	if !ok || pin.view == nil || pin.view.liveDocs == 0 {
		return 0, 0, nil
	}
	if pin.maxStableIDBytes > opts.MaxStableIDBytes {
		return 0, 0, fmt.Errorf("%w: stable ID bytes=%d exceeds limit=%d", ErrVectorIndexPartitionLiveUnavailableV1, pin.maxStableIDBytes, opts.MaxStableIDBytes)
	}
	nodes := uint64(len(pin.view.nodes) + len(pin.view.deltaNodes))
	if nodes > vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return 0, 0, ErrVectorIndexPartitionLiveCapacityV1
	}
	// The live-delta ANN scratch consists of visited ordinals and bounded
	// candidate heaps. Account a conservative 64 bytes for every allocated
	// node plus the largest possible returned stable IDs before doing work.
	if nodes > ^uint64(0)/64 {
		return 0, 0, ErrVectorIndexPartitionLiveCapacityV1
	}
	scratch := nodes * 64
	resultRows := uint64(minInt(opts.TopK, pin.view.liveDocs))
	resultBytes := resultRows * uint64(opts.MaxStableIDBytes)
	if resultBytes/resultRows != uint64(opts.MaxStableIDBytes) || scratch > ^uint64(0)-resultBytes {
		return 0, 0, ErrVectorIndexPartitionLiveCapacityV1
	}
	return nodes, scratch + resultBytes, nil
}

func (p *VectorIndexPartitionLiveSearchPinV1) SearchDomainV1(ctx context.Context, domain uint32, query []float32, opts VectorPartitionSearchOptionsV1) ([]VectorPartitionSearchResultV1, VectorPartitionSearchMetricsV1, error) {
	if p == nil || opts.MaxScoreCalls < 0 {
		return nil, VectorPartitionSearchMetricsV1{}, ErrVectorIndexPartitionLiveUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	pinned, ok := p.domains[domain]
	if !ok || pinned.view == nil || pinned.view.liveDocs == 0 {
		return nil, VectorPartitionSearchMetricsV1{Route: VectorPartitionSearchRouteHNSWSearchPackV1}, nil
	}
	if opts.MaxStableIDBytes > 0 && pinned.maxStableIDBytes > opts.MaxStableIDBytes {
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: stable ID bytes=%d exceeds limit=%d", ErrVectorIndexPartitionLiveUnavailableV1, pinned.maxStableIDBytes, opts.MaxStableIDBytes)
	}
	var buffer VectorIndexSearchBuffer
	buffer.nativeSearchWorkEnabled = true
	buffer.nativeSearchScratch.context = ctx
	defer func() { buffer.nativeSearchScratch.context = nil }()
	results, err := pinned.view.searchGraphOnlyWithScoreBudget(query, opts.TopK, opts.EfSearch, opts.MaxScoreCalls, &buffer)
	if err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	out := make([]VectorPartitionSearchResultV1, len(results))
	for i, result := range results {
		out[i] = VectorPartitionSearchResultV1{ID: string(result.ID), Score: float32(result.Score)}
	}
	candidates := uint64(buffer.nativeSearchWork.baseVisited + buffer.nativeSearchWork.deltaVisited)
	return out, VectorPartitionSearchMetricsV1{ScoreCalls: uint64(buffer.nativeSearchWork.scoreCalls), Candidates: candidates, Route: VectorPartitionSearchRouteHNSWSearchPackV1}, nil
}
