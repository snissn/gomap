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
	Version               int                                       `json:"version"`
	IndexDefinitionDigest string                                    `json:"index_definition_digest"`
	Source                VectorPartitionSourceIdentityV1           `json:"source"`
	Generation            uint64                                    `json:"generation"`
	Revision              uint64                                    `json:"revision"`
	Coverage              uint64                                    `json:"coverage"`
	Cutovers              uint64                                    `json:"cutovers"`
	PackDomains           []uint32                                  `json:"pack_domains"`
	Representatives       []vectorIndexPartitionLivePersistRepV1    `json:"representatives"`
	Owners                []vectorIndexPartitionLivePersistOwnerV1  `json:"owners"`
	Domains               []vectorIndexPartitionLivePersistDomainV1 `json:"domains"`
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
	Snapshot vectorIndexPersistSnapshot `json:"snapshot"`
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

func (idx *VectorIndex) reconcileVectorPartitionMutationLocked(documentID []byte, vector []float32) error {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil
	}
	key := string(documentID)
	old, existed := live.owners[key]
	if !existed && len(live.owners) >= vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if vector == nil {
		if existed && !old.deleted {
			if delta := live.domains[old.domain]; delta != nil {
				delta.mu.Lock()
				delta.tombstoneDocumentIDLocked(documentID)
				delta.publishSearchViewLocked(false)
				delta.mu.Unlock()
			}
		}
		live.owners[key] = vectorPartitionLiveOwnerV1{deleted: true}
	} else {
		domain, err := idx.partitionLiveRouteLocked(vector)
		if err != nil {
			return err
		}
		delta := live.domains[domain]
		newDomain := delta == nil
		if delta == nil {
			delta, err = idx.newPartitionLiveDomainIndexV1()
			if err != nil {
				return err
			}
		}
		delta.mu.Lock()
		beforeNodes := len(delta.nodes)
		err = delta.insertVectorLocked(documentID, vector)
		if err == nil {
			delta.acknowledgeSearchViewStateLocked()
			delta.publishSearchViewLocked(false)
		}
		afterNodes := len(delta.nodes)
		delta.mu.Unlock()
		if err != nil {
			return err
		}
		if newDomain {
			live.domains[domain] = delta
		}
		if existed && !old.deleted && old.domain != domain {
			if oldDelta := live.domains[old.domain]; oldDelta != nil {
				oldDelta.mu.Lock()
				oldDelta.tombstoneDocumentIDLocked(documentID)
				oldDelta.publishSearchViewLocked(false)
				oldDelta.mu.Unlock()
			}
		}
		live.owners[key] = vectorPartitionLiveOwnerV1{domain: domain}
		if afterNodes > beforeNodes {
			live.nodeIDBytes += uint64(len(documentID))
		}
	}
	if !existed {
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
	if additionalNodes > math.MaxInt-nodes || additionalOwners > math.MaxInt-len(live.owners) {
		return false
	}
	total := uint64(1024 + len(live.packDomains)*(4+128) + (len(live.owners)+additionalOwners)*128)
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
	for _, owner := range idx.partitionLive.owners {
		if !owner.deleted {
			active++
		}
	}
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
	ownerIDs := make([]string, 0, len(live.owners))
	for id, owner := range live.owners {
		if !owner.deleted {
			ownerIDs = append(ownerIDs, id)
		}
	}
	sort.Strings(ownerIDs)
	fresh := make(map[uint32]*VectorIndex, len(live.domains))
	for _, id := range ownerIDs {
		owner := live.owners[id]
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
	for _, domain := range domains {
		delta := fresh[domain]
		delta.mu.Lock()
		delta.acknowledgeSearchViewStateLocked()
		delta.publishSearchViewLocked(true)
		delta.mu.Unlock()
		live.cutoverPublications++
	}
	live.domains = fresh
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
	_, exists := idx.partitionLive.owners[string(documentID)]
	if !exists && len(idx.partitionLive.owners) >= vectorIndexPartitionLiveMaxMutatedIDsV1 {
		return ErrVectorIndexPartitionLiveCapacityV1
	}
	if vector != nil {
		domain, err := idx.partitionLiveRouteLocked(vector)
		if err != nil {
			return err
		}
		additionalNodes := 1
		additionalNodeIDBytes := uint64(len(documentID))
		if owner, ok := idx.partitionLive.owners[string(documentID)]; ok && !owner.deleted && owner.domain == domain {
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
	owners := len(idx.partitionLive.owners)
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
		if _, exists := idx.partitionLive.owners[key]; !exists {
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
			if current, ok := idx.partitionLive.owners[key]; ok {
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
		status:            VectorIndexPartitionLiveStatusV1{Generation: live.generation, Revision: live.revision, Coverage: live.coverage, MutatedIDs: len(live.owners), Cutovers: live.cutovers},
		packDomains:       append([]uint32(nil), live.packDomains...),
		excludedStableIDs: make(map[string]struct{}, len(live.owners)),
		domains:           make(map[uint32]vectorIndexPartitionLiveDomainPinV1, len(live.domains)),
	}
	maxStableIDBytes := make(map[uint32]int, len(live.domains))
	for id, owner := range live.owners {
		pin.excludedStableIDs[id] = struct{}{}
		if !owner.deleted {
			pin.status.LiveIDs++
			maxStableIDBytes[owner.domain] = maxInt(maxStableIDBytes[owner.domain], len(id))
		}
	}
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

func (idx *VectorIndex) partitionLivePersistLocked() *vectorIndexPartitionLivePersistV1 {
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil
	}
	persisted := &vectorIndexPartitionLivePersistV1{
		Version: 1, IndexDefinitionDigest: live.indexDefinitionDigest, Source: live.source,
		Generation: live.generation, Revision: live.revision, Coverage: live.coverage, Cutovers: live.cutovers,
		PackDomains:     append([]uint32(nil), live.packDomains...),
		Representatives: make([]vectorIndexPartitionLivePersistRepV1, len(live.representatives)),
		Owners:          make([]vectorIndexPartitionLivePersistOwnerV1, 0, len(live.owners)),
		Domains:         make([]vectorIndexPartitionLivePersistDomainV1, 0, len(live.domains)),
	}
	for i, rep := range live.representatives {
		persisted.Representatives[i] = vectorIndexPartitionLivePersistRepV1{Domain: rep.domain, Vector: append([]float32(nil), rep.vector...)}
	}
	for id, owner := range live.owners {
		persisted.Owners = append(persisted.Owners, vectorIndexPartitionLivePersistOwnerV1{ID: id, Domain: owner.domain, Deleted: owner.deleted})
	}
	sort.Slice(persisted.Owners, func(i, j int) bool { return persisted.Owners[i].ID < persisted.Owners[j].ID })
	domains := make([]uint32, 0, len(live.domains))
	for domain := range live.domains {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	for _, domain := range domains {
		snapshot, _ := live.domains[domain].persistSnapshot()
		persisted.Domains = append(persisted.Domains, vectorIndexPartitionLivePersistDomainV1{Domain: domain, Snapshot: snapshot})
	}
	return persisted
}

func (idx *VectorIndex) restorePartitionLiveV1(persisted *vectorIndexPartitionLivePersistV1, sourceCoverage uint64) (*vectorIndexPartitionLiveStateV1, string) {
	if persisted == nil {
		return nil, ""
	}
	if persisted.Version != 1 || persisted.IndexDefinitionDigest == "" || persisted.Generation == 0 || persisted.Source.Generation == 0 || persisted.Coverage != sourceCoverage || len(persisted.PackDomains) == 0 || len(persisted.Representatives) == 0 || len(persisted.Owners) > vectorIndexPartitionLiveMaxMutatedIDsV1 || !idx.vectorPartitionLivePersistBytesFitV1(persisted) {
		return nil, "invalid_partition_live_meta"
	}
	mappedDomains := make(map[uint32]struct{}, len(persisted.PackDomains))
	for _, domain := range persisted.PackDomains {
		mappedDomains[domain] = struct{}{}
	}
	live := &vectorIndexPartitionLiveStateV1{indexDefinitionDigest: persisted.IndexDefinitionDigest, source: persisted.Source, generation: persisted.Generation, revision: persisted.Revision, coverage: persisted.Coverage, cutovers: persisted.Cutovers, nodeCapacity: vectorIndexPartitionLiveMaxMutatedIDsV1, byteCapacity: vectorIndexPartitionLiveMaxBytesV1, bindingDurable: true, packDomains: append([]uint32(nil), persisted.PackDomains...), representatives: make([]vectorPartitionLiveRepresentativeV1, len(persisted.Representatives)), domains: make(map[uint32]*VectorIndex), owners: make(map[string]vectorPartitionLiveOwnerV1, len(persisted.Owners))}
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
	for _, domain := range persisted.Domains {
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
		for _, node := range domain.Snapshot.Nodes {
			live.nodeIDBytes += uint64(len(node.DocumentID))
		}
		live.domains[domain.Domain] = delta
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
	for i, owner := range persisted.Owners {
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
	if p == nil || opts.TopK <= 0 || opts.EfSearch <= 0 || opts.MaxStableIDBytes <= 0 {
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
	if p == nil {
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
	results, err := pinned.view.searchGraphOnlyWithBuffer(query, opts.TopK, opts.EfSearch, &buffer)
	if err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	out := make([]VectorPartitionSearchResultV1, len(results))
	for i, result := range results {
		out[i] = VectorPartitionSearchResultV1{ID: string(result.ID), Score: float32(result.Score)}
	}
	candidates := uint64(buffer.nativeSearchWork.baseVisited + buffer.nativeSearchWork.deltaVisited)
	return out, VectorPartitionSearchMetricsV1{Candidates: candidates, Route: VectorPartitionSearchRouteHNSWSearchPackV1}, nil
}
