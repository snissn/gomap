package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"hash/fnv"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type stableResourceEntry struct {
	token                *StableResourceToken
	pins                 []*StableResourceToken
	pinIndex             *stableTable[*StableResourceToken, struct{}]
	logicalLane          string
	resourceID           string
	diagnosticPath       string
	frontier             DurableFrontier
	reachability         stableReachabilitySet
	logicalObligations   stableLogicalObligationView
	dependencyManifestV1 *dependencyManifestEntryCacheV1
}

type dependencyManifestEntryCacheV1 struct {
	mu    sync.Mutex
	value *dependencyManifestEncodedEntryV1
}

// stableLogicalObligationView is an immutable persistent sequence. Appending a
// certified mutation adds one small node while older visible/durable roots keep
// their exact prefix. Destructive transitions materialize and rebuild through
// the ordinary exact filter path.
type stableLogicalObligationView struct {
	tail        *stableLogicalObligationNode
	count       int
	index       *stableLogicalObligationIndexNode
	commitments map[ReachabilityField]stableLogicalObligationCommitment
	directory   *DependencyDirectoryV2
	owner       []byte
	removed     map[stableLogicalObligationIndex]StableLogicalObligation
}

// stableLogicalObligationCommitment is an order-independent cryptographic
// multiset commitment. Count distinguishes multiplicity and sum is addition
// modulo 2^256 of hashes that bind every obligation field. The pair supports
// exact mutation-local add/remove checks without materializing retained
// obligation history. Hash collisions remain the sole probabilistic boundary.
type stableLogicalObligationCommitment struct {
	count uint64
	sum   [sha256.Size]byte
}

func stableLogicalObligationHash(obligation StableLogicalObligation) [sha256.Size]byte {
	h := sha256.New()
	writeString := func(value string) {
		var size [8]byte
		binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	writeUint64 := func(value uint64) {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], value)
		_, _ = h.Write(raw[:])
	}
	writeString(obligation.Class)
	writeString(obligation.Kind)
	writeString(obligation.Namespace)
	writeUint64(obligation.Generation)
	writeUint64(obligation.PartID)
	writeUint64(obligation.FileID)
	writeUint64(uint64(obligation.Offset))
	writeUint64(uint64(obligation.Length))
	writeUint64(uint64(obligation.Checksum))
	writeString(string(obligation.Reachability))
	_, _ = h.Write(obligation.Digest[:])
	var result [sha256.Size]byte
	h.Sum(result[:0])
	return result
}

func addStableLogicalObligationDigest(target *[sha256.Size]byte, value [sha256.Size]byte) {
	carry := uint16(0)
	for i := len(target) - 1; i >= 0; i-- {
		sum := uint16(target[i]) + uint16(value[i]) + carry
		target[i] = byte(sum)
		carry = sum >> 8
	}
}

func subtractStableLogicalObligationDigest(target *[sha256.Size]byte, value [sha256.Size]byte) {
	borrow := int16(0)
	for i := len(target) - 1; i >= 0; i-- {
		difference := int16(target[i]) - int16(value[i]) - borrow
		if difference < 0 {
			difference += 1 << 8
			borrow = 1
		} else {
			borrow = 0
		}
		target[i] = byte(difference)
	}
}

func (commitment *stableLogicalObligationCommitment) addObligation(obligation StableLogicalObligation) {
	commitment.count++
	addStableLogicalObligationDigest(&commitment.sum, stableLogicalObligationHash(obligation))
}

func (commitment *stableLogicalObligationCommitment) removeObligation(obligation StableLogicalObligation) bool {
	if commitment.count == 0 {
		return false
	}
	commitment.count--
	subtractStableLogicalObligationDigest(&commitment.sum, stableLogicalObligationHash(obligation))
	return true
}

func (commitment *stableLogicalObligationCommitment) add(other stableLogicalObligationCommitment) {
	commitment.count += other.count
	addStableLogicalObligationDigest(&commitment.sum, other.sum)
}

func cloneStableLogicalObligationCommitments(source map[ReachabilityField]stableLogicalObligationCommitment) map[ReachabilityField]stableLogicalObligationCommitment {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[ReachabilityField]stableLogicalObligationCommitment, len(source))
	for field, commitment := range source {
		clone[field] = commitment
	}
	return clone
}

func addStableLogicalObligationCommitments(left, right map[ReachabilityField]stableLogicalObligationCommitment) map[ReachabilityField]stableLogicalObligationCommitment {
	result := cloneStableLogicalObligationCommitments(left)
	if result == nil && len(right) != 0 {
		result = make(map[ReachabilityField]stableLogicalObligationCommitment, len(right))
	}
	for field, other := range right {
		commitment := result[field]
		commitment.add(other)
		result[field] = commitment
	}
	return result
}

func stableLogicalObligationCommitments(values []StableLogicalObligation) map[ReachabilityField]stableLogicalObligationCommitment {
	if len(values) == 0 {
		return nil
	}
	commitments := make(map[ReachabilityField]stableLogicalObligationCommitment)
	for _, obligation := range values {
		commitment := commitments[obligation.Reachability]
		commitment.addObligation(obligation)
		commitments[obligation.Reachability] = commitment
	}
	return commitments
}

func stableLogicalObligationRequirementCommitments(fields []ReachabilityField, values []StableLogicalObligation) map[ReachabilityField]stableLogicalObligationCommitment {
	commitments := make(map[ReachabilityField]stableLogicalObligationCommitment, len(fields))
	for _, field := range fields {
		commitments[field] = stableLogicalObligationCommitment{}
	}
	for _, obligation := range values {
		commitment := commitments[obligation.Reachability]
		commitment.addObligation(obligation)
		commitments[obligation.Reachability] = commitment
	}
	return commitments
}

func stableLogicalObligationCommitmentCount(commitments map[ReachabilityField]stableLogicalObligationCommitment) (uint64, bool) {
	var total uint64
	for _, commitment := range commitments {
		if ^uint64(0)-total < commitment.count {
			return 0, false
		}
		total += commitment.count
	}
	return total, true
}

type stableLogicalObligationNode struct {
	parent *stableLogicalObligationNode
	values []StableLogicalObligation
}

type stableLogicalObligationIndexNode struct {
	key        stableLogicalObligationIndex
	obligation StableLogicalObligation
	priority   uint64
	left       *stableLogicalObligationIndexNode
	right      *stableLogicalObligationIndexNode
}

func findStableLogicalObligationIndex(root *stableLogicalObligationIndexNode, obligation StableLogicalObligation, work *StableResourceClosureWork) (StableLogicalObligation, bool) {
	key := stableLogicalObligationKey(obligation)
	if work != nil {
		work.AggregateMembershipProbes++
	}
	for root != nil {
		if work != nil {
			work.AggregateMembershipNodeVisits++
		}
		if key == root.key {
			return root.obligation, true
		}
		if stableLogicalObligationIndexLess(key, root.key) {
			root = root.left
		} else {
			root = root.right
		}
	}
	return StableLogicalObligation{}, false
}

func newStableLogicalObligationView(values []StableLogicalObligation) stableLogicalObligationView {
	return newStableLogicalObligationViewWithWork(values, nil)
}

func newStableLogicalObligationViewWithWork(values []StableLogicalObligation, work *StableResourceClosureWork) stableLogicalObligationView {
	if len(values) == 0 {
		return stableLogicalObligationView{}
	}
	var index *stableLogicalObligationIndexNode
	for _, obligation := range values {
		var err error
		index, err = insertFreshStableLogicalObligationIndex(index, obligation, work)
		if err != nil {
			// Values reaching this constructor were normalized or already held
			// by an exact resource closure. A conflict here is an internal
			// invariant violation rather than recoverable producer input.
			panic(err)
		}
	}
	return stableLogicalObligationView{
		tail:        &stableLogicalObligationNode{values: stableLogicalObligationList(values)},
		count:       len(values),
		index:       index,
		commitments: stableLogicalObligationCommitments(values),
	}
}

func (view stableLogicalObligationView) appendCertified(values []StableLogicalObligation, work *StableResourceClosureWork) (stableLogicalObligationView, error) {
	if len(values) == 0 {
		return view, nil
	}
	if view.directory != nil {
		return view.appendDirectoryDelta(values, work)
	}
	if view.index == nil && view.count != 0 {
		view = newStableLogicalObligationView(view.deltaSlice())
	}
	index := view.index
	var added []StableLogicalObligation
	for i, obligation := range values {
		nextIndex, err := insertStableLogicalObligationIndex(index, obligation, work)
		if err != nil {
			return stableLogicalObligationView{}, err
		}
		if nextIndex == index {
			if added == nil {
				added = append(make([]StableLogicalObligation, 0, len(values)-1), values[:i]...)
			}
			continue
		}
		index = nextIndex
		if added != nil {
			added = append(added, obligation)
		}
	}
	if added != nil {
		values = added
	}
	if len(values) == 0 {
		return view, nil
	}
	commitments := cloneStableLogicalObligationCommitments(view.commitments)
	if commitments == nil {
		commitments = make(map[ReachabilityField]stableLogicalObligationCommitment)
	}
	for _, obligation := range values {
		commitment := commitments[obligation.Reachability]
		commitment.addObligation(obligation)
		commitments[obligation.Reachability] = commitment
	}
	next := stableLogicalObligationView{
		tail:        &stableLogicalObligationNode{parent: view.tail, values: stableLogicalObligationList(values)},
		count:       view.count + len(values),
		index:       index,
		commitments: commitments,
	}
	return next, nil
}

func stableLogicalObligationIndexLess(left, right stableLogicalObligationIndex) bool {
	if left.class != right.class {
		return left.class < right.class
	}
	if left.kind != right.kind {
		return left.kind < right.kind
	}
	if left.namespace != right.namespace {
		return left.namespace < right.namespace
	}
	if left.generation != right.generation {
		return left.generation < right.generation
	}
	if left.partID != right.partID {
		return left.partID < right.partID
	}
	if left.fileID != right.fileID {
		return left.fileID < right.fileID
	}
	if left.offset != right.offset {
		return left.offset < right.offset
	}
	if left.length != right.length {
		return left.length < right.length
	}
	return left.reachability < right.reachability
}

func stableLogicalObligationPriority(key stableLogicalObligationIndex) uint64 {
	h := fnv.New64a()
	writeString := func(value string) {
		var size [8]byte
		binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	writeUint64 := func(value uint64) {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], value)
		_, _ = h.Write(raw[:])
	}
	writeString(key.class)
	writeString(key.kind)
	writeString(key.namespace)
	writeUint64(key.generation)
	writeUint64(key.partID)
	writeUint64(key.fileID)
	writeUint64(uint64(key.offset))
	writeUint64(uint64(key.length))
	writeString(string(key.reachability))
	return h.Sum64()
}

func insertStableLogicalObligationIndex(root *stableLogicalObligationIndexNode, obligation StableLogicalObligation, work *StableResourceClosureWork) (*stableLogicalObligationIndexNode, error) {
	key := stableLogicalObligationKey(obligation)
	if root == nil {
		if work != nil {
			work.LogicalIndexNodesAdmitted++
		}
		return &stableLogicalObligationIndexNode{key: key, obligation: obligation, priority: stableLogicalObligationPriority(key)}, nil
	}
	if work != nil {
		work.RetainedIndexNodeVisits++
	}
	if key == root.key {
		if root.obligation != obligation {
			return nil, fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
		}
		return root, nil
	}
	if stableLogicalObligationIndexLess(key, root.key) {
		child, err := insertStableLogicalObligationIndex(root.left, obligation, work)
		if err != nil {
			return nil, err
		}
		if child == root.left {
			return root, nil
		}
		next := *root
		if work != nil {
			work.RetainedIndexNodeCopies++
		}
		next.left = child
		result := &next
		if child.priority < result.priority {
			promoted := *child
			result.left = promoted.right
			promoted.right = result
			return &promoted, nil
		}
		return result, nil
	}
	child, err := insertStableLogicalObligationIndex(root.right, obligation, work)
	if err != nil {
		return nil, err
	}
	if child == root.right {
		return root, nil
	}
	next := *root
	if work != nil {
		work.RetainedIndexNodeCopies++
	}
	next.right = child
	result := &next
	if child.priority < result.priority {
		promoted := *child
		result.right = promoted.left
		promoted.left = result
		return &promoted, nil
	}
	return result, nil
}

// insertFreshStableLogicalObligationIndex mutates only a newly owned tree.
// Unlike append insertion, no published view can share these nodes, so bulk
// construction must not allocate a copied search path for every obligation.
func insertFreshStableLogicalObligationIndex(root *stableLogicalObligationIndexNode, obligation StableLogicalObligation, work *StableResourceClosureWork) (*stableLogicalObligationIndexNode, error) {
	key := stableLogicalObligationKey(obligation)
	if root == nil {
		if work != nil {
			work.LogicalIndexNodesAdmitted++
		}
		return &stableLogicalObligationIndexNode{key: key, obligation: obligation, priority: stableLogicalObligationPriority(key)}, nil
	}
	if key == root.key {
		if root.obligation != obligation {
			return nil, fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
		}
		return nil, fmt.Errorf("%w: fresh logical obligation set repeats obligation %+v", ErrResourceConflict, key)
	}
	if stableLogicalObligationIndexLess(key, root.key) {
		child, err := insertFreshStableLogicalObligationIndex(root.left, obligation, work)
		if err != nil {
			return nil, err
		}
		root.left = child
		if child.priority < root.priority {
			root.left = child.right
			child.right = root
			return child, nil
		}
		return root, nil
	}
	child, err := insertFreshStableLogicalObligationIndex(root.right, obligation, work)
	if err != nil {
		return nil, err
	}
	root.right = child
	if child.priority < root.priority {
		root.right = child.left
		child.left = root
		return child, nil
	}
	return root, nil
}

// rangeDeltaValues visits only producer values; inherited directory records
// require walk or the set-level WalkLogicalObligations API.
func (view stableLogicalObligationView) rangeDeltaValues(visit func(StableLogicalObligation) bool) {
	if view.tail == nil || visit == nil {
		return
	}
	nodes := make([]*stableLogicalObligationNode, 0, 8)
	for node := view.tail; node != nil; node = node.parent {
		nodes = append(nodes, node)
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		for _, obligation := range nodes[i].values {
			if !visit(obligation) {
				return
			}
		}
	}
}

func (view stableLogicalObligationView) deltaSlice() []StableLogicalObligation {
	if view.count == 0 {
		return nil
	}
	capacity := view.count
	if view.directory != nil {
		capacity = 0
		for node := view.tail; node != nil; node = node.parent {
			capacity += len(node.values)
		}
	}
	result := make([]StableLogicalObligation, 0, capacity)
	view.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		result = append(result, obligation)
		return true
	})
	return result
}

// StableResourceClosureWork reports semantic work performed while deriving an
// independently owned resource closure. The counters deliberately separate
// physical entries/handle copies from logical obligations: one append-only
// physical segment can carry thousands of independently reclaimable logical
// references.
type StableResourceClosureWork struct {
	CloneOperations                         uint64
	FreezeOperations                        uint64
	RequirementFieldsInspected              uint64
	RequirementObligationsInspected         uint64
	SourceEntriesInspected                  uint64
	SourceObligationsInspected              uint64
	RetainedEntries                         uint64
	RetainedObligations                     uint64
	DroppedEntries                          uint64
	DroppedObligations                      uint64
	CopiedEntries                           uint64
	CopiedObligations                       uint64
	PhysicalHandleCopies                    uint64
	PhysicalHandleShares                    uint64
	PhysicalRootShares                      uint64
	LogicalObligationNormalizations         uint64
	RetainedIndexNodeVisits                 uint64
	RetainedIndexNodeCopies                 uint64
	LogicalIndexNodesAdmitted               uint64
	AggregateMembershipProbes               uint64
	AggregateMembershipNodeVisits           uint64
	AggregateMembershipNodeCopies           uint64
	AggregateMembershipAdmissions           uint64
	NewlyAdmittedEntries                    uint64
	NewlyAdmittedObligations                uint64
	RemovedObligations                      uint64
	AppendOnlyFastPath                      uint64
	AppendOnlyCollisionFastPath             uint64
	AppendOnlyCollisionFallbacks            uint64
	AppendOnlyFallbacks                     uint64
	DestructiveFallbacks                    uint64
	FullClosureValidations                  uint64
	FinalRequirementProofFastPath           uint64
	FinalRequirementProofFallbacks          uint64
	FinalRequirementRecordsDecoded          uint64
	FinalRequirementObligationsMaterialized uint64
	// PhysicalEntryLookup* counts only the indexed path, entered after the
	// small <=16-entry linear fast path. They are the scale-gate witness.
	PhysicalEntryLookupProbes      uint64
	PhysicalEntryLookupComparisons uint64
	PhysicalEntryLookupAdmissions  uint64
}

func (work *StableResourceClosureWork) Add(other StableResourceClosureWork) {
	if work == nil {
		return
	}
	work.CloneOperations += other.CloneOperations
	work.FreezeOperations += other.FreezeOperations
	work.RequirementFieldsInspected += other.RequirementFieldsInspected
	work.RequirementObligationsInspected += other.RequirementObligationsInspected
	work.SourceEntriesInspected += other.SourceEntriesInspected
	work.SourceObligationsInspected += other.SourceObligationsInspected
	work.RetainedEntries += other.RetainedEntries
	work.RetainedObligations += other.RetainedObligations
	work.DroppedEntries += other.DroppedEntries
	work.DroppedObligations += other.DroppedObligations
	work.CopiedEntries += other.CopiedEntries
	work.CopiedObligations += other.CopiedObligations
	work.PhysicalHandleCopies += other.PhysicalHandleCopies
	work.PhysicalHandleShares += other.PhysicalHandleShares
	work.PhysicalRootShares += other.PhysicalRootShares
	work.LogicalObligationNormalizations += other.LogicalObligationNormalizations
	work.RetainedIndexNodeVisits += other.RetainedIndexNodeVisits
	work.RetainedIndexNodeCopies += other.RetainedIndexNodeCopies
	work.LogicalIndexNodesAdmitted += other.LogicalIndexNodesAdmitted
	work.AggregateMembershipProbes += other.AggregateMembershipProbes
	work.AggregateMembershipNodeVisits += other.AggregateMembershipNodeVisits
	work.AggregateMembershipNodeCopies += other.AggregateMembershipNodeCopies
	work.AggregateMembershipAdmissions += other.AggregateMembershipAdmissions
	work.NewlyAdmittedEntries += other.NewlyAdmittedEntries
	work.NewlyAdmittedObligations += other.NewlyAdmittedObligations
	work.RemovedObligations += other.RemovedObligations
	work.AppendOnlyFastPath += other.AppendOnlyFastPath
	work.AppendOnlyCollisionFastPath += other.AppendOnlyCollisionFastPath
	work.AppendOnlyCollisionFallbacks += other.AppendOnlyCollisionFallbacks
	work.AppendOnlyFallbacks += other.AppendOnlyFallbacks
	work.DestructiveFallbacks += other.DestructiveFallbacks
	work.FullClosureValidations += other.FullClosureValidations
	work.FinalRequirementProofFastPath += other.FinalRequirementProofFastPath
	work.FinalRequirementProofFallbacks += other.FinalRequirementProofFallbacks
	work.FinalRequirementRecordsDecoded += other.FinalRequirementRecordsDecoded
	work.FinalRequirementObligationsMaterialized += other.FinalRequirementObligationsMaterialized
	work.PhysicalEntryLookupProbes += other.PhysicalEntryLookupProbes
	work.PhysicalEntryLookupComparisons += other.PhysicalEntryLookupComparisons
	work.PhysicalEntryLookupAdmissions += other.PhysicalEntryLookupAdmissions
}

// stableResourceEntryLookup is transient builder state. The frozen set remains
// an ordered slice so deterministic publication and ownership behavior stay
// unchanged.
type stableResourceEntryLookup struct {
	logical stableTable[stableLogicalResourceKey, int]
	// physical points back into the ordered entries for identity-wide conflict
	// metadata and the first exact candidate. Only identities with multiple
	// generations need the collision map.
	physical           stableTable[stablePhysicalIdentityKey, int]
	physicalCollisions stableTable[stablePhysicalResourceKey, int]
}

const stableResourceEntryLinearLookupLimit = 16

func newStableResourceEntryLookup(entries []stableResourceEntry) stableResourceEntryLookup {
	lookup := stableResourceEntryLookup{
		logical:  newStableTable[stableLogicalResourceKey, int](stableLogicalResourceKeyLess),
		physical: newStableTable[stablePhysicalIdentityKey, int](stablePhysicalIdentityKeyLess),
	}
	for i := range entries {
		lookup.add(entries, i)
	}
	return lookup
}

func (lookup *stableResourceEntryLookup) add(entries []stableResourceEntry, index int) {
	p, err := lookup.prepareAdd(entries, index, nil)
	if err != nil {
		panic(err)
	}
	p.apply()
}

type stableResourceEntryLookupIterator struct {
	single int
}

func (lookup *stableResourceEntryLookup) physicalIterator(entries []stableResourceEntry, token *StableResourceToken) (stableResourceEntryLookupIterator, error) {
	position := lookup.physical.get(token.physicalIdentityKey())
	if position == 0 {
		return stableResourceEntryLookupIterator{}, nil
	}
	representative := entries[position-1].token
	if representative.stability != token.stability {
		return stableResourceEntryLookupIterator{}, fmt.Errorf("%w: physical identity has conflicting stability policy", ErrResourceConflict)
	}
	if token.stability == ResourceImmutable && representative.digest != token.digest {
		return stableResourceEntryLookupIterator{}, fmt.Errorf("%w: immutable physical identity has conflicting content digest", ErrResourceConflict)
	}
	coalescingKey := token.physicalCoalescingKey()
	if representative.physicalCoalescingKey() == coalescingKey {
		return stableResourceEntryLookupIterator{single: position}, nil
	}
	return stableResourceEntryLookupIterator{single: lookup.physicalCollisions.get(coalescingKey)}, nil
}

func (iterator *stableResourceEntryLookupIterator) Next() (int, bool) {
	if iterator.single != 0 {
		position := iterator.single
		iterator.single = 0
		return position - 1, true
	}
	return 0, false
}

func (lookup *stableResourceEntryLookup) replaceRepresentative(index int, old, replacement *StableResourceToken) {
	if lookup.logical.get(old.logicalKey()) == index {
		lookup.logical.remove(old.logicalKey())
	}
	lookup.logical.set(replacement.logicalKey(), index)
}

func cloneStableResourceEntry(entry stableResourceEntry) stableResourceEntry {
	reachability := newStableReachabilitySet(entry.reachability.len())
	for _, stableBinding1 := range entry.reachability.records() {
		field := stableBinding1.key
		reachability.set(field, struct{}{})
	}
	clone := stableResourceEntry{
		token: entry.token, logicalLane: entry.logicalLane, resourceID: entry.resourceID,
		diagnosticPath: entry.diagnosticPath, frontier: cloneDurableFrontier(entry.frontier),
		// The persistent obligation view is immutable and may be shared across
		// independently pinned visible/durable resource-set generations.
		reachability: reachability, logicalObligations: entry.logicalObligations,
		dependencyManifestV1: entry.dependencyManifestV1,
	}
	if clone.dependencyManifestV1 == nil {
		clone.dependencyManifestV1 = &dependencyManifestEntryCacheV1{}
	}
	if len(entry.pins) != 0 {
		clone.pins = append([]*StableResourceToken(nil), entry.pins...)
	}
	return clone
}

func appendUniquePins(entry *stableResourceEntry, incoming ...*StableResourceToken) {
	if entry.pinIndex == nil {
		entry.pinIndex = newStableTokenTable()
		for _, token := range entry.pins {
			entry.pinIndex.set(token, struct{}{})
		}
	}
	for _, token := range incoming {
		if token == nil {
			continue
		}
		if _, ok := entry.pinIndex.lookup(token); ok {
			continue
		}
		entry.pinIndex.set(token, struct{}{})
		entry.pins = append(entry.pins, token)
	}
}

func activeEntryToken(entry stableResourceEntry) *StableResourceToken {
	if entry.token != nil && !entry.token.released.Load() {
		return entry.token
	}
	for _, token := range entry.pins {
		if token != nil && !token.released.Load() {
			return token
		}
	}
	return entry.token
}

func mergeStableLogicalObligations(target *stableLogicalObligationView, incoming stableLogicalObligationView) error {
	if target.directory != nil || incoming.directory != nil {
		base := *target
		if base.directory == nil {
			base, incoming = incoming, base
		}
		if incoming.directory != nil {
			if incoming.directory != base.directory || !bytes.Equal(incoming.owner, base.owner) || len(incoming.removed) != len(base.removed) {
				return ErrResourceConflict
			}
			for key, value := range incoming.removed {
				if base.removed[key] != value {
					return ErrResourceConflict
				}
			}
		}
		next, err := base.appendDirectoryDelta(incoming.deltaSlice(), nil)
		if err != nil {
			return err
		}
		*target = next
		return nil
	}
	if incoming.count == 0 {
		return nil
	}
	base := *target
	if base.index == nil && base.count != 0 {
		base = newStableLogicalObligationView(base.deltaSlice())
	}
	// Exact tail ancestry proves containment of these immutable histories.
	// Reuse the descendant instead of scanning its accumulated obligations.
	older, newer := base, incoming
	if older.count > newer.count {
		older, newer = newer, older
	}
	tail, count := newer.tail, newer.count
	for tail != nil && count > older.count {
		count -= len(tail.values)
		tail = tail.parent
	}
	if count == older.count && tail == older.tail {
		*target = newer
		return nil
	}
	// Both views own immutable, normalized obligations. Probe the existing index
	// and retain only additions instead of copying and hashing the full retained
	// payload again on each closure merge.
	var added []StableLogicalObligation
	var conflict error
	incoming.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		if existing, ok := findStableLogicalObligationIndex(base.index, obligation, nil); ok {
			if existing != obligation {
				conflict = fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, stableLogicalObligationKey(obligation))
				return false
			}
			return true
		}
		added = append(added, obligation)
		return true
	})
	// A late conflict must leave the target unchanged, including additions seen
	// earlier in this incoming batch. appendCertified path-copies the index and
	// commitments only after the complete preflight succeeds.
	if conflict != nil {
		return conflict
	}
	if len(added) == 0 {
		return nil
	}
	next, err := base.appendCertified(added, nil)
	if err != nil {
		return err
	}
	*target = next
	return nil
}

func stableLogicalObligationList(obligations []StableLogicalObligation) []StableLogicalObligation {
	return obligations[:len(obligations):len(obligations)]
}

type StableResourceSetBuilder struct {
	ordinaryMetadata   bool // provenance invariant only; never backing credit
	cleanupRunning     bool
	cleanupViewCursor  int
	cleanupViewStarted bool
	pendingCleanup     *stableBuilderCleanupOwnerV1
	stagingNextV1      *StableResourceSetBuilder // private exact cleanup custody only
	mu                 sync.Mutex
	entries            []stableResourceEntry
	kindViews          stableKindViews
	required           stableReachabilitySet
	closed             bool
	abandoned          bool
	state              ResourceOwnerState
	indexed            *stableResourceBuilderIndexedState
}

type stableResourceBuilderIndexedState struct {
	// Keep scale-only state out of the common small builder allocation.
	lookup stableResourceEntryLookup
	work   StableResourceClosureWork
}

func NewStableResourceSetBuilder(required ...ReachabilityField) *StableResourceSetBuilder {
	requiredSet := newStableReachabilitySet(len(required))
	for _, field := range required {
		if field != "" {
			requiredSet.set(field, struct{}{})
		}
	}
	return &StableResourceSetBuilder{ordinaryMetadata: true, required: requiredSet, state: ResourceOwnerBuilder}
}

// ClosureWorkSnapshot returns exact builder-local closure work accumulated by
// Add and Merge. It is reset by constructing a new builder.
func (builder *StableResourceSetBuilder) ClosureWorkSnapshot() StableResourceClosureWork {
	if builder == nil {
		return StableResourceClosureWork{}
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()
	if builder.indexed == nil {
		return StableResourceClosureWork{}
	}
	return builder.indexed.work
}

func (builder *StableResourceSetBuilder) State() ResourceOwnerState {
	if builder == nil {
		return ResourceOwnerReleased
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()
	return builder.state
}

// Exact call-local cleanup owners, never an accumulating callback queue.
// State handoff precedes unlock; reentrant callbacks observe the adopted state.
type stableBuilderAddCleanup struct {
	retired, failed *StableResourceToken
	views           stableKindViews
	temporary       *StableResourceSetBuilder
	viewCursor      int
	viewStarted     bool
	views2          stableKindViews
	viewCursor2     int
	viewStarted2    bool
	dropped         []*StableResourceToken
	droppedCursor   int
	temporary2      *StableResourceSetBuilder
	temporarySet    *StableResourceSet
	stagedBuilders  *StableResourceSetBuilder
	unclaimed       *StableResourceToken
}

func (c *stableBuilderAddCleanup) empty() bool {
	return c.retired == nil && c.failed == nil && c.views == nil && c.views2 == nil && c.temporary == nil && c.temporary2 == nil && c.temporarySet == nil && c.dropped == nil && c.stagedBuilders == nil && c.unclaimed == nil
}
func (c *stableBuilderAddCleanup) release() (result error) {
	for c.stagedBuilders != nil {
		staged := c.stagedBuilders
		if err := staged.AbandonCheckedV1(); err != nil { return errors.Join(result, err) }
		c.stagedBuilders = staged.stagingNextV1
		staged.stagingNextV1 = nil
	}
	if c.unclaimed != nil {
		err := c.unclaimed.Release()
		result = errors.Join(result, err)
		if !c.unclaimed.CleanupCompleteV1() { return errors.Join(result, ErrStableResourceOperationBusy) }
		c.unclaimed = nil
	}
	if c.temporary != nil {
		if err := c.temporary.AbandonCheckedV1(); err != nil {
			return errors.Join(result, err)
		}
		c.temporary = nil
	}
	if c.temporary2 != nil {
		if err := c.temporary2.AbandonCheckedV1(); err != nil {
			return errors.Join(result, err)
		}
		c.temporary2 = nil
	}
	if c.temporarySet != nil {
		err := c.temporarySet.Release()
		result = errors.Join(result, err)
		if c.temporarySet.Owner() != ResourceOwnerReleased {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
		c.temporarySet = nil
	}
	if c.views != nil {
		if err := releaseStableResourceKindViewsCursorV1(c.views, &c.viewCursor, &c.viewStarted); err != nil {
			return errors.Join(result, err)
		}
		c.views = nil
	}
	if c.views2 != nil {
		if err := releaseStableResourceKindViewsCursorV1(c.views2, &c.viewCursor2, &c.viewStarted2); err != nil {
			return errors.Join(result, err)
		}
		c.views2 = nil
	}
	for c.droppedCursor < len(c.dropped) {
		token := c.dropped[c.droppedCursor]
		err := token.releaseFrom(ResourceOwnerBuilder)
		result = errors.Join(result, err)
		if !token.CleanupCompleteV1() {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
		c.droppedCursor++
	}
	c.dropped = nil
	if c.retired != nil {
		err := c.retired.releaseFrom(ResourceOwnerBuilder)
		result = errors.Join(result, err)
		if !c.retired.CleanupCompleteV1() {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
		if c.failed == c.retired {
			c.failed = nil
		}
		c.retired = nil
	}
	if c.failed != nil {
		err := c.failed.releaseFrom(ResourceOwnerBuilder)
		result = errors.Join(result, err)
		if !c.failed.CleanupCompleteV1() {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
		c.failed = nil
	}
	return
}

// Each operation is born before ownership effects and remains on the actual
// builder through callbacks. Nested ordinary Add gets its own control; failure
// cannot overwrite another operation's exact retired roles. This is synchronous
// owner custody, never a scheduled cleanup queue or finite certificate.
type stableBuilderCleanupOwnerV1 struct {
	cleanup                    stableBuilderAddCleanup
	next                       *stableBuilderCleanupOwnerV1
	creator                    *residentcredit.Scope
	running, failed, uncertain bool
}

func (builder *StableResourceSetBuilder) hasFailedCleanupLockedV1() bool {
	for op := builder.pendingCleanup; op != nil; op = op.next {
		if op.failed || op.uncertain {
			return true
		}
	}
	return false
}
func cleanupTokenInViewsV1(views stableKindViews) *StableResourceToken {
	if views == nil {
		return nil
	}
	for _, binding := range views.records() {
		node := binding.value.root
		for node != nil && len(node.entries) == 0 {
			if node.left != nil {
				node = node.left
			} else {
				node = node.right
			}
		}
		if node != nil && len(node.entries) > 0 {
			return node.entries[0].token
		}
	}
	return nil
}
func firstCleanupTokenV1(builder *StableResourceSetBuilder, child *StableResourceSet) *StableResourceToken {
	if len(builder.entries) > 0 {
		return builder.entries[0].token
	}
	if token := cleanupTokenInViewsV1(builder.kindViews); token != nil {
		return token
	}
	if child != nil {
		if len(child.entries) > 0 {
			return child.entries[0].token
		}
		return cleanupTokenInViewsV1(child.kindViews)
	}
	return nil
}
func (builder *StableResourceSetBuilder) beginCleanupOwnerLockedV1(token *StableResourceToken, child *StableResourceSet) (*stableBuilderCleanupOwnerV1, error) {
	if token == nil {
		token = firstCleanupTokenV1(builder, child)
	}
	var creator *residentcredit.Scope
	if token != nil {
		token.metadataMu.Lock()
		creator = token.callbackCreator
		token.metadataMu.Unlock()
	}
	if creator != nil {
		n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableBuilderCleanupOwnerV1{})), true)
		if err != nil {
			return nil, err
		}
		if err = creator.ReserveOriginalLifetime(n); err != nil {
			return nil, err
		}
		if err = creator.RetainOriginalLifetime(); err != nil {
			return nil, err
		}
	}
	op := &stableBuilderCleanupOwnerV1{creator: creator, running: true, next: builder.pendingCleanup}
	builder.pendingCleanup = op
	return op, nil
}
func (builder *StableResourceSetBuilder) recordCleanupPanicV1(op *stableBuilderCleanupOwnerV1) {
	if op == nil {
		return
	}
	builder.mu.Lock()
	op.running = false
	op.uncertain = true
	op.failed = true
	builder.mu.Unlock()
}
func (builder *StableResourceSetBuilder) finishCleanupOwnerV1(op *stableBuilderCleanupOwnerV1) (result error) {
	if op == nil {
		return nil
	}
	defer func() {
		v := recover()
		builder.mu.Lock()
		op.running = false
		if v != nil {
			op.uncertain = true
			op.failed = true
		} else {
			op.failed = !op.cleanup.empty()
		}
		var creator *residentcredit.Scope
		if !op.failed {
			link := &builder.pendingCleanup
			for *link != nil && *link != op {
				link = &(*link).next
			}
			if *link == op {
				*link = op.next
			}
			op.next = nil
			creator = op.creator
			op.creator = nil
		}
		builder.mu.Unlock()
		op = nil
		if creator != nil {
			creator.ReleaseStableMetadata()
			creator = nil
		}
		if v != nil {
			panic(v)
		}
	}()
	if op.uncertain {
		return ErrStableResourceOperationBusy
	}
	return op.cleanup.release()
}

func requireIdleStableResourceOperationLockedV1(builder *StableResourceSetBuilder, child *StableResourceSet) error {
	if child != nil && (child.activeOperations != 0 || child.terminalReleasing) {
		return ErrStableResourceOperationBusy
	}
	return requireOrdinaryStableResourceOperationLocked(builder, child)
}
func (set *StableResourceSet) beginBackingOperationV1() error {
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.Owner() == ResourceOwnerReleased || set.Owner() == ResourceOwnerTransferred {
		return ErrResourceOwnership
	}
	if set.terminalReleasing || set.activeOperations == ^uint64(0) {
		return ErrStableResourceOperationBusy
	}
	set.activeOperations++
	return nil
}
func (set *StableResourceSet) endBackingOperationV1() {
	set.mu.Lock()
	set.activeOperations--
	set.mu.Unlock()
}
func (builder *StableResourceSetBuilder) Add(token *StableResourceToken) error {
	return builder.addWithCleanupBoundaryV1(token, false)
}

// Private staging borrows inherited gates. Its exact cleanup owners remain on
// that same builder until the outer operation drops every gate and drains them.
func (builder *StableResourceSetBuilder) pauseCleanupOwnerV1(op *stableBuilderCleanupOwnerV1) {
	if op == nil { return }
	builder.mu.Lock()
	op.running = false
	var creator *residentcredit.Scope
	// A successful clone/Add has transferred every token role to the actual
	// destination. Only a truly empty operation shell may finish here: no
	// token/provider/registry/environment cleanup can execute under inherited
	// gates. Nonempty, failed and uncertain roles stay on this same builder.
	if !op.failed && !op.uncertain && op.cleanup.empty() {
		link := &builder.pendingCleanup
		for *link != nil && *link != op { link = &(*link).next }
		if *link == op {
			*link = op.next
			op.next = nil
			creator = op.creator
			op.creator = nil
		}
	}
	builder.mu.Unlock()
	// Scope is concrete scalar accounting under its independent Owner mutex;
	// it has no extensible release callback. Public/control births remain
	// cumulatively charged; this discharges only the exact temporary edge.
	op = nil
	if creator != nil { creator.ReleaseStableMetadata() }
}
func (builder *StableResourceSetBuilder) addWithCleanupBoundaryV1(token *StableResourceToken, staging bool) (result error) {
	if builder == nil || token == nil {
		return ErrResourceOwnership
	}
	builder.mu.Lock()
	var op *stableBuilderCleanupOwnerV1
	var cleanup *stableBuilderAddCleanup
	if builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		builder.mu.Unlock()
		return ErrStableResourceOperationBusy
	}
	defer func() {
		v := recover()
		builder.mu.Unlock()
		if v != nil {
			builder.recordCleanupPanicV1(op)
			panic(v)
		}
		if staging { builder.pauseCleanupOwnerV1(op) } else { result = errors.Join(result, builder.finishCleanupOwnerV1(op)) }
	}()
	if err := token.RequireMetadataExport(); err != nil {
		return err
	}
	if err := builder.requireOrdinaryMetadataLocked(); err != nil {
		return err
	}
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		return ErrResourceOwnership
	}
	op, err := builder.beginCleanupOwnerLockedV1(token, nil)
	if err != nil {
		return err
	}
	cleanup = &op.cleanup
	if builder.kindViews != nil {
		return builder.addToViewsLocked(token, cleanup)
	}
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		return err
	}
	if builder.indexed == nil {
		if len(builder.entries) < stableResourceEntryLinearLookupLimit {
			retired, err := mergeOwnedTokenLinear(&builder.entries, token)
			cleanup.retired = retired
			if err != nil {
				cleanup.failed = token
			}
			return err
		}
		builder.indexed = &stableResourceBuilderIndexedState{
			lookup: newStableResourceEntryLookup(builder.entries),
		}
	}
	retired, err := mergeOwnedToken(&builder.entries, &builder.indexed.lookup, token, &builder.indexed.work)
	cleanup.retired = retired
	if err != nil {
		cleanup.failed = token
		return err
	}
	return nil
}

func (builder *StableResourceSetBuilder) addToViewsLocked(token *StableResourceToken, cleanup *stableBuilderAddCleanup) error {
	if err := token.namespace.validateStable(); err != nil {
		return err
	}
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		return err
	}
	incoming := []stableResourceEntry{{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         newStableReachabilitySet(1, token.reachability),
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	}}
	if !stableResourceViewsConflict(builder.kindViews, &incoming[0]) {
		views, err := buildStableResourceKindViews(incoming)
		if err != nil {
			cleanup.failed = token
			return err
		}
		merged, ok := mergeDistinctStableResourceKindViews(builder.kindViews, views, nil)
		if !ok {
			cleanup.views = views
			return ErrResourceConflict
		}
		builder.kindViews = merged
		return nil
	}

	// Exact identity collisions retain the established coalescing rules.
	temporary := NewStableResourceSetBuilder()
	cleanup.temporary = temporary
	if err := cloneStableResourceViewsIntoBuilder(temporary, builder.kindViews); err != nil {
		cleanup.temporary = temporary
		cleanup.failed = token
		return err
	}
	temporary.mu.Lock()
	var err error
	if temporary.indexed == nil && len(temporary.entries) < stableResourceEntryLinearLookupLimit {
		cleanup.retired, err = mergeOwnedTokenLinear(&temporary.entries, token)
	} else {
		if temporary.indexed == nil {
			temporary.indexed = &stableResourceBuilderIndexedState{lookup: newStableResourceEntryLookup(temporary.entries)}
		}
		cleanup.retired, err = mergeOwnedToken(&temporary.entries, &temporary.indexed.lookup, token, &temporary.indexed.work)
	}
	if err != nil {
		temporary.mu.Unlock()
		cleanup.temporary = temporary
		cleanup.failed = token
		return err
	}
	oldViews := builder.kindViews
	builder.kindViews = nil
	builder.entries = temporary.entries
	builder.indexed = temporary.indexed
	temporary.entries = nil
	temporary.indexed = nil
	temporary.closed = true
	temporary.mu.Unlock()
	cleanup.views = oldViews
	return nil
}

func mergeOwnedTokenLinear(entries *[]stableResourceEntry, token *StableResourceToken) (*StableResourceToken, error) {
	logicalKey := token.logicalKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(token) {
			return nil, fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
		coalesce, err := stableResourcesCoalesce(existing, token)
		if err != nil {
			return nil, err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return nil, fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, newStableLogicalObligationView(token.logicalObligations)); err != nil {
			return nil, err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability.set(token.reachability, struct{}{})
		mergeStableResourceDescriptorIdentity(entry, token.logicalLane, token.resourceID, token.diagnosticPath)
		if existing.namespace == nil && token.namespace != nil {
			entry.token = token
			entry.pins = nil
			entry.pinIndex = nil
			return existing, nil
		} else {
			return token, nil
		}
	}
	*entries = append(*entries, stableResourceEntry{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         newStableReachabilitySet(1, token.reachability),
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	})
	return nil, nil
}

func mergeOwnedToken(entries *[]stableResourceEntry, lookup *stableResourceEntryLookup, token *StableResourceToken, work *StableResourceClosureWork) (*StableResourceToken, error) {
	logicalKey := token.logicalKey()
	work.PhysicalEntryLookupProbes++
	if existingIndex, ok := lookup.logical.lookup(logicalKey); ok {
		existing := (*entries)[existingIndex].token
		if !existing.samePhysicalIdentity(token) {
			return nil, fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
	}
	iterator, err := lookup.physicalIterator(*entries, token)
	if err != nil {
		return nil, err
	}
	for {
		i, ok := iterator.Next()
		if !ok {
			break
		}
		entry := &(*entries)[i]
		existing := entry.token
		work.PhysicalEntryLookupComparisons++
		coalesce, err := stableResourcesCoalesce(existing, token)
		if err != nil {
			return nil, err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return nil, fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, newStableLogicalObligationView(token.logicalObligations)); err != nil {
			return nil, err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability.set(token.reachability, struct{}{})
		mergeStableResourceDescriptorIdentity(entry, token.logicalLane, token.resourceID, token.diagnosticPath)
		if existing.namespace == nil && token.namespace != nil {
			// Preserve the one namespace-creation obligation independently of
			// insertion order by making its exact-handle token representative.
			entry.token = token
			lookup.replaceRepresentative(i, existing, token)
			entry.pins = nil
			entry.pinIndex = nil
			return existing, nil
		} else {
			return token, nil
		}
	}
	*entries = append(*entries, stableResourceEntry{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         newStableReachabilitySet(1, token.reachability),
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	})
	lookup.add(*entries, len(*entries)-1)
	work.PhysicalEntryLookupAdmissions++
	return nil, nil
}

func mergeStableResourceDescriptorIdentity(entry *stableResourceEntry, lane, resourceID, diagnosticPath string) {
	if entry == nil {
		return
	}
	entry.dependencyManifestV1 = &dependencyManifestEntryCacheV1{}
	if entry.logicalLane == "" || lane < entry.logicalLane ||
		(lane == entry.logicalLane && (resourceID < entry.resourceID ||
			(resourceID == entry.resourceID && diagnosticPath < entry.diagnosticPath))) {
		entry.logicalLane = lane
		entry.resourceID = resourceID
		entry.diagnosticPath = diagnosticPath
	}
}

func stableResourcesCoalesce(existing, incoming *StableResourceToken) (bool, error) {
	if !existing.samePhysicalIdentity(incoming) {
		return false, nil
	}
	if existing.stability != incoming.stability {
		return false, fmt.Errorf("%w: physical identity has conflicting stability policy", ErrResourceConflict)
	}
	switch existing.stability {
	case ResourceMutableAppend:
		if existing.mutablePhysicalKey() != incoming.mutablePhysicalKey() {
			return false, nil
		}
		if existing.digest != incoming.digest {
			return false, fmt.Errorf("%w: mutable physical identity has conflicting immutable header digest", ErrResourceConflict)
		}
		return true, nil
	case ResourceImmutable:
		if existing.digest != incoming.digest {
			return false, fmt.Errorf("%w: immutable physical identity has conflicting content digest", ErrResourceConflict)
		}
		// A physical immutable file may satisfy multiple logical generations,
		// but each generation remains an independent publication and deletion
		// obligation. Keep one owned pin per generation so generation-scoped
		// frontier and deletion lookups cannot lose authority during coalescing.
		if existing.generation != incoming.generation {
			return false, nil
		}
		return true, nil
	default:
		return false, fmt.Errorf("%w: missing stability policy", ErrUnresolvedResource)
	}
}

// The validated-count mode is private to coordinator admission. Its inputs
// are owned frozen candidate entries whose frontiers were validated and cloned
// at construction. It still reconciles identities, namespaces and accumulated
// logical obligations, but never builds a resource view for publication or sync.
type stableResourceViewMode uint8

const (
	stableResourceViewUnpinned stableResourceViewMode = iota
	stableResourceViewPinned
	stableResourceViewValidatedCount
	// Physical-only unions never export logical authority.
	stableResourceViewPhysicalPinned
	// Admission only: borrow immutable physical metadata for compatibility/count.
	// Never expose this transient result as a publication or ownership view.
	stableResourceViewPhysicalCount
)

func mergeViewEntry(entries *[]stableResourceEntry, lookup *stableResourceEntryLookup, incoming stableResourceEntry, mode stableResourceViewMode, work *StableResourceClosureWork) error {
	logicalKey := incoming.token.logicalKey()
	if work != nil {
		work.PhysicalEntryLookupProbes++
	}
	if existingIndex, ok := lookup.logical.lookup(logicalKey); ok {
		existing := (*entries)[existingIndex].token
		if !existing.samePhysicalIdentity(incoming.token) {
			return fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
	}
	iterator, err := lookup.physicalIterator(*entries, incoming.token)
	if err != nil {
		return err
	}
	for {
		i, ok := iterator.Next()
		if !ok {
			break
		}
		entry := &(*entries)[i]
		existing := entry.token
		if work != nil {
			work.PhysicalEntryLookupComparisons++
		}
		coalesce, err := stableResourcesCoalesce(existing, incoming.token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(incoming.token) || (mode != stableResourceViewValidatedCount && !frontierCompatible(entry.frontier, incoming.frontier)) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if mode != stableResourceViewPhysicalPinned && mode != stableResourceViewPhysicalCount {
			if err := mergeStableLogicalObligations(&entry.logicalObligations, incoming.logicalObligations); err != nil {
				return err
			}
		}
		if mode != stableResourceViewValidatedCount && mode != stableResourceViewPhysicalCount {
			entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
			mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
			for _, stableBinding1 := range incoming.reachability.records() {
				field := stableBinding1.key
				entry.reachability.set(field, struct{}{})
			}
		}
		if mode == stableResourceViewPinned || mode == stableResourceViewPhysicalPinned {
			if len(entry.pins) == 0 {
				entry.pins = []*StableResourceToken{entry.token}
			}
			if len(incoming.pins) == 0 {
				appendUniquePins(entry, incoming.token)
			} else {
				appendUniquePins(entry, incoming.pins...)
			}
		}
		if existing.namespace == nil && incoming.token.namespace != nil {
			entry.token = incoming.token
			lookup.replaceRepresentative(i, existing, incoming.token)
			if mode != stableResourceViewPinned && mode != stableResourceViewPhysicalPinned {
				entry.pins = nil
				entry.pinIndex = nil
			}
		}
		return nil
	}
	if mode == stableResourceViewValidatedCount {
		*entries = append(*entries, stableResourceEntry{token: incoming.token, logicalObligations: incoming.logicalObligations})
	} else if mode == stableResourceViewPhysicalCount {
		*entries = append(*entries, stableResourceEntry{token: incoming.token, frontier: incoming.frontier})
	} else {
		*entries = append(*entries, cloneStableResourceEntry(incoming))
	}
	lookup.add(*entries, len(*entries)-1)
	if work != nil {
		work.PhysicalEntryLookupAdmissions++
	}
	return nil
}

func mergeViewEntryLinear(entries *[]stableResourceEntry, incoming stableResourceEntry, mode stableResourceViewMode, work *StableResourceClosureWork) error {
	logicalKey := incoming.token.logicalKey()
	if work != nil {
		work.PhysicalEntryLookupProbes++
	}
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(incoming.token) {
			return fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
		if work != nil {
			work.PhysicalEntryLookupComparisons++
		}
		coalesce, err := stableResourcesCoalesce(existing, incoming.token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(incoming.token) || (mode != stableResourceViewValidatedCount && !frontierCompatible(entry.frontier, incoming.frontier)) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if mode != stableResourceViewPhysicalPinned && mode != stableResourceViewPhysicalCount {
			if err := mergeStableLogicalObligations(&entry.logicalObligations, incoming.logicalObligations); err != nil {
				return err
			}
		}
		if mode != stableResourceViewValidatedCount && mode != stableResourceViewPhysicalCount {
			entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
			mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
			for _, stableBinding1 := range incoming.reachability.records() {
				field := stableBinding1.key
				entry.reachability.set(field, struct{}{})
			}
		}
		if mode == stableResourceViewPinned || mode == stableResourceViewPhysicalPinned {
			if len(entry.pins) == 0 {
				entry.pins = []*StableResourceToken{entry.token}
			}
			if len(incoming.pins) == 0 {
				appendUniquePins(entry, incoming.token)
			} else {
				appendUniquePins(entry, incoming.pins...)
			}
		}
		if existing.namespace == nil && incoming.token.namespace != nil {
			entry.token = incoming.token
			if mode != stableResourceViewPinned && mode != stableResourceViewPhysicalPinned {
				entry.pins = nil
				entry.pinIndex = nil
			}
		}
		return nil
	}
	if mode == stableResourceViewValidatedCount {
		*entries = append(*entries, stableResourceEntry{token: incoming.token, logicalObligations: incoming.logicalObligations})
	} else if mode == stableResourceViewPhysicalCount {
		*entries = append(*entries, stableResourceEntry{token: incoming.token, frontier: incoming.frontier})
	} else {
		*entries = append(*entries, cloneStableResourceEntry(incoming))
	}
	if work != nil {
		work.PhysicalEntryLookupAdmissions++
	}
	return nil
}

func mergeAppendOnlyViewEntryLinear(entries *[]stableResourceEntry, incoming stableResourceEntry, work *StableResourceClosureWork) error {
	logicalKey := incoming.token.logicalKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(incoming.token) {
			return fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
		coalesce, err := stableResourcesCoalesce(existing, incoming.token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if incoming.logicalObligations.directory != nil {
			err = mergeStableLogicalObligations(&entry.logicalObligations, incoming.logicalObligations)
		} else {
			entry.logicalObligations, err = entry.logicalObligations.appendCertified(incoming.logicalObligations.deltaSlice(), work)
		}
		if err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
		mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
		for _, stableBinding1 := range incoming.reachability.records() {
			field := stableBinding1.key
			entry.reachability.set(field, struct{}{})
		}
		if existing.namespace == nil && incoming.token.namespace != nil {
			entry.token = incoming.token
			entry.pins = nil
			entry.pinIndex = nil
		}
		return nil
	}
	if err := rejectDistinctLogicalObligationOverlap(*entries, incoming); err != nil {
		return err
	}
	*entries = append(*entries, cloneStableResourceEntry(incoming))
	return nil
}

func rejectDistinctLogicalObligationOverlap(entries []stableResourceEntry, incoming stableResourceEntry) error {
	for i := range entries {
		existing := entries[i].logicalObligations
		if existing.directory != nil && incoming.logicalObligations.directory != nil && existing.directory != incoming.logicalObligations.directory {
			return fmt.Errorf("%w: distinct retained dependency directories", ErrResourceConflict)
		}
		var overlapErr error
		check := func(obligation StableLogicalObligation, view stableLogicalObligationView) bool {
			_, found, err := view.lookup(obligation, nil)
			if err != nil {
				overlapErr = err
				return false
			}
			if found {
				overlapErr = fmt.Errorf("%w: distinct resource repeated logical obligation %+v", ErrResourceConflict, stableLogicalObligationKey(obligation))
				return false
			}
			return true
		}
		incoming.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool { return check(obligation, existing) })
		if overlapErr != nil {
			return overlapErr
		}
		if incoming.logicalObligations.directory != nil {
			existing.rangeDeltaValues(func(obligation StableLogicalObligation) bool { return check(obligation, incoming.logicalObligations) })
			if overlapErr != nil {
				return overlapErr
			}
		}
	}
	return nil
}

// Generic closure derivation has not acquired finite set/registry backing
// credit. Reject the complete accounted input before staging any clone or
// Observe, including secondary coalesced pins and namespace-only accounts.
func stableResourceEntryHasMetadataAccount(entry *stableResourceEntry) bool {
	if entry == nil {
		return false
	}
	has := func(token *StableResourceToken) bool {
		return token != nil && token.RequireMetadataExport() != nil
	}
	if has(entry.token) {
		return true
	}
	for _, token := range entry.pins {
		if has(token) {
			return true
		}
	}
	if entry.pinIndex != nil && !entry.pinIndex.visit(func(token *StableResourceToken, _ struct{}) bool { return !has(token) }) {
		return true
	}
	return false
}
func rejectAccountedStableResourceViews(views stableKindViews) error {
	// Unknown sets may have mismatched indexes. Retained rope and physical-index
	// backing belong to the closure even when the authoritative logical view
	// omits them. Scan all three without sorting, snapshots or staging scratch.
	for _, stableBinding1 := range views.records() {
		view := stableBinding1.value
		if stableResourceRopeHasMetadataAccount(view.root) || stableResourceLogicalIndexHasMetadataAccount(view.logical) || stableResourcePhysicalIndexHasMetadataAccount(view.physical) {
			return ErrStableMetadataShapeUnsupported
		}
	}
	return nil
}
func stableResourceRopeHasMetadataAccount(node *stableResourceEntryNode) bool {
	if node == nil {
		return false
	}
	for i := range node.entries {
		if stableResourceEntryHasMetadataAccount(&node.entries[i]) {
			return true
		}
	}
	return stableResourceRopeHasMetadataAccount(node.left) || stableResourceRopeHasMetadataAccount(node.right)
}
func stableResourceLogicalIndexHasMetadataAccount(node *stableResourceLogicalIndexNode) bool {
	if node == nil {
		return false
	}
	return stableResourceEntryHasMetadataAccount(node.entry) || stableResourceLogicalIndexHasMetadataAccount(node.left) || stableResourceLogicalIndexHasMetadataAccount(node.right)
}
func stableResourcePhysicalIndexHasMetadataAccount(node *stableResourcePhysicalIndexNode) bool {
	if node == nil {
		return false
	}
	for _, entry := range node.entries {
		if stableResourceEntryHasMetadataAccount(entry) {
			return true
		}
	}
	return stableResourcePhysicalIndexHasMetadataAccount(node.left) || stableResourcePhysicalIndexHasMetadataAccount(node.right)
}
func (source *StableResourceSet) rejectAccountedCloneInput() error {
	return source.RequireMetadataExport()
}

// Each clone owns one scalar observation environment. It never captures the
// source token or the original Snapshot/counter/caller release environment.
type stableCloneObservation struct {
	registry *IdentityPinRegistry
	identity StableIdentity
}

func (e *stableCloneObservation) ReleaseStableResource() {
	registry, identity := e.registry, e.identity
	e.registry = nil
	e.identity = StableIdentity{}
	if registry != nil {
		_ = registry.Unobserve(identity)
	}
	registry = nil
}

// This actual source admission spans namespace/provider capture and the complete
// constructor. The deferred local scrub precedes the final source operation edge.
func cloneStableEntryToken(token *StableResourceToken, lane, resourceID, diagnosticPath string, frontier DurableFrontier, field ReachabilityField, obligations []StableLogicalObligation, directory *DependencyDirectoryV2, physical bool) (cloned *StableResourceToken, err error) {
	if err = token.beginOperation(); err != nil {
		return nil, err
	}
	var environment StableResourceReleaseEnvironment
	var registry *IdentityPinRegistry
	var namespace *StableNamespaceToken
	var spec StableResourceSpec
	observed := false
	defer func() {
		value := recover()
		if (err != nil || value != nil) && observed {
			_ = registry.Unobserve(token.identity)
		}
		if physical && namespace != nil {
			namespace.Release()
		}
		spec = StableResourceSpec{}
		environment, registry, namespace = nil, nil, nil
		token.endOperationOutcome(value)
	}()
	if err = token.namespace.validateStable(); err != nil {
		return nil, err
	}
	if token.identityPin != nil {
		registry = token.identityPin.registry
	}
	if registry != nil {
		if token.callbackCreator != nil {
			n, classErr := StableBackingClassBytes(uint64(unsafe.Sizeof(stableCloneObservation{})), true)
			if classErr != nil {
				return nil, classErr
			}
			if err = token.callbackCreator.ReserveOriginalLifetime(n); err != nil {
				return nil, err
			}
		}
		environment = &stableCloneObservation{registry: registry, identity: token.identity}
		if err = registry.Observe(token.identity); err != nil {
			return nil, err
		}
		observed = true
	}
	if !physical {
		cloned, err = token.cloneSharedPinnedEnvironment(lane, resourceID, diagnosticPath, frontier, field, obligations, directory, nil, environment, nil, 0, false, nil)
	} else {
		if token.namespace != nil && token.callbackCreator != nil {
			// Private platform namespace/FD internals remain uncertified; these actual
			// known public control births are still predebit before cloneStable.
			var plan stableBackingSizePlan
			plan.add(uint64(unsafe.Sizeof(StableNamespaceToken{})), true)
			plan.add(uint64(unsafe.Sizeof(os.File{})), true)
			if plan.err != nil {
				return nil, plan.err
			}
			if err = token.callbackCreator.ReserveOriginalLifetime(plan.bytes); err != nil {
				return nil, err
			}
		}
		namespace, err = token.namespace.cloneStable()
		if err != nil {
			return nil, err
		}
		spec = StableResourceSpec{
			Kind: token.kind, LogicalLane: lane, ResourceID: resourceID,
			Generation: token.generation, DiagnosticPath: diagnosticPath, File: token.pinned,
			Frontier: frontier, Digest: token.digest, Reachability: field, Namespace: namespace,
			LogicalObligations: obligations, PinRegistry: registry, StableIdentityOverride: token.identity,
			CallbackCreator: token.callbackCreator, CallbackProvider: token.callbackProvider, ReleaseEnvironment: environment,
		}
		if token.callbackProvider == nil && token.callbackBacked {
			spec.FlushThrough, spec.SyncThrough = token.flush, token.sync
		}
		cloned, err = newStableResourceToken(spec, obligations)
		if err == nil {
			cloned.syncedFrontier = cloneDurableFrontier(token.syncedFrontier)
			cloned.hasSyncedFrontier = token.hasSyncedFrontier
			if cloned.hasSyncedFrontier {
				cloned.metrics.physicalFileSyncs.Store(1)
			}
		}
	}
	if err == nil {
		observed = false
	}
	return cloned, err
}

func cloneStableResourceEntryIntoBuilder(builder *StableResourceSetBuilder, source *stableResourceEntry) error {
	if builder == nil || source == nil {
		return ErrResourceOwnership
	}
	if err := requireOrdinaryStableResourceBuilderInputs(builder); err != nil {
		return err
	}
	if stableResourceEntryHasMetadataAccount(source) {
		return ErrStableMetadataShapeUnsupported
	}
	token := activeEntryToken(*source)
	if token == nil || token.released.Load() {
		return ErrResourceOwnership
	}
	fields := make([]ReachabilityField, 0, source.reachability.len())
	for _, stableBinding1 := range source.reachability.records() {
		field := stableBinding1.key
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return ErrUnresolvedResource
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	builder.mu.Lock()
	cloneOwner, err := builder.beginCleanupOwnerLockedV1(token, nil)
	builder.mu.Unlock()
	if err != nil { return err }
	defer builder.pauseCleanupOwnerV1(cloneOwner)
	cloned, err := cloneStableEntryToken(token, source.logicalLane, source.resourceID, source.diagnosticPath, source.frontier, fields[0], source.logicalObligations.deltaSlice(), source.logicalObligations.directory, false)
	if err != nil {
		return err
	}
	cloneOwner.cleanup.unclaimed = cloned
	before := len(builder.entries)
	if err := builder.addWithCleanupBoundaryV1(cloned, true); err != nil {
		// A claimed token is held by Add's own exact retired/failed role or
		// destination entries. Only a genuinely unclaimed clone stays here.
		if ResourceOwnerState(cloned.owner.Load()) != ResourceOwnerToken { cloneOwner.cleanup.unclaimed = nil }
		return err
	}
	cloneOwner.cleanup.unclaimed = nil
	builder.mu.Lock()
	defer builder.mu.Unlock()
	var destination *stableResourceEntry
	for i := range builder.entries {
		coalesce, coalesceErr := stableResourcesCoalesce(builder.entries[i].token, token)
		if coalesceErr != nil {
			return coalesceErr
		}
		if coalesce {
			destination = &builder.entries[i]
			break
		}
	}
	if destination == nil {
		return ErrUnresolvedResource
	}
	for _, field := range fields {
		destination.reachability.set(field, struct{}{})
	}
	if len(builder.entries) > before {
		destination.logicalObligations = source.logicalObligations
		destination.dependencyManifestV1 = source.dependencyManifestV1
	} else if source.logicalObligations.directory != nil {
		if err := mergeStableLogicalObligations(&destination.logicalObligations, source.logicalObligations); err != nil {
			return err
		}
	}
	return nil
}

func cloneStableResourceViewsIntoBuilder(builder *StableResourceSetBuilder, views stableKindViews) error {
	if err := requireOrdinaryStableResourceBuilderInputs(builder); err != nil {
		return err
	}
	if err := rejectAccountedStableResourceViews(views); err != nil {
		return err
	}
	var cloneErr error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		cloneErr = cloneStableResourceEntryIntoBuilder(builder, entry)
		return cloneErr == nil
	})
	return cloneErr
}

func (builder *StableResourceSetBuilder) promoteEntriesToViewsLocked() error {
	if builder.kindViews != nil || len(builder.entries) == 0 {
		return nil
	}
	sortStableResourceEntries(builder.entries)
	views, err := buildStableResourceKindViews(builder.entries)
	if err != nil {
		return err
	}
	builder.kindViews = views
	builder.entries = nil
	builder.indexed = nil
	return nil
}

func (builder *StableResourceSetBuilder) mergeViewSet(child *StableResourceSet) (handled bool, result error) {
	if builder == nil || child == nil {
		return true, ErrResourceOwnership
	}
	builderLocked, childLocked := true, false
	builder.mu.Lock()
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		builderLocked = false
		builder.mu.Unlock()
		return true, ErrResourceOwnership
	}
	child.mu.Lock()
	childLocked = true
	if err := requireIdleStableResourceOperationLockedV1(builder, child); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, err
	}
	op, err := builder.beginCleanupOwnerLockedV1(nil, child)
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, err
	}
	defer func() {
		if v := recover(); v != nil {
			if childLocked {
				childLocked = false
				child.mu.Unlock()
			}
			if builderLocked {
				builderLocked = false
				builder.mu.Unlock()
			}
			builder.recordCleanupPanicV1(op)
			panic(v)
		}
		result = errors.Join(result, builder.finishCleanupOwnerV1(op))
	}()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder || child.kindViews == nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return false, nil
	}
	if builder.kindViews == nil && len(builder.entries) != 0 {
		// A mutable flat builder keeps the established exact representative and
		// work accounting. Only the frozen child is materialized.
		temporary := NewStableResourceSetBuilder()
		op.cleanup.temporary = temporary
		if err := cloneStableResourceViewsIntoBuilder(temporary, child.kindViews); err != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return true, err
		}
		temporary.mu.Lock()
		incoming := temporary.entries
		merged := cloneStableResourceEntries(builder.entries)
		lookup := stableResourceEntryLookup{}
		var indexedWork StableResourceClosureWork
		if len(merged)+len(incoming) > stableResourceEntryLinearLookupLimit {
			lookup = newStableResourceEntryLookup(merged)
			if builder.indexed != nil {
				indexedWork = builder.indexed.work
			}
		}
		for _, entry := range incoming {
			var err error
			if lookup.logical.less == nil {
				err = mergeViewEntryLinear(&merged, entry, stableResourceViewUnpinned, nil)
			} else {
				err = mergeViewEntry(&merged, &lookup, entry, stableResourceViewUnpinned, &indexedWork)
			}
			if err != nil {
				temporary.mu.Unlock()
				childLocked = false
				child.mu.Unlock()
				builderLocked = false
				builder.mu.Unlock()
				return true, err
			}
		}
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			temporary.mu.Unlock()
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return true, ErrResourceOwnership
		}
		dropped := droppedStableResourceTokens(merged, builder.entries, incoming)
		oldChildViews := child.kindViews
		builder.entries = merged
		if lookup.logical.less == nil {
			builder.indexed = nil
		} else {
			builder.indexed = &stableResourceBuilderIndexedState{lookup: lookup, work: indexedWork}
		}
		temporary.entries = nil
		temporary.indexed = nil
		temporary.closed = true
		temporary.mu.Unlock()
		child.kindViews = nil
		child.entries = nil
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		op.cleanup.views = oldChildViews
		op.cleanup.dropped = dropped
		return true, nil
	}
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, err
	}
	merged, distinct := mergeDistinctStableResourceKindViews(builder.kindViews, child.kindViews, nil)
	if distinct {
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return true, ErrResourceOwnership
		}
		builder.kindViews = merged
		child.kindViews = nil
		child.entries = nil
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, nil
	}

	// A collision is uncommon on the certified production path. Preserve every
	// existing coalescing and conflict rule by rebuilding an independently pinned
	// flat builder, then resume the ordinary exact implementation.
	currentViews := builder.kindViews
	childViews := child.kindViews

	temporary := NewStableResourceSetBuilder()
	op.cleanup.temporary = temporary
	if err := cloneStableResourceViewsIntoBuilder(temporary, currentViews); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, err
	}
	if err := cloneStableResourceViewsIntoBuilder(temporary, childViews); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, err
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return true, ErrResourceOwnership
	}
	oldBuilderViews, oldChildViews := builder.kindViews, child.kindViews
	builder.kindViews = nil
	builder.entries = temporary.entries
	builder.indexed = temporary.indexed
	temporary.entries = nil
	temporary.indexed = nil
	temporary.closed = true
	child.kindViews = nil
	child.entries = nil
	childLocked = false
	child.mu.Unlock()
	builderLocked = false
	builder.mu.Unlock()
	op.cleanup.views = oldBuilderViews
	op.cleanup.views2 = oldChildViews
	return true, nil
}

// Merge consumes a child builder-owned set only after the complete transitive
// union has passed conflict checks. This is the one-way child-to-parent
// transfer used before a parent installs a child root or catalog ID.
func (builder *StableResourceSetBuilder) Merge(child *StableResourceSet) (result error) {
	if child != nil && child.physicalOnly {
		return ErrResourceOwnership
	}
	if handled, err := builder.mergeViewSet(child); handled {
		return err
	}
	if builder == nil || child == nil {
		return ErrResourceOwnership
	}
	builderLocked, childLocked := true, false
	builder.mu.Lock()
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		builderLocked = false
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	child.mu.Lock()
	childLocked = true
	if err := requireIdleStableResourceOperationLockedV1(builder, child); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return err
	}
	op, err := builder.beginCleanupOwnerLockedV1(nil, child)
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return err
	}
	defer func() {
		if v := recover(); v != nil {
			if childLocked {
				childLocked = false
				child.mu.Unlock()
			}
			if builderLocked {
				builderLocked = false
				builder.mu.Unlock()
			}
			builder.recordCleanupPanicV1(op)
			panic(v)
		}
		result = errors.Join(result, builder.finishCleanupOwnerV1(op))
	}()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	merged := cloneStableResourceEntries(builder.entries)
	lookup := stableResourceEntryLookup{}
	var indexedWork StableResourceClosureWork
	if len(merged)+len(child.entries) > stableResourceEntryLinearLookupLimit {
		lookup = newStableResourceEntryLookup(merged)
		if builder.indexed != nil {
			indexedWork = builder.indexed.work
		}
	}
	for _, entry := range child.entries {
		var err error
		if lookup.logical.less == nil {
			err = mergeViewEntryLinear(&merged, entry, stableResourceViewUnpinned, nil)
		} else {
			err = mergeViewEntry(&merged, &lookup, entry, stableResourceViewUnpinned, &indexedWork)
		}
		if err != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return err
		}
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	dropped := droppedStableResourceTokens(merged, builder.entries, child.entries)
	// Kept tokens remain in the builder ownership phase. Only duplicates omitted
	// from the committed merged view are released, and only after the child CAS
	// makes the ownership transfer irreversible.
	builder.entries = merged
	if lookup.logical.less == nil {
		builder.indexed = nil
	} else {
		builder.indexed = &stableResourceBuilderIndexedState{lookup: lookup, work: indexedWork}
	}
	child.entries = nil
	emptyDirectory := child.emptyDependencyDirectoryLocked()
	child.extras = nil
	childLocked = false
	child.mu.Unlock()
	builderLocked = false
	builder.mu.Unlock()
	if emptyDirectory != nil {
		emptyDirectory.Release()
	}
	op.cleanup.dropped = dropped
	return nil
}

// MergeAppendOnlyLogicalObligations consumes a producer set using exact
// removal-free mutation evidence. Distinct immutable physical roots transfer
// directly; identity collisions retain the existing exact coalescing path.
func (builder *StableResourceSetBuilder) MergeAppendOnlyLogicalObligations(child *StableResourceSet, mutation StableLogicalObligationMutation) (resultWork StableResourceClosureWork, result error) {
	if child != nil && child.physicalOnly {
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	if builder == nil || child == nil {
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	builderLocked, childLocked := true, false
	builder.mu.Lock()
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	child.mu.Lock()
	childLocked = true
	if err := requireIdleStableResourceOperationLockedV1(builder, child); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	op, err := builder.beginCleanupOwnerLockedV1(nil, child)
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	defer func() {
		if v := recover(); v != nil {
			if childLocked {
				childLocked = false
				child.mu.Unlock()
			}
			if builderLocked {
				builderLocked = false
				builder.mu.Unlock()
			}
			builder.recordCleanupPanicV1(op)
			panic(v)
		}
		result = errors.Join(result, builder.finishCleanupOwnerV1(op))
	}()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	if child.emptyDependencyDirectoryLocked() != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, fmt.Errorf("%w: append-only producer is a retained directory", ErrResourceConflict)
	}
	if child.kindViews == nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return builder.mergeAppendOnlyLogicalObligationsFlat(child, mutation)
	}
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}

	_, directWork, err := validateAppendOnlyProducerViews(child.kindViews, mutation)
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return directWork, err
	}
	merged, distinct := mergeDistinctStableResourceKindViews(builder.kindViews, child.kindViews, &directWork)
	if distinct {
		admissible, complete := true, true
		var admissionErr error
		rangeStableResourceKindViews(child.kindViews, func(entry *stableResourceEntry) bool {
			var entryComplete bool
			admissible, entryComplete, admissionErr = stableResourceViewsAdmitLogicalObligations(builder.kindViews, entry, nil, nil, &directWork)
			complete = complete && entryComplete
			return admissionErr == nil && admissible && complete
		})
		if admissionErr != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return directWork, admissionErr
		}
		if admissible && complete {
			if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
				childLocked = false
				child.mu.Unlock()
				builderLocked = false
				builder.mu.Unlock()
				return directWork, ErrResourceOwnership
			}
			builder.kindViews = merged
			child.kindViews = nil
			child.entries = nil
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			directWork.AppendOnlyFastPath = 1
			return directWork, nil
		}
	}
	if plan, work, certified, err := certifiedAppendOnlyPhysicalCoalesceWithCleanupV1(builder.kindViews, child.kindViews, mutation, &op.cleanup); certified || err != nil {
		if err != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			if plan != nil {
				op.cleanup.views = plan.staged
				plan.staged = nil
			}
			return work, err
		}
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			if plan != nil {
				op.cleanup.views = plan.staged
				plan.staged = nil
			}
			return work, ErrResourceOwnership
		}
		builder.kindViews = plan.views
		child.kindViews = nil
		child.entries = nil
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		op.cleanup.views = plan.releaseIncoming
		plan.releaseIncoming = nil
		work.AppendOnlyFastPath = 1
		work.AppendOnlyCollisionFastPath = 1
		return work, nil
	}

	// Identity collisions are rare in the production append path. Materialize
	// an independently pinned exact candidate so every established coalescing
	// and conflict rule remains authoritative.
	temporary := NewStableResourceSetBuilder()
	op.cleanup.temporary = temporary
	if err := cloneStableResourceViewsIntoBuilder(temporary, builder.kindViews); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	temporaryChildBuilder := NewStableResourceSetBuilder()
	op.cleanup.temporary2 = temporaryChildBuilder
	if err := cloneStableResourceViewsIntoBuilder(temporaryChildBuilder, child.kindViews); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	temporaryChildBuilder.mu.Lock()
	temporaryChild := &StableResourceSet{ordinaryMetadata: true, entries: temporaryChildBuilder.entries}
	temporaryChild.owner.Store(uint32(ResourceOwnerBuilder))
	op.cleanup.temporarySet = temporaryChild
	temporaryChildBuilder.entries = nil
	temporaryChildBuilder.closed = true
	temporaryChildBuilder.mu.Unlock()

	work, err := temporary.mergeAppendOnlyLogicalObligationsFlatWithCleanupV1(temporaryChild, mutation, &op.cleanup)
	work.AppendOnlyCollisionFallbacks = 1
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, err
	}
	work.CopiedEntries += uint64(stableResourceKindViewCount(builder.kindViews) + stableResourceKindViewCount(child.kindViews))
	work.PhysicalHandleShares += uint64(stableResourceKindViewCount(builder.kindViews) + stableResourceKindViewCount(child.kindViews))
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	oldBuilderViews, oldChildViews := builder.kindViews, child.kindViews
	temporary.mu.Lock()
	builder.entries = temporary.entries
	builder.indexed = temporary.indexed
	builder.kindViews = nil
	temporary.entries = nil
	temporary.indexed = nil
	temporary.closed = true
	temporary.mu.Unlock()
	child.kindViews = nil
	child.entries = nil
	childLocked = false
	child.mu.Unlock()
	builderLocked = false
	builder.mu.Unlock()
	op.cleanup.views = oldBuilderViews
	op.cleanup.views2 = oldChildViews
	return work, nil
}

type certifiedAppendOnlyPhysicalCoalescePlan struct {
	views           stableKindViews
	releaseIncoming stableKindViews
	staged          stableKindViews
}

// abandon releases only independently pinned staged roots. Provisional concat
// nodes never retain their inputs and must simply be discarded before adoption.
func (plan *certifiedAppendOnlyPhysicalCoalescePlan) abandon() {
	if plan != nil {
		releaseStableResourceKindViews(plan.staged)
		plan.staged = nil
	}
}

// certifiedAppendOnlyPhysicalCoalesce preflights a small producer containing
// distinct entries and unambiguous same-logical/same-physical appends. The
// persistent indexes are canonical. Collision-only producer roots are dropped;
// all-distinct roots transfer directly; mixed roots pin only their distinct
// delta so repeated collisions cannot accumulate hidden tokens or descriptors.
func certifiedAppendOnlyPhysicalCoalesce(target, incoming stableKindViews, mutation StableLogicalObligationMutation) (plan *certifiedAppendOnlyPhysicalCoalescePlan, work StableResourceClosureWork, certified bool, result error) {
	var cleanup stableBuilderAddCleanup
	defer func() { result = errors.Join(result, cleanup.release()) }()
	return certifiedAppendOnlyPhysicalCoalesceWithCleanupV1(target, incoming, mutation, &cleanup)
}
func certifiedAppendOnlyPhysicalCoalesceWithCleanupV1(target, incoming stableKindViews, mutation StableLogicalObligationMutation, cleanup *stableBuilderAddCleanup) (*certifiedAppendOnlyPhysicalCoalescePlan, StableResourceClosureWork, bool, error) {
	if err := requireOrdinaryStableResourceInputs(nil, target); err != nil {
		return nil, StableResourceClosureWork{}, false, err
	}
	if err := requireOrdinaryStableResourceInputs(nil, incoming); err != nil {
		return nil, StableResourceClosureWork{}, false, err
	}
	// Even a few shared files can retain many logical obligations. Keep their
	// existing indexes instead of selecting the flat path by physical count.
	retainedWork := stableResourceKindViewCount(target) + stableResourceKindViewCount(incoming)
	for _, stableBinding1 := range target.records() {
		view := stableBinding1.value
		if view.directory != nil {
			retainedWork = stableResourceEntryLinearLookupLimit + 1
			break
		}
		if retainedWork > stableResourceEntryLinearLookupLimit {
			break
		}
		retainedWork += view.logicalObligationCount
	}
	if retainedWork <= stableResourceEntryLinearLookupLimit {
		return nil, StableResourceClosureWork{}, false, nil
	}
	if stableResourceKindViewCount(incoming) > stableResourceEntryLinearLookupLimit {
		return nil, StableResourceClosureWork{}, false, nil
	}
	_, work, err := validateAppendOnlyProducerViews(incoming, mutation)
	if err != nil {
		return nil, work, true, err
	}
	nextViews := newStableKindViews(target.len())
	for _, stableBinding2 := range target.records() {
		kind := stableBinding2.key
		current := stableBinding2.value
		nextViews.set(kind, current)
	}
	distinct := make(map[ResourceKind][]*stableResourceEntry)
	collisions := 0
	var preflightErr error
	certified := true
	rangeStableResourceKindViews(incoming, func(child *stableResourceEntry) bool {
		view, hadKind := nextViews.lookup(child.token.kind)
		replacedLogicalCommitments := false
		if !hadKind {
			view.reachability = newStableReachabilitySet(0)
		}
		existing := findStableResourceLogical(view.logical, child.token.logicalKey())
		if existing != nil && !existing.token.samePhysicalIdentity(child.token) {
			preflightErr = fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, child.token.logicalKey())
			return false
		}
		admissible, complete, err := stableResourceViewsAdmitLogicalObligations(nextViews, child, existing, nil, &work)
		if err != nil {
			preflightErr = err
			return false
		}
		if !complete || !admissible {
			certified = false
			return false
		}

		physicalKey := child.token.physicalIdentityKey()
		var candidates []*stableResourceEntry
		for _, stableBinding3 := range nextViews.records() {
			kind := stableBinding3.key
			other := stableBinding3.value
			matches := findStableResourcePhysical(other.physical, physicalKey)
			if len(matches) == 0 {
				continue
			}
			if kind != child.token.kind {
				for _, match := range matches {
					coalesce, err := stableResourcesCoalesce(match.token, child.token)
					if err != nil {
						preflightErr = err
						return false
					}
					if coalesce {
						certified = false
						return false
					}
				}
				continue
			}
			if candidates != nil {
				certified = false
				return false
			}
			candidates = matches
		}
		work.PhysicalEntryLookupProbes++
		switch len(candidates) {
		case 0:
			if existing != nil {
				preflightErr = fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, child.token.logicalKey())
				return false
			}
			view.logical = insertStableResourceLogical(view.logical, child)
			view.physical = insertStableResourcePhysical(view.physical, child)
			view.count++
			distinct[child.token.kind] = append(distinct[child.token.kind], child)
			work.PhysicalEntryLookupAdmissions++
		case 1:
			work.PhysicalEntryLookupComparisons++
			if existing == nil || candidates[0] != existing {
				certified = false
				return false
			}
			coalesce, coalesceErr := stableResourcesCoalesce(existing.token, child.token)
			if coalesceErr != nil {
				preflightErr = coalesceErr
				return false
			}
			if !coalesce || !existing.token.namespaceCompatible(child.token) || !frontierCompatible(existing.frontier, child.frontier) {
				preflightErr = fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.token.identityKey())
				return false
			}
			// Representative replacement remains on the exact path because the
			// canonical token would otherwise move between ownership ropes.
			if existing.token.namespace == nil && child.token.namespace != nil {
				certified = false
				return false
			}
			nextEntry := cloneStableResourceEntry(*existing)
			nextEntry.logicalObligations, preflightErr = nextEntry.logicalObligations.appendCertified(child.logicalObligations.deltaSlice(), &work)
			if preflightErr != nil {
				return false
			}
			if view.logicalObligationCount < existing.logicalObligations.count {
				preflightErr = ErrUnresolvedResource
				return false
			}
			commitments := cloneStableLogicalObligationCommitments(view.logicalCommitments)
			for field, old := range existing.logicalObligations.commitments {
				commitment, ok := commitments[field]
				if !ok || commitment.count < old.count {
					preflightErr = ErrUnresolvedResource
					return false
				}
				commitment.count -= old.count
				subtractStableLogicalObligationDigest(&commitment.sum, old.sum)
				commitments[field] = commitment
			}
			view.logicalCommitments = addStableLogicalObligationCommitments(commitments, nextEntry.logicalObligations.commitments)
			view.logicalObligationCount += nextEntry.logicalObligations.count - existing.logicalObligations.count
			replacedLogicalCommitments = true
			nextEntry.frontier = maxFrontier(nextEntry.frontier, child.frontier)
			mergeStableResourceDescriptorIdentity(&nextEntry, child.logicalLane, child.resourceID, child.diagnosticPath)
			for _, stableBinding4 := range child.reachability.records() {
				field := stableBinding4.key
				nextEntry.reachability.set(field, struct{}{})
			}
			view.logical = insertStableResourceLogical(view.logical, &nextEntry)
			view.physical = replaceStableResourcePhysical(view.physical, existing, &nextEntry)
			collisions++
		default:
			certified = false
			return false
		}
		child.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			if view.directory != nil {
				_, found, err := lookupStableLogicalMembershipV2(view.logicalMembership, view.directory, view.logical, child.token.kind, obligation, &work)
				if err != nil {
					preflightErr = err
					return false
				}
				if found {
					return true
				}
			}
			var admitted bool
			view.logicalMembership, admitted = insertStableLogicalMembership(view.logicalMembership, obligation, &work)
			if admitted {
				view.logicalMembershipCount++
			}
			return true
		})
		if preflightErr != nil {
			return false
		}
		view.reachability = cloneReachabilityUnion(view.reachability, child.reachability)
		if !replacedLogicalCommitments {
			view.logicalCommitments = addStableLogicalObligationCommitments(view.logicalCommitments, child.logicalObligations.commitments)
			view.logicalObligationCount += child.logicalObligations.count
		}
		nextViews.set(child.token.kind, view)
		return true
	})
	if preflightErr != nil {
		return nil, work, true, preflightErr
	}
	if !certified || collisions == 0 {
		return nil, work, false, nil
	}
	plan := &certifiedAppendOnlyPhysicalCoalescePlan{
		views: nextViews, releaseIncoming: newStableKindViews(0),
		staged: newStableKindViews(0),
	}
	// Root concatenation allocates lineage but does not retain or mutate either
	// input. Mixed roots clone only their distinct delta before the ownership CAS.
	for _, stableBinding5 := range incoming.records() {
		kind := stableBinding5.key
		child := stableBinding5.value
		view := plan.views.get(kind)
		current, hadTarget := target.lookup(kind)
		if !hadTarget && len(distinct[kind]) != child.count {
			return plan, work, true, ErrUnresolvedResource
		}
		switch len(distinct[kind]) {
		case child.count:
			if hadTarget {
				view.root = concatOwnedStableResourceEntryNodes(current.root, child.root)
			} else {
				view.root = child.root
			}
		case 0:
			view.root = current.root
			plan.releaseIncoming.set(kind, child)
		default:
			staged, cloneErr := cloneStableResourceEntriesToKindViewWithCleanupV1(distinct[kind], cleanup)
			if cloneErr != nil {
				return plan, work, true, cloneErr
			}
			plan.staged.set(kind, staged)
			plan.releaseIncoming.set(kind, child)
			for _, original := range distinct[kind] {
				replacement := findStableResourceLogical(staged.logical, original.token.logicalKey())
				view.logical = insertStableResourceLogical(view.logical, replacement)
				view.physical = replaceStableResourcePhysical(view.physical, original, replacement)
			}
			view.root = concatOwnedStableResourceEntryNodes(current.root, staged.root)
			work.CopiedEntries += uint64(len(distinct[kind]))
			work.PhysicalHandleShares += uint64(len(distinct[kind]))
		}
		plan.views.set(kind, view)
	}
	return plan, work, true, nil
}

func cloneStableResourceEntriesToKindView(entries []*stableResourceEntry) (view stableResourceKindView, result error) {
	var cleanup stableBuilderAddCleanup
	defer func() { result = errors.Join(result, cleanup.release()) }()
	return cloneStableResourceEntriesToKindViewWithCleanupV1(entries, &cleanup)
}
func cloneStableResourceEntriesToKindViewWithCleanupV1(entries []*stableResourceEntry, cleanup *stableBuilderAddCleanup) (stableResourceKindView, error) {
	for _, entry := range entries {
		if stableResourceEntryHasMetadataAccount(entry) {
			return stableResourceKindView{}, ErrStableMetadataShapeUnsupported
		}
	}
	builder := NewStableResourceSetBuilder()
	builder.stagingNextV1 = cleanup.stagedBuilders
	cleanup.stagedBuilders = builder
	for _, entry := range entries {
		if err := cloneStableResourceEntryIntoBuilder(builder, entry); err != nil {
			return stableResourceKindView{}, err
		}
	}
	builder.mu.Lock()
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		builder.mu.Unlock()
		return stableResourceKindView{}, err
	}
	if len(entries) == 0 || builder.kindViews.len() != 1 {
		builder.mu.Unlock()
		return stableResourceKindView{}, ErrUnresolvedResource
	}
	view, ok := builder.kindViews.lookup(entries[0].token.kind)
	if !ok {
		builder.mu.Unlock()
		return stableResourceKindView{}, ErrUnresolvedResource
	}
	builder.kindViews = nil
	builder.closed = true
	builder.mu.Unlock()
	return view, nil
}

func cloneReachabilityUnion(left, right stableReachabilitySet) stableReachabilitySet {
	result := newStableReachabilitySet(left.len() + right.len())
	for _, stableBinding1 := range left.records() {
		field := stableBinding1.key
		result.set(field, struct{}{})
	}
	for _, stableBinding2 := range right.records() {
		field := stableBinding2.key
		result.set(field, struct{}{})
	}
	return result
}

func validateAppendOnlyProducerViews(views stableKindViews, mutation StableLogicalObligationMutation) (StableLogicalObligationMutation, StableResourceClosureWork, error) {
	work := StableResourceClosureWork{}
	normalized, err := NormalizeStableLogicalObligationMutation(mutation)
	if err != nil {
		return normalized, work, err
	}
	work.LogicalObligationNormalizations = uint64(len(normalized.Added) + len(normalized.Removed))
	if len(normalized.Removed) != 0 {
		return normalized, work, fmt.Errorf("%w: append-only merge received removals", ErrResourceConflict)
	}
	work.RequirementFieldsInspected = uint64(len(normalized.ScopedFields))
	work.RequirementObligationsInspected = uint64(len(normalized.Added))
	work.NewlyAdmittedObligations = uint64(len(normalized.Added))
	work.NewlyAdmittedEntries = uint64(stableResourceKindViewCount(views))
	desired := make(map[StableLogicalObligation]struct{}, len(normalized.Added))
	for _, obligation := range normalized.Added {
		desired[obligation] = struct{}{}
	}
	scoped := newStableReachabilitySet(len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped.set(field, struct{}{})
	}
	seen := make(map[stableLogicalObligationIndex]StableLogicalObligation, len(desired))
	var validateErr error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		if entry.logicalObligations.directory != nil {
			validateErr = fmt.Errorf("%w: append-only producer contains retained directory state", ErrResourceConflict)
			return false
		}
		work.SourceEntriesInspected++
		for _, stableBinding1 := range entry.reachability.records() {
			field := stableBinding1.key
			if _, applies := scoped.lookup(field); applies && entry.logicalObligations.commitments[field].count == 0 {
				validateErr = fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
				return false
			}
		}
		entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			work.SourceObligationsInspected++
			if _, applies := scoped.lookup(obligation.Reachability); !applies {
				validateErr = fmt.Errorf("%w: append-only producer obligation uses unscoped field %q", ErrResourceConflict, obligation.Reachability)
				return false
			}
			if _, ok := desired[obligation]; !ok {
				validateErr = fmt.Errorf("%w: append-only producer supplied unannounced logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			key := stableLogicalObligationKey(obligation)
			if existing, duplicate := seen[key]; duplicate {
				validateErr = fmt.Errorf("%w: append-only producer logical obligation key %+v repeats immutable payload %+v as %+v", ErrResourceConflict, key, existing, obligation)
				return false
			}
			seen[key] = obligation
			return true
		})
		return validateErr == nil
	})
	if validateErr != nil {
		return normalized, work, validateErr
	}
	if len(seen) != len(desired) {
		return normalized, work, fmt.Errorf("%w: append-only producer admitted %d of %d declared logical obligations", ErrUnresolvedResource, len(seen), len(desired))
	}
	return normalized, work, nil
}

func (builder *StableResourceSetBuilder) mergeAppendOnlyLogicalObligationsFlat(child *StableResourceSet, mutation StableLogicalObligationMutation) (StableResourceClosureWork, error) {
	return builder.mergeAppendOnlyLogicalObligationsFlatWithCleanupV1(child, mutation, nil)
}
func (builder *StableResourceSetBuilder) mergeAppendOnlyLogicalObligationsFlatWithCleanupV1(child *StableResourceSet, mutation StableLogicalObligationMutation, inherited *stableBuilderAddCleanup) (resultWork StableResourceClosureWork, result error) {
	if err := requireOrdinaryStableResourceBuilderInputs(builder, child); err != nil {
		return StableResourceClosureWork{}, err
	}
	work := StableResourceClosureWork{}
	if builder == nil || child == nil {
		return work, ErrResourceOwnership
	}
	normalized, err := NormalizeStableLogicalObligationMutation(mutation)
	if err != nil {
		return work, err
	}
	work.LogicalObligationNormalizations = uint64(len(normalized.Added) + len(normalized.Removed))
	if len(normalized.Removed) != 0 {
		return work, fmt.Errorf("%w: append-only merge received removals", ErrResourceConflict)
	}
	work.RequirementFieldsInspected = uint64(len(normalized.ScopedFields))
	work.RequirementObligationsInspected = uint64(len(normalized.Added))
	work.NewlyAdmittedObligations = uint64(len(normalized.Added))
	work.NewlyAdmittedEntries = uint64(child.Len())
	desired := make(map[StableLogicalObligation]struct{}, len(normalized.Added))
	for _, obligation := range normalized.Added {
		desired[obligation] = struct{}{}
	}
	scoped := newStableReachabilitySet(len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped.set(field, struct{}{})
	}

	builderLocked, childLocked := true, false
	builder.mu.Lock()
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.hasFailedCleanupLockedV1() {
		builderLocked = false
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	child.mu.Lock()
	childLocked = true
	if err := requireIdleStableResourceOperationLockedV1(builder, child); err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	op, err := builder.beginCleanupOwnerLockedV1(nil, child)
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}
	defer func() {
		if v := recover(); v != nil {
			if childLocked {
				childLocked = false
				child.mu.Unlock()
			}
			if builderLocked {
				builderLocked = false
				builder.mu.Unlock()
			}
			builder.recordCleanupPanicV1(op)
			panic(v)
		}
		if inherited != nil { builder.pauseCleanupOwnerV1(op) } else { result = errors.Join(result, builder.finishCleanupOwnerV1(op)) }
	}()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	work.SourceEntriesInspected += uint64(len(builder.entries) + len(child.entries))
	work.CopiedEntries += uint64(len(builder.entries))
	work.RetainedEntries += uint64(len(builder.entries))
	for _, entry := range builder.entries {
		work.RetainedObligations += uint64(entry.logicalObligations.count)
	}
	seen := make(map[stableLogicalObligationIndex]StableLogicalObligation, len(desired))
	for _, entry := range child.entries {
		if entry.logicalObligations.directory != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return work, ErrResourceConflict
		}
		for _, stableBinding1 := range entry.reachability.records() {
			field := stableBinding1.key
			if _, applies := scoped.lookup(field); !applies {
				continue
			}
			if entry.logicalObligations.commitments[field].count == 0 {
				childLocked = false
				child.mu.Unlock()
				builderLocked = false
				builder.mu.Unlock()
				return work, fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
			}
		}
		entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			work.SourceObligationsInspected++
			if _, applies := scoped.lookup(obligation.Reachability); !applies {
				err = fmt.Errorf("%w: append-only producer obligation uses unscoped field %q", ErrResourceConflict, obligation.Reachability)
				return false
			}
			if _, ok := desired[obligation]; !ok {
				err = fmt.Errorf("%w: append-only producer supplied unannounced logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			key := stableLogicalObligationKey(obligation)
			if existing, duplicate := seen[key]; duplicate {
				err = fmt.Errorf("%w: append-only producer logical obligation key %+v repeats immutable payload %+v as %+v", ErrResourceConflict, key, existing, obligation)
				return false
			}
			seen[key] = obligation
			return true
		})
		if err != nil {
			childLocked = false
			child.mu.Unlock()
			builderLocked = false
			builder.mu.Unlock()
			return work, err
		}
	}
	if len(seen) != len(desired) {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, fmt.Errorf("%w: append-only producer admitted %d of %d declared logical obligations", ErrUnresolvedResource, len(seen), len(desired))
	}

	merged := cloneStableResourceEntries(builder.entries)
	lookup := stableResourceEntryLookup{}
	if len(merged)+len(child.entries) > stableResourceEntryLinearLookupLimit {
		lookup = newStableResourceEntryLookup(merged)
	}
	for _, incoming := range child.entries {
		if lookup.logical.less == nil {
			err = mergeAppendOnlyViewEntryLinear(&merged, incoming, &work)
			if err != nil {
				break
			}
			continue
		}
		logicalKey := incoming.token.logicalKey()
		coalesced := false
		work.PhysicalEntryLookupProbes++
		if existingIndex, ok := lookup.logical.lookup(logicalKey); ok {
			existing := merged[existingIndex].token
			if !existing.samePhysicalIdentity(incoming.token) {
				err = fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
			}
		}
		if err != nil {
			break
		}
		iterator, iteratorErr := lookup.physicalIterator(merged, incoming.token)
		if iteratorErr != nil {
			err = iteratorErr
			break
		}
		for {
			i, ok := iterator.Next()
			if !ok {
				break
			}
			entry := &merged[i]
			existing := entry.token
			work.PhysicalEntryLookupComparisons++
			var canCoalesce bool
			canCoalesce, err = stableResourcesCoalesce(existing, incoming.token)
			if err != nil {
				break
			}
			if !canCoalesce {
				continue
			}
			if !existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
				err = fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
				break
			}
			incomingValues := incoming.logicalObligations.deltaSlice()
			entry.logicalObligations, err = entry.logicalObligations.appendCertified(incomingValues, &work)
			if err != nil {
				break
			}
			entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
			mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
			for _, stableBinding2 := range incoming.reachability.records() {
				field := stableBinding2.key
				entry.reachability.set(field, struct{}{})
			}
			if existing.namespace == nil && incoming.token.namespace != nil {
				entry.token = incoming.token
				lookup.replaceRepresentative(i, existing, incoming.token)
				entry.pins = nil
				entry.pinIndex = nil
			}
			coalesced = true
			break
		}
		if err != nil {
			break
		}
		if !coalesced {
			if overlapErr := rejectDistinctLogicalObligationOverlap(merged, incoming); overlapErr != nil {
				err = overlapErr
				break
			}
			merged = append(merged, cloneStableResourceEntry(incoming))
			lookup.add(merged, len(merged)-1)
			work.PhysicalEntryLookupAdmissions++
		}
	}
	if err != nil {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, err
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		childLocked = false
		child.mu.Unlock()
		builderLocked = false
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	dropped := droppedStableResourceTokens(merged, builder.entries, child.entries)
	builder.entries = merged
	if lookup.logical.less == nil {
		builder.indexed = nil
	} else {
		if builder.indexed == nil {
			builder.indexed = &stableResourceBuilderIndexedState{}
		}
		builder.indexed.lookup = lookup
	}
	child.entries = nil
	// This caller-created shell had only entries. Discharge its exact role
	// only after this CAS transferred them and every owned backing is empty.
	if inherited != nil && inherited.temporarySet == child && child.kindViews == nil && child.extras == nil && child.metadataAccount == nil && child.metadataBacking == 0 && !child.finiteMetadata && child.ordinaryMetadata && child.pinHighWater == nil {
		inherited.temporarySet = nil
	}
	childLocked = false
	child.mu.Unlock()
	builderLocked = false
	builder.mu.Unlock()
	op.cleanup.dropped = dropped
	work.AppendOnlyFastPath = 1
	return work, nil
}

func droppedStableResourceTokens(kept []stableResourceEntry, sources ...[]stableResourceEntry) []*StableResourceToken {
	retained := make(map[*StableResourceToken]struct{}, len(kept))
	for _, entry := range kept {
		retained[entry.token] = struct{}{}
	}
	var dropped []*StableResourceToken
	seen := make(map[*StableResourceToken]struct{})
	for _, entries := range sources {
		for _, entry := range entries {
			if _, ok := retained[entry.token]; ok {
				continue
			}
			if _, ok := seen[entry.token]; ok {
				continue
			}
			seen[entry.token] = struct{}{}
			dropped = append(dropped, entry.token)
		}
	}
	return dropped
}

func (builder *StableResourceSetBuilder) Freeze() (*StableResourceSet, error) {
	if builder == nil {
		return nil, ErrResourceOwnership
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()
	if err := builder.requireOrdinaryMetadataLocked(); err != nil {
		return nil, err
	}
	if builder.closed || builder.abandoned || builder.cleanupRunning || builder.pendingCleanup != nil {
		// Freeze transfers entries/views, never operation controls. A remaining
		// running, paused, failed or uncertain owner must drain on this builder.
		return nil, ErrResourceOwnership
	}
	covered := newStableReachabilitySet(0)
	if builder.kindViews != nil {
		for _, stableBinding1 := range builder.kindViews.records() {
			view := stableBinding1.value
			for _, stableBinding2 := range view.reachability.records() {
				field := stableBinding2.key
				covered.set(field, struct{}{})
			}
		}
	}
	for _, entry := range builder.entries {
		if err := entry.token.namespace.validateStable(); err != nil {
			return nil, err
		}
		for _, stableBinding3 := range entry.reachability.records() {
			field := stableBinding3.key
			covered.set(field, struct{}{})
		}
	}
	for _, stableBinding4 := range builder.required.records() {
		required := stableBinding4.key
		if _, ok := covered.lookup(required); !ok {
			return nil, fmt.Errorf("%w: missing reachability field %q", ErrUnresolvedResource, required)
		}
	}
	if builder.kindViews != nil {
		views := builder.kindViews
		set := &StableResourceSet{
			ordinaryMetadata: true,
			kindViews:        views,
			pinHighWater:     stableResourcePinCountsFromViews(views),
		}
		set.owner.Store(uint32(ResourceOwnerBuilder))
		builder.kindViews = nil
		builder.closed = true
		return set, nil
	}
	entries := cloneStableResourceEntries(builder.entries)
	sortStableResourceEntries(entries)
	views, err := buildStableResourceKindViews(entries)
	if err != nil {
		return nil, err
	}
	set := &StableResourceSet{
		ordinaryMetadata: true,
		entries:          entries, kindViews: views,
		pinHighWater: stableResourcePinCounts(entries),
	}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	builder.entries = nil
	builder.closed = true
	return set, nil
}

func (builder *StableResourceSetBuilder) Abandon() { _ = builder.AbandonCheckedV1() }
func (builder *StableResourceSetBuilder) AbandonCheckedV1() (result error) {
	if builder == nil {
		return nil
	}
	builder.mu.Lock()
	if builder.abandoned || builder.closed && builder.pendingCleanup == nil {
		builder.mu.Unlock()
		return nil
	}
	if builder.cleanupRunning {
		builder.mu.Unlock()
		return ErrStableResourceOperationBusy
	}
	builder.cleanupRunning = true
	entries, views := builder.entries, builder.kindViews
	builder.mu.Unlock()
	defer func() { builder.mu.Lock(); builder.cleanupRunning = false; builder.mu.Unlock() }()
	builder.mu.Lock()
	for op := builder.pendingCleanup; op != nil; op = op.next {
		if op.running {
			builder.mu.Unlock()
			return ErrStableResourceOperationBusy
		}
	}
	op := builder.pendingCleanup
	builder.mu.Unlock()
	for op != nil {
		builder.mu.Lock()
		next := op.next
		op.running = true
		builder.mu.Unlock()
		err := builder.finishCleanupOwnerV1(op)
		result = errors.Join(result, err)
		if err != nil {
			return result
		}
		op = next
	}
	if views != nil {
		if err := releaseStableResourceKindViewsCursorV1(views, &builder.cleanupViewCursor, &builder.cleanupViewStarted); err != nil {
			return err
		}
	}
	for i := range entries {
		token := entries[i].token
		if token.CleanupCompleteV1() {
			continue
		}
		err := token.releaseFrom(ResourceOwnerBuilder)
		result = errors.Join(result, err)
		if !token.CleanupCompleteV1() {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
	}
	builder.mu.Lock()
	builder.entries = nil
	builder.kindViews = nil
	builder.abandoned = true
	builder.state = ResourceOwnerReleased
	builder.mu.Unlock()
	return result
}

func cloneStableResourceEntries(entries []stableResourceEntry) []stableResourceEntry {
	out := make([]stableResourceEntry, len(entries))
	for i, entry := range entries {
		out[i] = cloneStableResourceEntry(entry)
	}
	return out
}

func sortStableResourceEntries(entries []stableResourceEntry) {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i].token, entries[j].token
		if left.kind != right.kind {
			return left.kind < right.kind
		}
		if entries[i].logicalLane != entries[j].logicalLane {
			return entries[i].logicalLane < entries[j].logicalLane
		}
		if entries[i].resourceID != entries[j].resourceID {
			return entries[i].resourceID < entries[j].resourceID
		}
		if left.generation != right.generation {
			return left.generation < right.generation
		}
		return bytes.Compare(left.identity.ObjectID[:], right.identity.ObjectID[:]) < 0
	})
}

// stableResourceSetExtras holds optional logical evidence and directory leases.
// Ordinary physical sets need neither. Owned clones have independent sidecars;
// immutable references remain diagnostic after the one ownership release.
type stableResourceSetExtras struct {
	logicalMembershipEvidence map[ResourceKind]stableLogicalMembershipEvidence
	empty                     *DependencyDirectoryV2
	physical                  []*DependencyDirectoryV2
}

// These accessors require the set lock or exclusive unpublished ownership.
func (set *StableResourceSet) emptyDependencyDirectoryLocked() *DependencyDirectoryV2 {
	if set.extras == nil {
		return nil
	}
	return set.extras.empty
}

func (set *StableResourceSet) physicalDependencyDirectoriesLocked() []*DependencyDirectoryV2 {
	if set.extras == nil {
		return nil
	}
	return set.extras.physical
}

func (set *StableResourceSet) logicalMembershipEvidenceLocked() map[ResourceKind]stableLogicalMembershipEvidence {
	if set.extras == nil {
		return nil
	}
	return set.extras.logicalMembershipEvidence
}

type StableResourceSet struct {
	terminalReleasing      bool   // exact synchronous release reservation, no retained consumer
	activeOperations       uint64 // immutable set backing retained by synchronous Flush/Sync
	releaseCursor          int
	releaseViewStarted     bool
	releaseDirectoriesDone bool
	metadataAccount        StableMetadataAccount
	metadataBacking        uint64
	finiteMetadata         bool // immutable generic-export refusal, including after terminal scrub
	ordinaryMetadata       bool // constructor-stamped absence of finite provenance
	extras                 *stableResourceSetExtras
	mu                     sync.Mutex
	entries                []stableResourceEntry
	kindViews              stableKindViews
	pinHighWater           map[ResourceKind]uint64
	owner                  atomic.Uint32
	// physicalOnly is an explicit durability/reachability capability. It carries
	// no complete logical metadata and cannot be used for publication proofs.
	physicalOnly bool
}

func (set *StableResourceSet) rangeEntries(visit func(*stableResourceEntry) bool) bool {
	if set == nil || visit == nil {
		return true
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return set.rangeEntriesLocked(visit)
}

func (set *StableResourceSet) rangeEntriesLocked(visit func(*stableResourceEntry) bool) bool {
	if set.kindViews != nil {
		return rangeStableResourceKindViews(set.kindViews, visit)
	}
	for i := range set.entries {
		if !visit(&set.entries[i]) {
			return false
		}
	}
	return true
}

func (set *StableResourceSet) entrySnapshotLocked() []stableResourceEntry {
	if set.kindViews == nil {
		return set.entries
	}
	entries := make([]stableResourceEntry, 0, stableResourceKindViewCount(set.kindViews))
	rangeStableResourceKindViews(set.kindViews, func(entry *stableResourceEntry) bool {
		entries = append(entries, *entry)
		return true
	})
	return entries
}

func cloneStableResourceSetKindView(source *StableResourceSet, excluded ...ResourceKind) (*StableResourceSet, bool, error) {
	if err := source.rejectAccountedCloneInput(); err != nil {
		return nil, false, err
	}
	if source == nil {
		return nil, true, nil
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	if source.physicalOnly {
		return nil, false, ErrResourceOwnership
	}
	source.mu.Lock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		source.mu.Unlock()
		return nil, false, ErrResourceOwnership
	}
	if source.emptyDependencyDirectoryLocked() != nil {
		if err := source.emptyDependencyDirectoryLocked().Retain(); err != nil {
			source.mu.Unlock()
			return nil, true, ErrResourceOwnership
		}
		set := &StableResourceSet{ordinaryMetadata: true, extras: &stableResourceSetExtras{empty: source.emptyDependencyDirectoryLocked()}}
		set.owner.Store(uint32(ResourceOwnerBuilder))
		source.mu.Unlock()
		return set, true, nil
	}
	if source.kindViews == nil {
		source.mu.Unlock()
		return nil, false, nil
	}
	views, ok := cloneStableResourceKindViews(source.kindViews, excludedKinds)
	source.mu.Unlock()
	if !ok {
		return nil, false, ErrResourceOwnership
	}
	set := &StableResourceSet{
		ordinaryMetadata: true,
		kindViews:        views,
		pinHighWater:     stableResourcePinCountsFromViews(views),
	}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set, true, nil
}

// StableLogicalObligationNamespaceScope identifies one exact logical namespace
// within a reachability field. Namespace is a nonempty opaque identifier, not a
// filesystem basename (for example, "docs/column-assets" is valid).
type StableLogicalObligationNamespaceScope struct {
	Field     ReachabilityField
	Namespace string
}

// StableLogicalObligationRequirements scopes an exact logical-reference closure
// for a later root publication. ScopedFields replaces a complete field across
// all namespaces; ScopedNamespaces replaces only the named field/namespace.
// An empty declared scope removes its old obligations, whereas an undeclared
// scope is retained. The two forms must not overlap on the same field.
type StableLogicalObligationRequirements struct {
	ScopedFields     []ReachabilityField
	ScopedNamespaces []StableLogicalObligationNamespaceScope
	Obligations      []StableLogicalObligation
	// commitments is derived at the normalization boundary and remains an
	// immutable-by-convention proof of the complete per-field requirement set.
	// It is intentionally package-private so callers cannot forge fast-path
	// authorization independently of the normalized obligations.
	commitments map[ReachabilityField]stableLogicalObligationCommitment
}

// StableLogicalObligationMutation is exact transition evidence supplied by a
// producer that already published a root-local mutation delta. Added
// obligations are admitted from the producer resource set; Removed obligations
// are filtered from the inherited closure. An empty Removed list is the only
// append-only fast-path authorization.
type StableLogicalObligationMutation struct {
	ScopedFields []ReachabilityField
	Added        []StableLogicalObligation
	Removed      []StableLogicalObligation
}

func NormalizeStableLogicalObligationMutation(mutation StableLogicalObligationMutation) (StableLogicalObligationMutation, error) {
	added, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: mutation.ScopedFields,
		Obligations:  mutation.Added,
	})
	if err != nil {
		return StableLogicalObligationMutation{}, err
	}
	removed, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: mutation.ScopedFields,
		Obligations:  mutation.Removed,
	})
	if err != nil {
		return StableLogicalObligationMutation{}, err
	}
	addedSet := make(map[StableLogicalObligation]struct{}, len(added.Obligations))
	for _, obligation := range added.Obligations {
		addedSet[obligation] = struct{}{}
	}
	for _, obligation := range removed.Obligations {
		if _, both := addedSet[obligation]; both {
			return StableLogicalObligationMutation{}, fmt.Errorf("%w: logical obligation appears in both added and removed mutation sets", ErrResourceConflict)
		}
	}
	return StableLogicalObligationMutation{
		ScopedFields: added.ScopedFields,
		Added:        added.Obligations,
		Removed:      removed.Obligations,
	}, nil
}

func MergeStableLogicalObligationMutations(left, right StableLogicalObligationMutation) (StableLogicalObligationMutation, error) {
	return NormalizeStableLogicalObligationMutation(StableLogicalObligationMutation{
		ScopedFields: append(append([]ReachabilityField(nil), left.ScopedFields...), right.ScopedFields...),
		Added:        append(append([]StableLogicalObligation(nil), left.Added...), right.Added...),
		Removed:      append(append([]StableLogicalObligation(nil), left.Removed...), right.Removed...),
	})
}

// ValidateStableLogicalObligationMutationFinalRequirements checks mutation-
// local authorization against an already-normalized complete requirement set
// without scanning or copying retained history. Added obligations must appear
// in the candidate closure and removed obligations must not. DB registration
// normalizes the complete requirements before this check.
func ValidateStableLogicalObligationMutationFinalRequirements(mutation StableLogicalObligationMutation, requirements StableLogicalObligationRequirements) error {
	normalized, err := NormalizeStableLogicalObligationMutation(mutation)
	if err != nil {
		return err
	}
	if len(normalized.ScopedFields) == 0 {
		return nil
	}
	requirementFields := newStableReachabilitySet(len(requirements.ScopedFields))
	for _, field := range requirements.ScopedFields {
		requirementFields.set(field, struct{}{})
	}
	for _, scope := range requirements.ScopedNamespaces {
		requirementFields.set(scope.Field, struct{}{})
	}
	inScope := func(obligation StableLogicalObligation) bool {
		for _, field := range requirements.ScopedFields {
			if field == obligation.Reachability {
				return true
			}
		}
		for _, scope := range requirements.ScopedNamespaces {
			if scope.Field == obligation.Reachability && scope.Namespace == obligation.Namespace {
				return true
			}
		}
		return false
	}
	for _, field := range normalized.ScopedFields {
		if _, ok := requirementFields.lookup(field); !ok {
			return fmt.Errorf("%w: mutation field %q absent from final requirements", ErrUnresolvedResource, field)
		}
	}
	contains := func(target StableLogicalObligation) bool {
		start := sort.Search(len(requirements.Obligations), func(index int) bool {
			return requirements.Obligations[index].Reachability >= target.Reachability
		})
		end := start + sort.Search(len(requirements.Obligations)-start, func(offset int) bool {
			return requirements.Obligations[start+offset].Reachability > target.Reachability
		})
		position := start + sort.Search(end-start, func(offset int) bool {
			return !stableLogicalObligationLess(requirements.Obligations[start+offset], target)
		})
		return position < end && requirements.Obligations[position] == target
	}
	for _, obligation := range normalized.Added {
		if !inScope(obligation) || !contains(obligation) {
			return fmt.Errorf("%w: added mutation obligation absent from final requirements %+v", ErrUnresolvedResource, obligation)
		}
	}
	for _, obligation := range normalized.Removed {
		if !inScope(obligation) {
			return fmt.Errorf("%w: removed mutation obligation outside final scopes %+v", ErrResourceConflict, obligation)
		}
		if contains(obligation) {
			return fmt.Errorf("%w: removed mutation obligation retained by final requirements %+v", ErrResourceConflict, obligation)
		}
	}
	return nil
}

// CertifyStableLogicalObligationMutationFinalRequirements proves that mutation
// is the complete transition from source to requirements for every scoped
// field. It aggregates immutable per-entry commitments and applies only the
// declared additions/removals, so retained obligation history is neither
// scanned nor copied. A false result is an authorization decline, not malformed
// state: callers must retain the exact full filter/validation fallback.
func CertifyStableLogicalObligationMutationFinalRequirements(source *StableResourceSet, mutation StableLogicalObligationMutation, requirements StableLogicalObligationRequirements, excluded ...ResourceKind) (bool, error) {
	if err := source.RequireMetadataExport(); err != nil {
		return false, err
	}
	if source != nil && source.physicalOnly {
		return false, ErrResourceOwnership
	}
	normalizedMutation, err := NormalizeStableLogicalObligationMutation(mutation)
	if err != nil {
		return false, err
	}
	// Per-field commitments cannot certify a namespace-local replacement.
	// Keep the exact filter/validation fallback for these requirements.
	if len(requirements.ScopedNamespaces) != 0 {
		return false, nil
	}
	if len(normalizedMutation.ScopedFields) == 0 {
		return false, nil
	}
	finalCommitments := requirements.commitments
	if finalCommitments == nil {
		// Requirements without a normalization-boundary commitment carry no
		// bounded completeness proof. Decline instead of reconstructing one by
		// scanning the complete final requirement history here.
		return false, nil
	}
	finalCount, countOK := stableLogicalObligationCommitmentCount(finalCommitments)
	if !countOK || finalCount != uint64(len(requirements.Obligations)) {
		return false, nil
	}
	for _, field := range normalizedMutation.ScopedFields {
		if _, ok := finalCommitments[field]; !ok {
			return false, nil
		}
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	baseCommitments := make(map[ReachabilityField]stableLogicalObligationCommitment, len(normalizedMutation.ScopedFields))
	if source != nil {
		source.mu.Lock()
		defer source.mu.Unlock()
		owner := ResourceOwnerState(source.owner.Load())
		if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
			return false, ErrResourceOwnership
		}
		for _, entry := range source.entrySnapshotLocked() {
			token := activeEntryToken(entry)
			if token == nil || token.released.Load() {
				return false, ErrResourceOwnership
			}
			if _, skip := excludedKinds[token.kind]; skip {
				continue
			}
			if entry.logicalObligations.count != 0 && entry.logicalObligations.commitments == nil {
				// A non-empty legacy/incomplete view has no bounded proof. Decline
				// rather than reconstructing it by scanning retained history.
				return false, nil
			}
			entryCount, entryCountOK := stableLogicalObligationCommitmentCount(entry.logicalObligations.commitments)
			if !entryCountOK || entryCount != uint64(entry.logicalObligations.count) {
				return false, nil
			}
			for _, field := range normalizedMutation.ScopedFields {
				commitment := baseCommitments[field]
				commitment.add(entry.logicalObligations.commitments[field])
				baseCommitments[field] = commitment
			}
		}
	}
	expected := cloneStableLogicalObligationCommitments(baseCommitments)
	if expected == nil {
		expected = make(map[ReachabilityField]stableLogicalObligationCommitment, len(normalizedMutation.ScopedFields))
	}
	for _, obligation := range normalizedMutation.Removed {
		commitment := expected[obligation.Reachability]
		if !commitment.removeObligation(obligation) {
			return false, nil
		}
		expected[obligation.Reachability] = commitment
	}
	for _, obligation := range normalizedMutation.Added {
		commitment := expected[obligation.Reachability]
		commitment.addObligation(obligation)
		expected[obligation.Reachability] = commitment
	}
	for _, field := range normalizedMutation.ScopedFields {
		if expected[field] != finalCommitments[field] {
			return false, nil
		}
	}
	return true, nil
}

// CertifyStableLogicalObligationAppendMutation binds an exact append mutation
// to the capture-time source and producer commitments without materializing the
// retained obligation history. An entry may extend the same logical and physical
// source resource or introduce obligations proven absent by the source's complete
// aggregate membership index. Every other shape requires the caller's exact
// requirements fallback before ownership changes.
func CertifyStableLogicalObligationAppendMutation(source, producer *StableResourceSet, mutation StableLogicalObligationMutation, excluded ...ResourceKind) (StableResourceClosureWork, bool, error) {
	if err := requireOrdinaryStableResourceSets(source, producer); err != nil {
		return StableResourceClosureWork{}, false, err
	}
	if (source != nil && source.physicalOnly) || (producer != nil && producer.physicalOnly) {
		return StableResourceClosureWork{}, false, ErrResourceOwnership
	}
	var normalized StableLogicalObligationMutation
	var work StableResourceClosureWork
	var err error
	var producerViews stableKindViews
	if producer == nil {
		normalized, work, err = validateAppendOnlyProducerViews(nil, mutation)
	} else {
		producer.mu.Lock()
		owner := ResourceOwnerState(producer.owner.Load())
		if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
			producer.mu.Unlock()
			return work, false, ErrResourceOwnership
		}
		normalized, work, err = validateAppendOnlyProducerViews(producer.kindViews, mutation)
		producerViews = producer.kindViews
		producer.mu.Unlock()
	}
	if err != nil || len(normalized.ScopedFields) == 0 || len(normalized.Removed) != 0 {
		return work, false, nil
	}
	baseCommitments, complete, err := stableResourceSetLogicalObligationCommitments(source, normalized.ScopedFields, excluded...)
	if err != nil || !complete {
		return work, false, err
	}
	matches, matchErr := stableAppendProducerHasPhysicalPredecessors(source, producerViews, &work, excluded...)
	if matchErr != nil || !matches {
		return work, false, matchErr
	}
	producerCommitments, complete, err := stableResourceSetLogicalObligationCommitments(producer, normalized.ScopedFields)
	if err != nil || !complete {
		return work, false, err
	}
	addedCommitments := stableLogicalObligationRequirementCommitments(normalized.ScopedFields, normalized.Added)
	expectedFinal := addStableLogicalObligationCommitments(baseCommitments, addedCommitments)
	candidateFinal := addStableLogicalObligationCommitments(baseCommitments, producerCommitments)
	for _, field := range normalized.ScopedFields {
		if producerCommitments[field] != addedCommitments[field] || candidateFinal[field] != expectedFinal[field] {
			return work, false, nil
		}
	}
	return work, true, nil
}

func stableAppendProducerHasPhysicalPredecessors(source *StableResourceSet, producer stableKindViews, work *StableResourceClosureWork, excluded ...ResourceKind) (bool, error) {
	if stableResourceKindViewCount(producer) == 0 {
		return true, nil
	}
	if source == nil {
		return true, nil
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return false, ErrResourceOwnership
	}
	var matchErr error
	matches := rangeStableResourceKindViews(producer, func(entry *stableResourceEntry) bool {
		producerToken := activeEntryToken(*entry)
		if producerToken == nil || producerToken.released.Load() {
			matchErr = ErrResourceOwnership
			return false
		}
		if _, skip := excludedKinds[producerToken.kind]; skip {
			return true
		}
		var predecessor *stableResourceEntry
		if source.kindViews == nil {
			for i := range source.entries {
				candidate := &source.entries[i]
				candidateToken := activeEntryToken(*candidate)
				if candidateToken == nil || candidateToken.released.Load() {
					matchErr = ErrResourceOwnership
					return false
				}
				if candidateToken.logicalKey() == producerToken.logicalKey() {
					predecessor = candidate
					break
				}
			}
			if predecessor == nil {
				if len(source.entries) == 0 {
					return true
				}
				admissible, complete, err := stableLogicalMembershipEvidenceAdmits(source.logicalMembershipEvidenceLocked(), source.pinHighWater, entry, nil, excludedKinds, work)
				matchErr = err
				return err == nil && complete && admissible
			}
			predecessorToken := activeEntryToken(*predecessor)
			if predecessorToken == nil || predecessorToken.released.Load() {
				matchErr = ErrResourceOwnership
				return false
			}
			if predecessorToken.physicalIdentityKey() != producerToken.physicalIdentityKey() {
				return false
			}
			admissible, complete, err := stableLogicalMembershipEvidenceAdmits(source.logicalMembershipEvidenceLocked(), source.pinHighWater, entry, predecessor, excludedKinds, work)
			matchErr = err
			return err == nil && complete && admissible
		}
		if view, ok := source.kindViews.lookup(producerToken.kind); ok {
			predecessor = findStableResourceLogical(view.logical, producerToken.logicalKey())
		}

		if predecessor == nil {
			admissible, complete, err := stableResourceViewsAdmitLogicalObligations(source.kindViews, entry, nil, excludedKinds, work)
			matchErr = err
			return err == nil && complete && admissible
		}
		predecessorToken := activeEntryToken(*predecessor)
		if predecessorToken == nil || predecessorToken.released.Load() {
			matchErr = ErrResourceOwnership
			return false
		}
		if predecessorToken.physicalIdentityKey() != producerToken.physicalIdentityKey() {
			return false
		}
		admissible, complete, err := stableResourceViewsAdmitLogicalObligations(source.kindViews, entry, predecessor, excludedKinds, work)
		matchErr = err
		return err == nil && complete && admissible
	})
	return matches, matchErr
}

func stableResourceSetLogicalObligationCommitments(source *StableResourceSet, fields []ReachabilityField, excluded ...ResourceKind) (map[ReachabilityField]stableLogicalObligationCommitment, bool, error) {
	if err := source.RequireMetadataExport(); err != nil {
		return nil, false, err
	}
	result := make(map[ReachabilityField]stableLogicalObligationCommitment, len(fields))
	if source == nil {
		return result, true, nil
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, false, ErrResourceOwnership
	}
	if source.kindViews != nil {
		for _, stableBinding1 := range source.kindViews.records() {
			kind := stableBinding1.key
			view := stableBinding1.value
			if _, skip := excludedKinds[kind]; skip {
				continue
			}
			count, ok := stableLogicalObligationCommitmentCount(view.logicalCommitments)
			if !ok || count != uint64(view.logicalObligationCount) {
				return nil, false, nil
			}
			for _, field := range fields {
				commitment := result[field]
				commitment.add(view.logicalCommitments[field])
				result[field] = commitment
			}
		}
		return result, true, nil
	}
	for i := range source.entries {
		entry := &source.entries[i]
		token := activeEntryToken(*entry)
		if token == nil || token.released.Load() {
			return nil, false, ErrResourceOwnership
		}
		if _, skip := excludedKinds[token.kind]; skip {
			continue
		}
		count, ok := stableLogicalObligationCommitmentCount(entry.logicalObligations.commitments)
		if !ok || count != uint64(entry.logicalObligations.count) {
			return nil, false, nil
		}
		for _, field := range fields {
			commitment := result[field]
			commitment.add(entry.logicalObligations.commitments[field])
			result[field] = commitment
		}
	}
	return result, true, nil
}

// NormalizeStableLogicalObligationRequirements validates, de-duplicates, and
// deterministically sorts one exact logical-reference closure.
func NormalizeStableLogicalObligationRequirements(requirements StableLogicalObligationRequirements) (StableLogicalObligationRequirements, error) {
	fields := append([]ReachabilityField(nil), requirements.ScopedFields...)
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	uniqueFields := fields[:0]
	for _, field := range fields {
		if field == "" {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: empty scoped logical-obligation field", ErrUnresolvedResource)
		}
		if len(uniqueFields) == 0 || uniqueFields[len(uniqueFields)-1] != field {
			uniqueFields = append(uniqueFields, field)
		}
	}
	namespaces := append([]StableLogicalObligationNamespaceScope(nil), requirements.ScopedNamespaces...)
	sort.Slice(namespaces, func(i, j int) bool {
		if namespaces[i].Field != namespaces[j].Field {
			return namespaces[i].Field < namespaces[j].Field
		}
		return namespaces[i].Namespace < namespaces[j].Namespace
	})
	uniqueNamespaces := namespaces[:0]
	for _, scope := range namespaces {
		if scope.Field == "" || scope.Namespace == "" {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: incomplete logical-obligation namespace scope", ErrUnresolvedResource)
		}
		if len(uniqueNamespaces) == 0 || uniqueNamespaces[len(uniqueNamespaces)-1] != scope {
			uniqueNamespaces = append(uniqueNamespaces, scope)
		}
	}
	if len(uniqueFields) == 0 && len(uniqueNamespaces) == 0 {
		if len(requirements.Obligations) != 0 {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: logical obligations have no scoped fields", ErrUnresolvedResource)
		}
		return StableLogicalObligationRequirements{}, nil
	}
	scoped := newStableReachabilitySet(len(uniqueFields))
	byField := make(map[ReachabilityField][]StableLogicalObligation, len(uniqueFields))
	for _, field := range uniqueFields {
		scoped.set(field, struct{}{})
	}
	namespaceScopes := make(map[StableLogicalObligationNamespaceScope]struct{}, len(uniqueNamespaces))
	allFields := uniqueFields
	if len(uniqueNamespaces) != 0 {
		allFields = append([]ReachabilityField(nil), uniqueFields...)
	}
	for _, scope := range uniqueNamespaces {
		if _, global := scoped.lookup(scope.Field); global {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: overlapping global and namespace scope for %q", ErrResourceConflict, scope.Field)
		}
		namespaceScopes[scope] = struct{}{}
		if len(allFields) == 0 || allFields[len(allFields)-1] != scope.Field {
			allFields = append(allFields, scope.Field)
		}
	}
	sort.Slice(allFields, func(i, j int) bool { return allFields[i] < allFields[j] })
	for _, obligation := range requirements.Obligations {
		_, global := scoped.lookup(obligation.Reachability)
		_, local := namespaceScopes[StableLogicalObligationNamespaceScope{Field: obligation.Reachability, Namespace: obligation.Namespace}]
		if !global && !local {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: logical obligation field %q is not scoped", ErrResourceConflict, obligation.Reachability)
		}
		byField[obligation.Reachability] = append(byField[obligation.Reachability], obligation)
	}
	normalized := make([]StableLogicalObligation, 0, len(requirements.Obligations))
	for _, field := range allFields {
		obligations, err := normalizeStableLogicalObligations(byField[field], field)
		if err != nil {
			return StableLogicalObligationRequirements{}, err
		}
		normalized = append(normalized, obligations...)
	}
	var commitments map[ReachabilityField]stableLogicalObligationCommitment
	if len(uniqueNamespaces) == 0 {
		commitments = stableLogicalObligationRequirementCommitments(uniqueFields, normalized)
	}
	return StableLogicalObligationRequirements{
		ScopedFields:     append([]ReachabilityField(nil), uniqueFields...),
		ScopedNamespaces: uniqueNamespaces,
		Obligations:      append([]StableLogicalObligation(nil), normalized...),
		commitments:      commitments,
	}, nil
}

// MergeStableLogicalObligationRequirements forms the exact union of two
// independently supplied requirement sets.
func MergeStableLogicalObligationRequirements(left, right StableLogicalObligationRequirements) (StableLogicalObligationRequirements, error) {
	return NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields:     append(append([]ReachabilityField(nil), left.ScopedFields...), right.ScopedFields...),
		ScopedNamespaces: append(append([]StableLogicalObligationNamespaceScope(nil), left.ScopedNamespaces...), right.ScopedNamespaces...),
		Obligations:      append(append([]StableLogicalObligation(nil), left.Obligations...), right.Obligations...),
	})
}

type stableLogicalObligationRequirementIndex struct {
	scoped     stableReachabilitySet
	global     stableReachabilitySet
	namespaces map[StableLogicalObligationNamespaceScope]struct{}
	desired    map[ReachabilityField]map[StableLogicalObligation]struct{}
}

func (index stableLogicalObligationRequirementIndex) containsScope(obligation StableLogicalObligation) bool {
	if _, ok := index.global.lookup(obligation.Reachability); ok {
		return true
	}
	_, ok := index.namespaces[StableLogicalObligationNamespaceScope{Field: obligation.Reachability, Namespace: obligation.Namespace}]
	return ok
}

func indexStableLogicalObligationRequirements(requirements StableLogicalObligationRequirements) (stableLogicalObligationRequirementIndex, error) {
	normalized, err := NormalizeStableLogicalObligationRequirements(requirements)
	if err != nil {
		return stableLogicalObligationRequirementIndex{}, err
	}
	if len(normalized.ScopedFields) == 0 && len(normalized.ScopedNamespaces) == 0 {
		return stableLogicalObligationRequirementIndex{}, nil
	}
	index := stableLogicalObligationRequirementIndex{
		scoped:     newStableReachabilitySet(len(normalized.ScopedFields)),
		desired:    make(map[ReachabilityField]map[StableLogicalObligation]struct{}, len(normalized.ScopedFields)),
		global:     newStableReachabilitySet(len(normalized.ScopedFields)),
		namespaces: make(map[StableLogicalObligationNamespaceScope]struct{}, len(normalized.ScopedNamespaces)),
	}
	for _, field := range normalized.ScopedFields {
		index.global.set(field, struct{}{})
		index.scoped.set(field, struct{}{})
		index.desired[field] = make(map[StableLogicalObligation]struct{})
	}
	for _, scope := range normalized.ScopedNamespaces {
		index.scoped.set(scope.Field, struct{}{})
		index.namespaces[scope] = struct{}{}
		if index.desired[scope.Field] == nil {
			index.desired[scope.Field] = make(map[StableLogicalObligation]struct{})
		}
	}
	for _, obligation := range normalized.Obligations {
		index.desired[obligation.Reachability][obligation] = struct{}{}
	}
	return index, nil
}

// StableResourceDescriptor is an immutable-by-copy view of one coalesced
// physical resource obligation. Unlike Tokens, it reports the unioned frontier,
// every reachability field, and every logical obligation retained by the set.
type StableResourceDescriptor struct {
	kind               ResourceKind
	logicalLane        string
	resourceID         string
	diagnosticPath     string
	identity           StableIdentity
	generation         uint64
	digest             [32]byte
	frontier           DurableFrontier
	reachability       []ReachabilityField
	logicalObligations []StableLogicalObligation
	namespace          *StableNamespaceDescriptor
}

// StableResourcePhysicalDescriptor is complete physical metadata. Logical count
// is exact only when LogicalObligationCountAvailable is true; physical unions
// cannot report logical counts. It owns no logical corpus or root lease.
type StableResourcePhysicalDescriptor struct {
	Kind                            ResourceKind
	Generation                      uint64
	LogicalObligationCount          uint64
	LogicalObligationCountAvailable bool
	logicalLane                     string
	resourceID                      string
	diagnosticPath                  string
	identity                        StableIdentity
	digest                          [32]byte
	frontier                        DurableFrontier
	reachability                    []ReachabilityField
	namespace                       *StableNamespaceDescriptor
}

func physicalDescriptorFromEntry(entry *stableResourceEntry) StableResourcePhysicalDescriptor {
	fields := make([]ReachabilityField, 0, entry.reachability.len())
	for _, stableBinding1 := range entry.reachability.records() {
		field := stableBinding1.key
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	descriptor := StableResourcePhysicalDescriptor{
		Kind: entry.token.kind, Generation: entry.token.generation,
		LogicalObligationCount: uint64(entry.logicalObligations.count), LogicalObligationCountAvailable: true,
		logicalLane: entry.logicalLane, resourceID: entry.resourceID,
		diagnosticPath: entry.diagnosticPath, identity: entry.token.identity,
		digest: entry.token.digest, frontier: cloneDurableFrontier(entry.frontier), reachability: fields,
	}
	if namespace := entry.token.namespace; namespace != nil {
		descriptor.namespace = &StableNamespaceDescriptor{
			ParentIdentity: namespace.parentIdentity, Operation: namespace.operation,
			OldName: namespace.oldName, NewName: namespace.newName, DiagnosticPath: namespace.diagnosticPath,
		}
	}
	return descriptor
}

// PhysicalDescriptors performs no logical directory reads.
func (set *StableResourceSet) PhysicalDescriptors() []StableResourcePhysicalDescriptor {
	if set.RequireMetadataExport() != nil {
		return nil
	}
	if set == nil {
		return nil
	}
	result := make([]StableResourcePhysicalDescriptor, 0, set.Len())
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		descriptor := physicalDescriptorFromEntry(entry)
		if set.physicalOnly {
			descriptor.LogicalObligationCountAvailable = false
		}
		result = append(result, descriptor)
		return true
	})
	return result
}

func (descriptor StableResourcePhysicalDescriptor) LogicalLane() string {
	return descriptor.logicalLane
}
func (descriptor StableResourcePhysicalDescriptor) ResourceID() string { return descriptor.resourceID }
func (descriptor StableResourcePhysicalDescriptor) DiagnosticPath() string {
	return descriptor.diagnosticPath
}
func (descriptor StableResourcePhysicalDescriptor) Identity() StableIdentity {
	return descriptor.identity
}
func (descriptor StableResourcePhysicalDescriptor) Digest() [32]byte { return descriptor.digest }
func (descriptor StableResourcePhysicalDescriptor) Frontier() DurableFrontier {
	return cloneDurableFrontier(descriptor.frontier)
}
func (descriptor StableResourcePhysicalDescriptor) RIDs() []uint64 { return descriptor.frontier.RIDs() }
func (descriptor StableResourcePhysicalDescriptor) ReachabilityFields() []ReachabilityField {
	return append([]ReachabilityField(nil), descriptor.reachability...)
}
func (descriptor StableResourcePhysicalDescriptor) Namespace() (StableNamespaceDescriptor, bool) {
	if descriptor.namespace == nil {
		return StableNamespaceDescriptor{}, false
	}
	return *descriptor.namespace, true
}

// StableNamespaceDescriptor is an immutable-by-copy recovery view of the
// exact parent namespace obligation retained by a resource token. Diagnostic
// names remain DB-relative metadata; the parent identity and generation are
// the durable conflict boundary.
type StableNamespaceDescriptor struct {
	ParentIdentity StableIdentity
	Operation      NamespaceOperation
	OldName        string
	NewName        string
	DiagnosticPath string
}

func (descriptor StableResourceDescriptor) Kind() ResourceKind       { return descriptor.kind }
func (descriptor StableResourceDescriptor) LogicalLane() string      { return descriptor.logicalLane }
func (descriptor StableResourceDescriptor) ResourceID() string       { return descriptor.resourceID }
func (descriptor StableResourceDescriptor) DiagnosticPath() string   { return descriptor.diagnosticPath }
func (descriptor StableResourceDescriptor) Identity() StableIdentity { return descriptor.identity }
func (descriptor StableResourceDescriptor) Generation() uint64       { return descriptor.generation }
func (descriptor StableResourceDescriptor) Digest() [32]byte         { return descriptor.digest }

func (descriptor StableResourceDescriptor) Frontier() DurableFrontier {
	return cloneDurableFrontier(descriptor.frontier)
}

func (descriptor StableResourceDescriptor) RIDs() []uint64 {
	return descriptor.frontier.RIDs()
}

func (descriptor StableResourceDescriptor) ReachabilityFields() []ReachabilityField {
	return append([]ReachabilityField(nil), descriptor.reachability...)
}

func (descriptor StableResourceDescriptor) LogicalObligations() []StableLogicalObligation {
	return cloneStableLogicalObligations(descriptor.logicalObligations)
}

func (descriptor StableResourceDescriptor) Namespace() (StableNamespaceDescriptor, bool) {
	if descriptor.namespace == nil {
		return StableNamespaceDescriptor{}, false
	}
	return *descriptor.namespace, true
}

func stableResourcePinCounts(entries []stableResourceEntry) map[ResourceKind]uint64 {
	counts := make(map[ResourceKind]uint64)
	for _, entry := range entries {
		counts[entry.token.kind]++
	}
	return counts
}

func stableResourcePinCountsFromViews(views stableKindViews) map[ResourceKind]uint64 {
	counts := make(map[ResourceKind]uint64, views.len())
	for _, stableBinding1 := range views.records() {
		kind := stableBinding1.key
		view := stableBinding1.value
		counts[kind] = uint64(view.count)
	}
	return counts
}

func (set *StableResourceSet) Len() int {
	if set == nil {
		return 0
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.kindViews != nil {
		return stableResourceKindViewCount(set.kindViews)
	}
	return len(set.entries)
}

// PhysicalSummary returns the count and total frontier bytes of coalesced
// physical resources without allocating descriptor views.
func (set *StableResourceSet) PhysicalSummary() (count, bytes uint64) {
	if set == nil {
		return 0, 0
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	add := func(entry *stableResourceEntry) bool {
		count++
		bytes += entry.frontier.Bytes
		return true
	}
	if set.kindViews != nil {
		for _, stableBinding1 := range set.kindViews.records() {
			view := stableBinding1.value
			rangeStableResourceLogicalIndex(view.logical, add)
		}
		return count, bytes
	}
	for i := range set.entries {
		add(&set.entries[i])
	}
	return count, bytes
}

func (set *StableResourceSet) Tokens() []*StableResourceToken {
	if set.RequireMetadataExport() != nil {
		return nil
	}
	if set == nil {
		return nil
	}
	tokens := make([]*StableResourceToken, 0, set.Len())
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		tokens = append(tokens, activeEntryToken(*entry))
		return true
	})
	return tokens
}

// IdentityPinRegistryStats reports the exact DB-scoped registries retained by
// this closure's physical pins. Registries are de-duplicated, including when
// several logical obligations coalesce onto one physical resource.
func (set *StableResourceSet) IdentityPinRegistryStats() []IdentityPinRegistryStats {
	if set.RequireMetadataExport() != nil {
		return nil
	}
	if set == nil {
		return nil
	}
	registries := make(map[*IdentityPinRegistry]struct{})
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		tokens := entry.pins
		if len(tokens) == 0 {
			tokens = []*StableResourceToken{entry.token}
		}
		for _, token := range tokens {
			if token == nil || token.identityPin == nil || token.identityPin.registry == nil {
				continue
			}
			registries[token.identityPin.registry] = struct{}{}
		}
		return true
	})

	stats := make([]IdentityPinRegistryStats, 0, len(registries))
	for registry := range registries {
		stats = append(stats, registry.Stats())
	}
	return stats
}

// Descriptors materializes complete logical metadata. On any read or ownership
// failure it returns no descriptors. Publication paths should use physical
// metadata or WalkLogicalObligations to keep scratch memory bounded.
func (set *StableResourceSet) Descriptors() ([]StableResourceDescriptor, error) {
	if set == nil {
		return nil, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if err := requireOrdinaryStableResourceInputs(nil, nil, set); err != nil {
		return nil, err
	}
	descriptors := make([]StableResourceDescriptor, 0)
	positions := make(map[stableLogicalResourceKey]int)
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		physical := physicalDescriptorFromEntry(entry)
		positions[entry.token.logicalKey()] = len(descriptors)
		descriptors = append(descriptors, StableResourceDescriptor{
			kind: physical.Kind, generation: physical.Generation, logicalLane: physical.logicalLane,
			resourceID: physical.resourceID, diagnosticPath: physical.diagnosticPath,
			identity: physical.identity, digest: physical.digest, frontier: physical.frontier,
			reachability: physical.reachability, namespace: physical.namespace,
		})
		return true
	})
	if err := set.walkLogicalObligationsLocked(func(physical StableResourcePhysicalDescriptor, obligation StableLogicalObligation) error {
		key := stableLogicalResourceKey{kind: physical.Kind, lane: physical.logicalLane, resourceID: physical.resourceID, generation: physical.Generation}
		i := positions[key]
		descriptors[i].logicalObligations = append(descriptors[i].logicalObligations, obligation)
		return nil
	}); err != nil {
		return nil, err
	}
	for i := range descriptors {
		values := descriptors[i].logicalObligations
		sort.Slice(values, func(i, j int) bool { return stableLogicalObligationLess(values[i], values[j]) })
	}
	sort.Slice(descriptors, func(i, j int) bool {
		left, right := descriptors[i], descriptors[j]
		if left.kind != right.kind {
			return left.kind < right.kind
		}
		if left.logicalLane != right.logicalLane {
			return left.logicalLane < right.logicalLane
		}
		if left.resourceID != right.resourceID {
			return left.resourceID < right.resourceID
		}
		if left.generation != right.generation {
			return left.generation < right.generation
		}
		return bytes.Compare(left.identity.ObjectID[:], right.identity.ObjectID[:]) < 0
	})
	return descriptors, nil
}

// DependencyManifestV1 builds the unchanged durable V1 stream while reusing
// canonical encodings for immutable retained entries. A coalesced entry clears
// only its own cache before a later Freeze.
func (set *StableResourceSet) DependencyManifestV1() (*DependencyManifestV1, DependencyManifestBuildWorkV1, error) {
	if err := set.RequireMetadataExport(); err != nil {
		return nil, DependencyManifestBuildWorkV1{}, err
	}
	if set == nil {
		return NewDependencyManifestV1WithWork(nil)
	}
	if set.physicalOnly {
		return nil, DependencyManifestBuildWorkV1{}, ErrResourceOwnership
	}
	if set.hasDependencyDirectoryV2() {
		return nil, DependencyManifestBuildWorkV1{}, fmt.Errorf("%w: V1 manifest requested for a dependency directory", ErrResourceConflict)
	}
	work := DependencyManifestBuildWorkV1{}
	encoded := make([]*dependencyManifestEncodedEntryV1, 0, set.Len())
	var buildErr error
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		work.EntriesVisited++
		cache := entry.dependencyManifestV1
		if cache == nil {
			cache = &dependencyManifestEntryCacheV1{}
		}
		cache.mu.Lock()
		if cache.value == nil {
			manifestEntry := dependencyManifestEntryV1FromStableResourceEntry(*entry)
			normalized, err := normalizeDependencyManifestEntryV1(manifestEntry)
			if err != nil {
				cache.mu.Unlock()
				buildErr = err
				return false
			}
			raw := encodeDependencyManifestEntryV1(normalized)
			cache.value = &dependencyManifestEncodedEntryV1{entry: normalized, encoded: raw}
			work.EntriesEncoded++
			work.BytesEncoded += uint64(len(raw))
		}
		// Cache invalidation replaces the value; it never mutates a published
		// encoding. The manifest owns these references independently of pins.
		encoded = append(encoded, cache.value)
		cache.mu.Unlock()
		return true
	})
	if buildErr != nil {
		return nil, work, buildErr
	}
	manifest, err := newDependencyManifestV1FromEncoded(encoded, true)
	return manifest, work, err
}

func dependencyManifestEntryV1FromStableResourceEntry(entry stableResourceEntry) DependencyManifestEntryV1 {
	fields := make([]ReachabilityField, 0, entry.reachability.len())
	for _, stableBinding1 := range entry.reachability.records() {
		field := stableBinding1.key
		fields = append(fields, field)
	}
	logicalObligations := entry.logicalObligations.deltaSlice()
	result := DependencyManifestEntryV1{
		Kind: entry.token.kind, LogicalLane: entry.logicalLane, ResourceID: entry.resourceID,
		DiagnosticPath: entry.diagnosticPath, Identity: entry.token.identity, Generation: entry.token.generation,
		Digest: entry.token.digest, Frontier: cloneDurableFrontier(entry.frontier), Reachability: fields,
		LogicalObligations: logicalObligations,
	}
	if namespace := entry.token.namespace; namespace != nil {
		result.Namespace = &DependencyManifestNamespaceV1{
			ParentIdentity: namespace.parentIdentity, Operation: namespace.operation,
			OldName: namespace.oldName, NewName: namespace.newName, DiagnosticPath: namespace.diagnosticPath,
		}
	}
	return result
}

// CloneStableResourceSetExcludingKinds creates a new independently-owned
// closure from the exact handles retained by source. Diagnostic paths are
// never reopened. This is used when a later durable root still references an
// immutable external resource retained by the currently selected root slot.
//
// Excluded kinds let a caller replace mutable resources, such as value-log
// segments, with a fresh exact reachability scan for the candidate root. The
// returned set is builder-owned and can be merged into another builder.
func CloneStableResourceSetExcludingKinds(source *StableResourceSet, excluded ...ResourceKind) (*StableResourceSet, error) {
	return CloneStableResourceSetForLogicalObligations(source, StableLogicalObligationRequirements{}, excluded...)
}

// CloneStableResourceSetForLogicalObligations independently retains the exact
// source handles that still satisfy a candidate root's scoped logical
// obligations. Unscoped reachability fields are cloned unchanged. For a
// scoped field, stale obligations are omitted and an empty desired set drops
// every source token owned only by that field. The caller must validate the
// final union after merging newly produced resources.
func CloneStableResourceSetForLogicalObligations(source *StableResourceSet, requirements StableLogicalObligationRequirements, excluded ...ResourceKind) (*StableResourceSet, error) {
	cloned, _, err := CloneStableResourceSetForLogicalObligationsWithWork(source, requirements, excluded...)
	return cloned, err
}

// CloneStableResourceSetForLogicalObligationsWithWork is the measured form of
// CloneStableResourceSetForLogicalObligations. Its counters describe only this
// closure derivation and are returned on both success and failure.
func CloneStableResourceSetForLogicalObligationsWithWork(source *StableResourceSet, requirements StableLogicalObligationRequirements, excluded ...ResourceKind) (*StableResourceSet, StableResourceClosureWork, error) {
	if err := source.rejectAccountedCloneInput(); err != nil {
		return nil, StableResourceClosureWork{}, err
	}
	if source != nil && source.physicalOnly {
		return nil, StableResourceClosureWork{}, ErrResourceOwnership
	}
	work := StableResourceClosureWork{CloneOperations: 1}
	if source == nil {
		return nil, work, nil
	}
	if len(requirements.ScopedFields) == 0 && len(requirements.ScopedNamespaces) == 0 && len(requirements.Obligations) == 0 {
		cloned, shared, err := cloneStableResourceSetKindView(source, excluded...)
		if err != nil {
			return nil, work, err
		}
		if shared {
			work.PhysicalRootShares = uint64(cloned.kindViews.len())
			return cloned, work, nil
		}
	}
	work.RequirementFieldsInspected = uint64(len(requirements.ScopedFields) + len(requirements.ScopedNamespaces))
	work.RequirementObligationsInspected = uint64(len(requirements.Obligations))
	requirementIndex, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return nil, work, err
	}
	if source.hasDependencyDirectoryV2() {
		return cloneDirectoryForRequirementsV2(source, requirements, requirementIndex, work, excluded...)
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	builder := NewStableResourceSetBuilder()
	abandon := true
	defer func() {
		if abandon {
			builder.Abandon()
		}
	}()

	source.mu.Lock()
	defer source.mu.Unlock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, work, ErrResourceOwnership
	}
	var sourceObligationTotal uint64
	for _, entry := range source.entrySnapshotLocked() {
		work.SourceEntriesInspected++
		sourceObligationTotal += uint64(entry.logicalObligations.count)
		token := activeEntryToken(entry)
		if token == nil || token.released.Load() {
			return nil, work, ErrResourceOwnership
		}
		if _, skip := excludedKinds[token.kind]; skip {
			work.DroppedEntries++
			work.DroppedObligations += uint64(entry.logicalObligations.count)
			continue
		}
		fields := make([]ReachabilityField, 0, entry.reachability.len())
		allFieldsUnscoped := true
		for _, stableBinding1 := range entry.reachability.records() {
			field := stableBinding1.key
			fields = append(fields, field)
			if _, scoped := requirementIndex.scoped.lookup(field); scoped {
				allFieldsUnscoped = false
			}
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		for _, field := range fields {
			_, scoped := requirementIndex.scoped.lookup(field)
			var obligations []StableLogicalObligation
			var sharedObligations stableLogicalObligationView
			if allFieldsUnscoped {
				// A wholly unscoped physical entry is retained byte-for-byte. Its
				// immutable obligation view and complete reachability-field set are
				// shared with an independent token reference to the exact pinned
				// handle. Multi-field entries must not re-walk the
				// same cumulative obligation history once per field.
				sharedObligations = entry.logicalObligations
			} else {
				obligations = make([]StableLogicalObligation, 0, entry.logicalObligations.count)
				entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
					work.SourceObligationsInspected++
					if obligation.Reachability != field {
						return true
					}
					if requirementIndex.containsScope(obligation) {
						if _, desired := requirementIndex.desired[field][obligation]; !desired {
							return true
						}
					}
					obligations = append(obligations, obligation)
					return true
				})
				work.CopiedObligations += uint64(len(obligations))
			}
			if scoped && len(obligations) == 0 {
				continue
			}
			work.RetainedEntries++
			retainedObligations := len(obligations)
			if allFieldsUnscoped {
				retainedObligations = sharedObligations.count
			}
			work.RetainedObligations += uint64(retainedObligations)
			work.CopiedEntries++
			var cloned *StableResourceToken
			if allFieldsUnscoped {
				work.PhysicalHandleShares++
			} else {
				work.PhysicalHandleCopies++
			}
			cloned, err = cloneStableEntryToken(token, entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, field, obligations, nil, !allFieldsUnscoped)
			if err != nil {
				return nil, work, err
			}
			if err := builder.Add(cloned); err != nil {
				cloned.Release()
				return nil, work, err
			}
			if allFieldsUnscoped {
				// The source set is already physically coalesced, so Add necessarily
				// appended one new destination entry. Restore its complete immutable
				// logical and reachability views after constructing the one-field
				// token used to duplicate the exact physical handle.
				destination := &builder.entries[len(builder.entries)-1]
				destination.logicalObligations = sharedObligations
				destination.dependencyManifestV1 = entry.dependencyManifestV1
				destination.reachability = newStableReachabilitySet(entry.reachability.len())
				for _, stableBinding2 := range entry.reachability.records() {
					retainedField := stableBinding2.key
					destination.reachability.set(retainedField, struct{}{})
				}
			}
			if allFieldsUnscoped {
				break
			}
		}
	}
	cloned, err := builder.Freeze()
	if err != nil {
		return nil, work, err
	}
	work.Add(builder.ClosureWorkSnapshot())
	work.FreezeOperations++
	// Obligations omitted from retained field projections are logical drops.
	// Excluded physical entries have already been accounted above.
	if sourceObligationTotal >= work.RetainedObligations+work.DroppedObligations {
		work.DroppedObligations = sourceObligationTotal - work.RetainedObligations
	}
	if cloned.Len() == 0 && work.SourceEntriesInspected != 0 && work.DroppedEntries == 0 {
		work.DroppedEntries = work.SourceEntriesInspected
	}
	abandon = false
	return cloned, work, nil
}

// CloneStableResourceSetApplyingLogicalObligationMutation derives the inherited
// side of a candidate closure from exact root-local mutation evidence. Added
// obligations are intentionally not synthesized here: the caller must merge
// their newly admitted exact-handle producer set. Destructive transitions use
// the full exact filter path; only removal-free transitions may share the
// immutable retained obligation view.
func CloneStableResourceSetApplyingLogicalObligationMutation(source *StableResourceSet, mutation StableLogicalObligationMutation, excluded ...ResourceKind) (*StableResourceSet, StableResourceClosureWork, error) {
	if err := source.rejectAccountedCloneInput(); err != nil {
		return nil, StableResourceClosureWork{}, err
	}
	if source != nil && source.physicalOnly {
		return nil, StableResourceClosureWork{}, ErrResourceOwnership
	}
	normalized, err := NormalizeStableLogicalObligationMutation(mutation)
	work := StableResourceClosureWork{
		RequirementFieldsInspected:      uint64(len(mutation.ScopedFields)),
		RequirementObligationsInspected: uint64(len(mutation.Added) + len(mutation.Removed)),
	}
	if err != nil {
		return nil, work, err
	}
	work.LogicalObligationNormalizations = uint64(len(normalized.Added) + len(normalized.Removed))
	work.RemovedObligations = uint64(len(normalized.Removed))
	if len(normalized.Removed) != 0 {
		if cloned, directoryWork, handled, err := cloneDirectoryRemovingV2(source, normalized, work, excluded...); handled {
			return cloned, directoryWork, err
		}
	}
	if len(normalized.Removed) == 0 {
		cloned, shared, cloneErr := cloneStableResourceSetKindView(source, excluded...)
		if cloneErr != nil {
			return nil, work, cloneErr
		}
		if shared {
			work.CloneOperations = 1
			if cloned != nil {
				work.PhysicalRootShares = uint64(cloned.kindViews.len())
			}
			return cloned, work, nil
		}
		cloned, cloneWork, cloneErr := CloneStableResourceSetForLogicalObligationsWithWork(
			source, StableLogicalObligationRequirements{}, excluded...,
		)
		cloneWork.RequirementFieldsInspected += work.RequirementFieldsInspected
		cloneWork.RequirementObligationsInspected += work.RequirementObligationsInspected
		return cloned, cloneWork, cloneErr
	}

	removed := make(map[StableLogicalObligation]struct{}, len(normalized.Removed))
	for _, obligation := range normalized.Removed {
		removed[obligation] = struct{}{}
	}
	retained := StableLogicalObligationRequirements{
		ScopedFields: append([]ReachabilityField(nil), normalized.ScopedFields...),
	}
	scoped := newStableReachabilitySet(len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped.set(field, struct{}{})
	}
	found := make(map[StableLogicalObligation]struct{}, len(removed))
	if source != nil {
		descriptors, err := source.Descriptors()
		if err != nil {
			return nil, work, err
		}
		for _, descriptor := range descriptors {
			for _, obligation := range descriptor.LogicalObligations() {
				if _, ok := scoped.lookup(obligation.Reachability); !ok {
					continue
				}
				if _, drop := removed[obligation]; drop {
					found[obligation] = struct{}{}
					continue
				}
				retained.Obligations = append(retained.Obligations, obligation)
			}
		}
	}
	for obligation := range removed {
		if _, ok := found[obligation]; !ok {
			return nil, work, fmt.Errorf("%w: removal mutation does not match inherited logical obligation %+v", ErrUnresolvedResource, obligation)
		}
	}
	cloned, cloneWork, cloneErr := CloneStableResourceSetForLogicalObligationsWithWork(source, retained, excluded...)
	cloneWork.RequirementFieldsInspected += work.RequirementFieldsInspected
	cloneWork.RequirementObligationsInspected += work.RequirementObligationsInspected
	cloneWork.RemovedObligations = work.RemovedObligations
	return cloned, cloneWork, cloneErr
}

// ValidateStableResourceSetLogicalObligations proves that the scoped fields in
// resources contain exactly the candidate root's desired logical references:
// no missing obligation and no stale extra obligation.
func ValidateStableResourceSetLogicalObligations(resources *StableResourceSet, requirements StableLogicalObligationRequirements) error {
	_, err := ValidateStableResourceSetLogicalObligationsWithWork(resources, requirements)
	return err
}

// ValidateStableResourceSetLogicalObligationsWithWork is the measured form of
// ValidateStableResourceSetLogicalObligations. It is used by destructive and
// mixed-producer fallbacks where scanning the complete exact closure is
// intentional and must remain visible in performance evidence.
func ValidateStableResourceSetLogicalObligationsWithWork(resources *StableResourceSet, requirements StableLogicalObligationRequirements) (StableResourceClosureWork, error) {
	if err := resources.RequireMetadataExport(); err != nil {
		return StableResourceClosureWork{}, err
	}
	if resources != nil && resources.physicalOnly {
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	work := StableResourceClosureWork{
		RequirementFieldsInspected:      uint64(len(requirements.ScopedFields) + len(requirements.ScopedNamespaces)),
		RequirementObligationsInspected: uint64(len(requirements.Obligations)),
		LogicalObligationNormalizations: uint64(len(requirements.Obligations)),
		FullClosureValidations:          1,
	}
	index, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return work, err
	}
	if index.scoped.len() == 0 {
		return work, nil
	}
	if resources.hasDependencyDirectoryV2() {
		return validateDirectoryRequirementsV2(resources, index, work)
	}
	actual := make(map[ReachabilityField]map[StableLogicalObligation]struct{}, index.scoped.len())
	for _, stableBinding1 := range index.scoped.records() {
		field := stableBinding1.key
		actual[field] = make(map[StableLogicalObligation]struct{})
	}
	if resources != nil {
		resources.mu.Lock()
		defer resources.mu.Unlock()
		for _, entry := range resources.entrySnapshotLocked() {
			work.SourceEntriesInspected++
			fields := make([]ReachabilityField, 0, entry.reachability.len())
			for _, stableBinding2 := range entry.reachability.records() {
				field := stableBinding2.key
				fields = append(fields, field)
			}
			for _, field := range fields {
				if _, scoped := index.scoped.lookup(field); !scoped {
					continue
				}
				foundForField := false
				var visitErr error
				entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
					work.SourceObligationsInspected++
					if obligation.Reachability != field {
						return true
					}
					foundForField = true
					if !index.containsScope(obligation) {
						return true
					}
					if _, desired := index.desired[field][obligation]; !desired {
						visitErr = fmt.Errorf("%w: stale logical obligation %+v", ErrResourceConflict, obligation)
						return false
					}
					if _, duplicate := actual[field][obligation]; duplicate {
						visitErr = fmt.Errorf("%w: duplicate logical obligation %+v", ErrResourceConflict, obligation)
						return false
					}
					actual[field][obligation] = struct{}{}
					return true
				})
				if visitErr != nil {
					return work, visitErr
				}
				if !foundForField {
					return work, fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
				}
			}
		}
	}
	for field, desired := range index.desired {
		for obligation := range desired {
			if _, ok := actual[field][obligation]; !ok {
				return work, fmt.Errorf("%w: missing logical obligation %+v", ErrUnresolvedResource, obligation)
			}
		}
	}
	return work, nil
}

func (set *StableResourceSet) covers(field ReachabilityField) bool {
	if set == nil || field == "" {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if requireOrdinaryStableResourceInputs(nil, nil, set) != nil {
		return false
	}
	if ResourceOwnerState(set.owner.Load()) != ResourceOwnerBuilder {
		return false
	}
	covered := false
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if _, ok := entry.reachability.lookup(field); ok {
			covered = true
			return false
		}
		return true
	})
	return covered
}

func (set *StableResourceSet) FrontierFor(identity StableIdentity, generation uint64) DurableFrontier {
	if set == nil {
		return DurableFrontier{}
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if requireOrdinaryStableResourceInputs(nil, nil, set) != nil {
		return DurableFrontier{}
	}
	var frontier DurableFrontier
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if sameStableObject(entry.token.identity, identity) && entry.token.generation == generation {
			frontier = cloneDurableFrontier(entry.frontier)
			return false
		}
		return true
	})
	return frontier
}

// FlushThrough flushes every pinned resource through the greatest frontier
// retained by this set. Coalescing can advance that frontier beyond the
// representative token's original registration, so callers must operate on
// the set rather than iterating Tokens().
func (set *StableResourceSet) FlushThrough() error {
	if err := set.RequireMetadataExport(); err != nil {
		return err
	}
	if set == nil {
		return nil
	}
	if err := set.beginBackingOperationV1(); err != nil {
		return err
	}
	defer set.endBackingOperationV1()
	// This short admission forbids transfer/terminal mutation while immutable
	// backing is read. User operations execute without the set gate held.
	var errs []error
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil {
			errs = append(errs, ErrResourceOwnership)
			return true
		}
		if err := token.flushThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("flush stable resource %+v: %w", token.logicalKey(), err))
		}
		return true
	})
	return errors.Join(errs...)
}

// SyncThrough persists every pinned resource through the greatest frontier
// retained by this set.
func (set *StableResourceSet) SyncThrough() error {
	if err := set.RequireMetadataExport(); err != nil {
		return err
	}
	if set == nil {
		return nil
	}
	if err := set.beginBackingOperationV1(); err != nil {
		return err
	}
	defer set.endBackingOperationV1()
	// This short admission forbids transfer/terminal mutation while immutable
	// backing is read. User operations execute without the set gate held.
	var errs []error
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil {
			errs = append(errs, ErrResourceOwnership)
			return true
		}
		if err := token.syncThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("sync stable resource %+v: %w", token.logicalKey(), err))
		}
		return true
	})
	return errors.Join(errs...)
}

func (set *StableResourceSet) Owner() ResourceOwnerState {
	if set == nil {
		return ResourceOwnerReleased
	}
	return ResourceOwnerState(set.owner.Load())
}

func (set *StableResourceSet) transfer(from, to ResourceOwnerState) error {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.terminalReleasing || set.activeOperations != 0 {
		return ErrResourceOwnership
	}
	if set.metadataAccount != nil {
		if !set.finiteMetadata || set.kindViews != nil || set.extras != nil {
			return ErrStableMetadataShapeUnsupported
		}
		for i := range set.entries {
			e := &set.entries[i]
			if e.token == nil || len(e.pins) != 0 || e.pinIndex != nil {
				return ErrStableMetadataShapeUnsupported
			}
			phase := ResourceOwnerState(e.token.owner.Load())
			if phase != from && phase != ResourceOwnerReleased {
				return ErrResourceOwnership
			}
			for j := 0; j < i; j++ {
				if set.entries[j].token == e.token {
					return ErrResourceOwnership
				}
			}
		}
		if !set.owner.CompareAndSwap(uint32(from), uint32(to)) {
			return ErrResourceOwnership
		}
		// Only the concrete flat collector closure uses this internal transfer.
		// Partial terminal cleanup transfers actual unfinished owners; already
		// released entries remain released and are never resurrected for recovery.
		for i := range set.entries {
			if ResourceOwnerState(set.entries[i].token.owner.Load()) != ResourceOwnerReleased {
				set.entries[i].token.owner.Store(uint32(to))
			}
		}
		return nil
	}
	if err := requireOrdinaryStableResourceInputs(nil, nil, set); err != nil {
		return err
	}
	if !set.owner.CompareAndSwap(uint32(from), uint32(to)) {
		return ErrResourceOwnership
	}
	if set.kindViews != nil {
		return nil
	}
	transferred := 0
	for _, entry := range set.entries {
		if err := entry.token.transfer(from, to); err != nil {
			for i := 0; i < transferred; i++ {
				_ = set.entries[i].token.transfer(to, from)
			}
			set.owner.Store(uint32(from))
			return err
		}
		transferred++
	}
	return nil
}

func (set *StableResourceSet) releaseOrdinaryFrom(owner ResourceOwnerState) (result error) {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	if set.Owner() == ResourceOwnerReleased {
		set.mu.Unlock()
		return nil
	}
	if set.Owner() != owner || set.terminalReleasing || set.activeOperations != 0 {
		set.mu.Unlock()
		return ErrStableResourceOperationBusy
	}
	set.terminalReleasing = true
	entries, views := set.entries, set.kindViews
	set.mu.Unlock()
	defer func() { set.mu.Lock(); set.terminalReleasing = false; set.mu.Unlock() }()
	if views != nil {
		if err := releaseStableResourceKindViewsCursorV1(views, &set.releaseCursor, &set.releaseViewStarted); err != nil {
			return err
		}
	} else {
		for set.releaseCursor < len(entries) {
			token := entries[set.releaseCursor].token
			err := token.releaseFrom(owner)
			result = errors.Join(result, err)
			if !token.CleanupCompleteV1() {
				return errors.Join(result, ErrStableResourceOperationBusy)
			}
			set.releaseCursor++
		}
	}
	if !set.releaseDirectoriesDone {
		set.mu.Lock()
		empty := set.emptyDependencyDirectoryLocked()
		directories := set.physicalDependencyDirectoriesLocked()
		set.mu.Unlock()
		for _, directory := range directories {
			directory.Release()
		}
		if empty != nil {
			empty.Release()
		}
		set.releaseDirectoriesDone = true
	}
	set.mu.Lock()
	set.owner.Store(uint32(ResourceOwnerReleased))
	set.mu.Unlock()
	return result
}

// Finite cleanup preserves the original set/account and remaining entries on an
// observable physical cleanup failure. Generic ordinary releases retain their
// existing alias/diagnostic and callback behavior.
func (set *StableResourceSet) releaseFrom(owner ResourceOwnerState) error {
	return set.releaseFromWithTerminal(owner, nil)
}
func (set *StableResourceSet) releaseFromWithTerminal(owner ResourceOwnerState, consumer StableSegmentTerminalConsumer) error {
	if set == nil || set.Owner() == ResourceOwnerReleased {
		return nil
	}
	set.mu.Lock()
	if set.Owner() != owner || set.terminalReleasing || set.activeOperations != 0 {
		set.mu.Unlock()
		return ErrResourceOwnership
	}
	if set.metadataAccount == nil {
		err := requireOrdinaryStableResourceInputs(nil, nil, set)
		set.mu.Unlock()
		if err != nil {
			return err
		}
		return set.releaseOrdinaryFrom(owner)
	}
	if !set.finiteMetadata || set.kindViews != nil || set.extras != nil {
		set.mu.Unlock()
		return ErrStableMetadataShapeUnsupported
	}
	for i := range set.entries {
		e := &set.entries[i]
		if e.token == nil || len(e.pins) != 0 || e.pinIndex != nil {
			set.mu.Unlock()
			return ErrStableMetadataShapeUnsupported
		}
		if ResourceOwnerState(e.token.owner.Load()) != owner && ResourceOwnerState(e.token.owner.Load()) != ResourceOwnerReleased {
			set.mu.Unlock()
			return ErrResourceOwnership
		}
		for j := 0; j < i; j++ {
			if set.entries[j].token == e.token {
				set.mu.Unlock()
				return ErrResourceOwnership
			}
		}
		if err := validateStableSegmentTerminalRetention(e.token.segmentRetention, consumer); err != nil {
			set.mu.Unlock()
			return err
		}
	}
	set.terminalReleasing = true
	entries := set.entries
	set.mu.Unlock()
	// No external terminal operation runs under the set mutex. The scalar
	// reservation forbids concurrent transfer/release of this actual owned array.
	for i := range entries {
		if err := entries[i].token.releaseFromWithTerminal(owner, consumer); err != nil {
			set.mu.Lock()
			set.terminalReleasing = false
			set.mu.Unlock()
			return err
		}
	}
	set.mu.Lock()
	account := set.metadataAccount
	set.metadataAccount = nil
	clear(set.entries)
	set.entries = nil
	set.terminalReleasing = false
	set.owner.Store(uint32(ResourceOwnerReleased))
	set.mu.Unlock()
	account.ReleaseStableMetadata()
	return nil
}

func (set *StableResourceSet) prepareTerminalRelease(consumer StableSegmentTerminalConsumer) error {
	count, account, err := inspectStableTerminalSet(set, consumer)
	if err != nil || count == 0 {
		return err
	}
	n, err := StableBackingClassBytes(uint64(count)*uint64(unsafe.Sizeof((*StableResourceToken)(nil))), true)
	if err != nil {
		return err
	}
	if err = account.ReserveStableMetadata(n); err != nil {
		return err
	}
	set.mu.Lock()
	tokens := make([]*StableResourceToken, 0, count)
	for i := range set.entries {
		if ResourceOwnerState(set.entries[i].token.owner.Load()) != ResourceOwnerReleased {
			tokens = append(tokens, set.entries[i].token)
		}
	}
	set.mu.Unlock()
	return prepareStableSegmentTerminalTokens(tokens, consumer)
}

func (set *StableResourceSet) Release() error { return set.ReleaseWithTerminal(nil) }
func (set *StableResourceSet) ReleaseWithTerminal(consumer StableSegmentTerminalConsumer) error {
	if set == nil {
		return nil
	}
	owner := set.Owner()
	if owner != ResourceOwnerBuilder && owner != ResourceOwnerRecovery {
		if owner == ResourceOwnerReleased || !set.finiteMetadata {
			return nil
		}
		return ErrResourceOwnership
	}
	joined := false
	if consumer != nil {
		var err error
		joined, err = consumer.BeginTerminalRelease()
		if err != nil {
			return err
		}
		defer consumer.EndTerminalRelease(joined)
	}
	if err := set.prepareTerminalRelease(consumer); err != nil {
		return err
	}
	return set.releaseFromWithTerminal(owner, consumer)
}

func stableLogicalObligationCommitmentsEqual(left, right map[ReachabilityField]stableLogicalObligationCommitment) bool {
	if len(left) != len(right) {
		return false
	}
	for field, commitment := range left {
		if right[field] != commitment {
			return false
		}
	}
	return true
}

func appendStableLogicalMembershipEvidenceCandidates(target map[ResourceKind][]stableLogicalMembershipEvidence, set *StableResourceSet) map[ResourceKind][]stableLogicalMembershipEvidence {
	if set.kindViews != nil {
		for _, stableBinding1 := range set.kindViews.records() {
			kind := stableBinding1.key
			view := stableBinding1.value
			evidence := stableLogicalMembershipEvidenceFromKindView(view)
			if evidence.logicalObligationCount != 0 && stableLogicalMembershipEvidenceComplete(evidence) {
				if target == nil {
					target = make(map[ResourceKind][]stableLogicalMembershipEvidence)
				}
				target[kind] = append(target[kind], evidence)
			}
		}
		return target
	}
	for kind, evidence := range set.logicalMembershipEvidenceLocked() {
		if evidence.logicalObligationCount != 0 && stableLogicalMembershipEvidenceComplete(evidence) {
			if target == nil {
				target = make(map[ResourceKind][]stableLogicalMembershipEvidence)
			}
			target[kind] = append(target[kind], evidence)
		}
	}
	return target
}

func stableUnionLogicalMembershipEvidence(entries []stableResourceEntry, candidates map[ResourceKind][]stableLogicalMembershipEvidence) map[ResourceKind]stableLogicalMembershipEvidence {
	summaries := make(map[ResourceKind]stableLogicalMembershipEvidence)
	for i := range entries {
		entry := &entries[i]
		kind := entry.token.kind
		summary := summaries[kind]
		summary.logicalObligationCount += entry.logicalObligations.count
		if summary.commitments == nil && len(entry.logicalObligations.commitments) != 0 {
			summary.commitments = make(map[ReachabilityField]stableLogicalObligationCommitment, len(entry.logicalObligations.commitments))
		}
		for field, incoming := range entry.logicalObligations.commitments {
			commitment := summary.commitments[field]
			commitment.add(incoming)
			summary.commitments[field] = commitment
		}
		summaries[kind] = summary
	}
	for kind, summary := range summaries {
		for _, candidate := range candidates[kind] {
			// This shares the count + SHA-256 multiset commitment's documented
			// collision boundary; it is not relaxed equality.
			if candidate.logicalObligationCount == summary.logicalObligationCount && stableLogicalObligationCommitmentsEqual(candidate.commitments, summary.commitments) {
				summary.root = candidate.root
				summary.logicalMembershipCount = candidate.logicalMembershipCount
				break
			}
		}
		summaries[kind] = summary
	}
	return summaries
}

func UnionStableResourceSets(sets ...*StableResourceSet) (*StableResourceSet, error) {
	return unionStableResourceSets(stableResourceViewPinned, sets...)
}

// validatedStableResourceUnionCount borrows privately owned candidate entries
// for the duration of enqueue under the coordinator lock. No resulting view or
// pin escapes; full union consumers retain their normal validation and output.
func validatedStableResourceUnionCount(sets ...*StableResourceSet) (int, error) {
	if set := singleStableResourceSet(sets); set != nil {
		return set.Len(), nil
	}
	view, err := unionStableResourceSets(stableResourceViewValidatedCount, sets...)
	if err != nil {
		return 0, err
	}
	return len(view.entries), nil
}

// singleStableResourceSet ignores nil inputs, but not empty sets.
func singleStableResourceSet(sets []*StableResourceSet) *StableResourceSet {
	var single *StableResourceSet
	for _, set := range sets {
		if set == nil {
			continue
		}
		if single != nil {
			return nil
		}
		single = set
	}
	return single
}

func unionStableResourceSets(mode stableResourceViewMode, sets ...*StableResourceSet) (*StableResourceSet, error) {
	if err := requireOrdinaryStableResourceSets(sets...); err != nil {
		return nil, err
	}
	physicalOnly := mode == stableResourceViewPhysicalPinned || mode == stableResourceViewPhysicalCount
	view := &StableResourceSet{ordinaryMetadata: true, physicalOnly: physicalOnly}
	view.owner.Store(uint32(ResourceOwnerView))
	single := singleStableResourceSet(sets)
	lookup := stableResourceEntryLookup{}
	var evidenceCandidates map[ResourceKind][]stableLogicalMembershipEvidence
	var inheritedDirectory *DependencyDirectoryV2
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		if set.physicalOnly && !physicalOnly {
			set.mu.Unlock()
			return nil, ErrResourceOwnership
		}
		// Merge consumes and may clear either flat entries or kind views. A
		// transferred source is never a complete diagnostic metadata snapshot.
		if set.Owner() == ResourceOwnerTransferred {
			set.mu.Unlock()
			return nil, ErrResourceOwnership
		}
		if set.Owner() == ResourceOwnerReleased {
			// V1 flat metadata remains immutable diagnostic evidence after its
			// producer releases ownership. Directory reads and physical union
			// capabilities instead depend on a live lease, even for empty roots.
			requiresLease := physicalOnly || set.emptyDependencyDirectoryLocked() != nil || len(set.physicalDependencyDirectoriesLocked()) != 0
			set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
				requiresLease = requiresLease || entry.logicalObligations.directory != nil
				return !requiresLease
			})
			if requiresLease {
				set.mu.Unlock()
				return nil, ErrResourceOwnership
			}
		}
		if !physicalOnly {
			compatible := func(directory *DependencyDirectoryV2) bool {
				if directory == nil {
					return true
				}
				if inheritedDirectory == nil {
					inheritedDirectory = directory
				}
				return inheritedDirectory == directory
			}
			if !compatible(set.emptyDependencyDirectoryLocked()) || !set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
				return compatible(entry.logicalObligations.directory)
			}) {
				set.mu.Unlock()
				return nil, ErrResourceConflict
			}
		}
		if mode != stableResourceViewValidatedCount && !physicalOnly {
			evidenceCandidates = appendStableLogicalMembershipEvidenceCandidates(evidenceCandidates, set)
		}
		// Frozen kind views and prior flat unions are already canonical. Keep
		// the same independent flat metadata snapshot, without reconciling it
		// against itself or borrowing any root ownership.
		direct := mode == stableResourceViewPinned && set == single &&
			(set.kindViews != nil || set.Owner() == ResourceOwnerView)
		var mergeErr error
		set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			if mode == stableResourceViewPhysicalPinned {
				physical := *entry
				physical.logicalObligations = stableLogicalObligationView{}
				physical.dependencyManifestV1 = &dependencyManifestEntryCacheV1{}
				entry = &physical
			}
			if direct {
				view.entries = append(view.entries, cloneStableResourceEntry(*entry))
				return true
			}
			var err error
			if lookup.logical.less == nil && len(view.entries) < stableResourceEntryLinearLookupLimit {
				err = mergeViewEntryLinear(&view.entries, *entry, mode, nil)
			} else {
				if lookup.logical.less == nil {
					lookup = newStableResourceEntryLookup(view.entries)
				}
				err = mergeViewEntry(&view.entries, &lookup, *entry, mode, nil)
			}
			if err != nil {
				mergeErr = err
				return false
			}
			return true
		})
		set.mu.Unlock()
		if mergeErr != nil {
			return nil, mergeErr
		}
	}
	if mode == stableResourceViewValidatedCount || mode == stableResourceViewPhysicalCount {
		return view, nil
	}
	sortStableResourceEntries(view.entries)
	if len(evidenceCandidates) != 0 {
		view.extras = &stableResourceSetExtras{logicalMembershipEvidence: stableUnionLogicalMembershipEvidence(view.entries, evidenceCandidates)}
	}
	view.pinHighWater = stableResourcePinCounts(view.entries)
	return view, nil
}

func (set *StableResourceSet) union(other immutableExtension) (immutableExtension, error) {
	otherSet, ok := other.(*StableResourceSet)
	if !ok {
		return nil, fmt.Errorf("%w: resource extension has type %T", ErrResourceConflict, other)
	}
	return UnionStableResourceSets(set, otherSet)
}

type StableResourceDeletionGuard struct {
	err     error // refusal survives no-error legacy construction
	entries []stableResourceEntry
}

func (set *StableResourceSet) DeletionGuard() StableResourceDeletionGuard {
	if set == nil {
		return StableResourceDeletionGuard{}
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if err := requireOrdinaryStableResourceInputs(nil, nil, set); err != nil {
		return StableResourceDeletionGuard{err: err}
	}
	capacity := len(set.entries)
	if set.kindViews != nil {
		capacity = stableResourceKindViewCount(set.kindViews)
	}
	entries := make([]stableResourceEntry, 0, capacity)
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		entries = append(entries, cloneStableResourceEntry(*entry))
		return true
	})
	return StableResourceDeletionGuard{entries: entries}
}

func (guard StableResourceDeletionGuard) Check(identity StableIdentity, generation uint64) error {
	if guard.err != nil {
		return guard.err
	}
	for _, entry := range guard.entries {
		pins := entry.pins
		if len(pins) == 0 {
			pins = []*StableResourceToken{entry.token}
		}
		for _, token := range pins {
			if sameStableObject(token.identity, identity) && token.generation == generation && !token.released.Load() {
				return ErrResourcePinned
			}
		}
	}
	return nil
}

// BytesNotCoveredBy returns conservative publication debt against an already
// published resource closure. A compatible mutable append receives credit for
// its exact durable prefix; every other advanced frontier is charged in full.
// ContentSynced does not establish root
// publication. The caller must supply its owned durable-root authority.
// Neither closure, its pins, nor its ordinary resource statistics are changed.
func (set *StableResourceSet) BytesNotCoveredBy(published *StableResourceSet) (uint64, error) {
	if err := requireOrdinaryStableResourceSets(set, published); err != nil {
		return 0, err
	}
	if (set != nil && set.physicalOnly) || (published != nil && published.physicalOnly) {
		return 0, ErrResourceOwnership
	}
	if set == nil {
		return 0, nil
	}
	// Retain the existing immutable indexes before locking set. Independent
	// callers may compare the same sets in either order, or release a source.
	baseline, _, err := cloneStableResourceSetKindView(published)
	if err != nil {
		return 0, err
	}
	defer baseline.Release()
	set.mu.Lock()
	defer set.mu.Unlock()
	if owner := ResourceOwnerState(set.owner.Load()); owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return 0, ErrResourceOwnership
	}
	var total uint64
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		var covered uint64
		if baseline != nil {
			view := baseline.kindViews.get(entry.token.kind)
			prior := findStableResourceLogical(view.logical, entry.token.logicalKey())
			if stableResourceEntryCoversPublication(prior, entry) {
				covered = entry.frontier.Bytes
			} else if prefix, ok := stableResourceEntryDurablePrefix(prior, entry); ok {
				covered = prefix
			}
		}
		total = saturatingAdd(total, entry.frontier.Bytes-covered)
		return true
	})
	return total, nil
}

func stableResourceEntryDurablePrefix(prior, entry *stableResourceEntry) (uint64, bool) {
	if prior != nil && (prior.logicalObligations.directory != nil || entry.logicalObligations.directory != nil) {
		return 0, false
	}
	if prior == nil || prior.logicalLane != entry.logicalLane || prior.resourceID != entry.resourceID {
		return 0, false
	}
	coalesce, err := stableResourcesCoalesce(prior.token, entry.token)
	if err != nil || !coalesce || prior.token.kind != entry.token.kind || entry.token.stability != ResourceMutableAppend || entry.token.kind == ResourceIndex {
		return 0, false
	}
	if entry.token.namespace != nil && (prior.token.namespace == nil || !prior.token.namespace.compatible(entry.token.namespace)) {
		return 0, false
	}
	if !durableFrontierCovers(entry.frontier, prior.frontier) {
		return 0, false
	}
	for _, stableBinding1 := range entry.reachability.records() {
		field := stableBinding1.key
		if _, exists := prior.reachability.lookup(field); !exists {
			return 0, false
		}
	}
	exact := true
	prior.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		current, exists := findStableLogicalObligationIndex(entry.logicalObligations.index, obligation, nil)
		exact = exists && current == obligation
		return exact
	})
	if !exact {
		return 0, false
	}
	entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		if previous, exists := findStableLogicalObligationIndex(prior.logicalObligations.index, obligation, nil); exists {
			exact = previous == obligation
			return exact
		}
		offset, length := uint64(obligation.Offset), uint64(obligation.Length)
		exact = obligation.Offset >= 0 && obligation.Length > 0 && offset >= prior.frontier.Bytes && offset <= entry.frontier.Bytes && length <= entry.frontier.Bytes-offset
		return exact
	})
	return prior.frontier.Bytes, exact
}

func stableResourceEntryCoversPublication(prior, entry *stableResourceEntry) bool {
	if prior != nil && (prior.logicalObligations.directory != nil || entry.logicalObligations.directory != nil) {
		if prior.logicalObligations.directory != entry.logicalObligations.directory || prior.logicalObligations.tail != entry.logicalObligations.tail || len(prior.logicalObligations.removed) != 0 || len(entry.logicalObligations.removed) != 0 {
			return false
		}
	}
	if prior == nil || prior.logicalLane != entry.logicalLane || prior.resourceID != entry.resourceID {
		return false
	}
	coalesce, err := stableResourcesCoalesce(prior.token, entry.token)
	if err != nil || !coalesce || prior.token.kind != entry.token.kind || entry.token.kind == ResourceIndex {
		return false
	}
	// Namespace compatibility during union permits a missing operation on either
	// side. Credit is directional: a new operation needs existing exact authority.
	if entry.token.namespace != nil && (prior.token.namespace == nil || !prior.token.namespace.compatible(entry.token.namespace)) {
		return false
	}
	// Registration validates and owns frontiers; frozen entry updates only clone
	// or union that state. Coverage need not rebuild their exact RID summaries.
	if !durableFrontierCovers(prior.frontier, entry.frontier) {
		return false
	}
	for _, stableBinding1 := range entry.reachability.records() {
		field := stableBinding1.key
		if _, exists := prior.reachability.lookup(field); !exists {
			return false
		}
	}
	if prior.logicalObligations.index == entry.logicalObligations.index && prior.logicalObligations.count == entry.logicalObligations.count {
		return true
	}
	covered := true
	entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		previous, exists := findStableLogicalObligationIndex(prior.logicalObligations.index, obligation, nil)
		covered = exists && previous == obligation
		return covered
	})
	return covered
}

func (set *StableResourceSet) Stats(now time.Time) []ResourceKindStats {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if err := requireOrdinaryStableResourceInputs(nil, nil, set); err != nil {
		return nil
	}
	byKind := make(map[ResourceKind]*ResourceKindStats)
	seenNamespaces := make(map[*StableNamespaceToken]struct{})
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil {
			return true
		}
		stats := byKind[token.kind]
		if stats == nil {
			stats = &ResourceKindStats{Kind: token.kind, LogicalObligationCountAvailable: !set.physicalOnly}
			byKind[token.kind] = stats
		}
		stats.PendingCount++
		stats.PendingBytes = saturatingAdd(stats.PendingBytes, entry.frontier.Bytes)
		stats.LogicalObligationCount = saturatingAdd(stats.LogicalObligationCount, uint64(entry.logicalObligations.count))
		registered := time.Unix(0, token.metrics.registeredNanos)
		age := now.Sub(registered)
		if age > stats.PendingAge {
			stats.PendingAge = age
		}
		stats.Flushes += token.metrics.flushes.Load()
		stats.FlushDuration += time.Duration(token.metrics.flushNanos.Load())
		stats.Syncs += token.metrics.syncs.Load()
		stats.SyncDuration += time.Duration(token.metrics.syncNanos.Load())
		stats.PhysicalFileSyncs += token.metrics.physicalFileSyncs.Load()
		stats.PhysicalFileSyncDuration += time.Duration(token.metrics.physicalFileSyncNanos.Load())
		if token.namespace != nil {
			if _, seen := seenNamespaces[token.namespace]; !seen {
				seenNamespaces[token.namespace] = struct{}{}
				syncs, duration := token.namespace.physicalSyncStats()
				stats.NamespaceSyncs += syncs
				stats.NamespaceSyncDuration += duration
			}
		}
		active := false
		if len(entry.pins) == 0 {
			active = entry.token != nil && !entry.token.released.Load()
		} else {
			for _, pin := range entry.pins {
				if pin != nil && !pin.released.Load() {
					active = true
					break
				}
			}
		}
		if active {
			stats.ActivePins++
		}
		return true
	})
	for kind, highWater := range set.pinHighWater {
		stats := byKind[kind]
		if stats == nil {
			stats = &ResourceKindStats{Kind: kind}
			byKind[kind] = stats
		}
		stats.PinHighWater = highWater
	}
	result := make([]ResourceKindStats, 0, len(byKind))
	for _, stats := range byKind {
		result = append(result, *stats)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result
}

// adjustActivePinsByKind updates a coordinator-owned aggregate without
// allocating per-set statistics. A pending candidate contributes one active
// pin per live resource entry, matching Stats.ActivePins. The coordinator owns
// every set while it is pending, so additions at enqueue and subtractions just
// before release form an exact running total.
func (set *StableResourceSet) adjustActivePinsByKind(counts map[ResourceKind]uint64, add bool) {
	if set == nil || counts == nil {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil {
			return true
		}
		active := false
		if len(entry.pins) == 0 {
			active = entry.token != nil && !entry.token.released.Load()
		} else {
			for _, pin := range entry.pins {
				if pin != nil && !pin.released.Load() {
					active = true
					break
				}
			}
		}
		if !active {
			return true
		}
		if add {
			counts[token.kind] = saturatingAdd(counts[token.kind], 1)
			return true
		}
		if counts[token.kind] <= 1 {
			delete(counts, token.kind)
		} else {
			counts[token.kind]--
		}
		return true
	})
}

func (set *StableResourceSet) validateResolved() error {
	if err := set.RequireMetadataExport(); err != nil {
		return err
	}
	if set != nil && set.physicalOnly {
		return ErrResourceOwnership
	}
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	var validateErr error
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if err := entry.token.namespace.validateStable(); err != nil {
			validateErr = err
			return false
		}
		stat, err := entry.token.pinned.Stat()
		if err != nil {
			validateErr = err
			return false
		}
		if entry.frontier.Bytes > uint64(stat.Size()) {
			validateErr = ErrFrontierBeyondResource
			return false
		}
		return true
	})
	return validateErr
}

func resourceSetsFromCandidates(candidates []*PreparedRootCandidate) []*StableResourceSet {
	sets := make([]*StableResourceSet, 0, len(candidates))
	for _, candidate := range candidates {
		if set := candidate.resourceSet(); set != nil {
			sets = append(sets, set)
		}
	}
	return sets
}

func stableSetPhysicalCount(sets []*StableResourceSet) int {
	count := 0
	for _, set := range sets {
		count += set.Len()
	}
	return count
}

func resourceSetConflict(err error) bool {
	return errors.Is(err, ErrResourceConflict) || errors.Is(err, ErrFrontierBeyondResource) ||
		errors.Is(err, ErrNamespaceUnstable) || errors.Is(err, ErrNamespacePersistenceUnsupported)
}

func stablePhysicalResourceKeyLess(a, b stablePhysicalResourceKey) bool {
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	ai := stablePhysicalIdentityKey{platform: a.platform, volumeID: a.volumeID, objectID: a.objectID}
	bi := stablePhysicalIdentityKey{platform: b.platform, volumeID: b.volumeID, objectID: b.objectID}
	if ai != bi {
		return stablePhysicalIdentityKeyLess(ai, bi)
	}
	return a.generation < b.generation
}
func newStableTokenTable(tokens ...*StableResourceToken) *stableTable[*StableResourceToken, struct{}] {
	t := newStableTable[*StableResourceToken, struct{}](func(a, b *StableResourceToken) bool { return uintptr(unsafe.Pointer(a)) < uintptr(unsafe.Pointer(b)) })
	p := &t
	for _, token := range tokens {
		p.set(token, struct{}{})
	}
	return p
}

// A lookup admission prepares its entire cross-table closure before allocating
// any node. The enclosing builder lock certifies both tables through apply.
// Keys are immutable aliases of the actual owned entry, never unowned copies.
type preparedStableResourceLookupAdd struct {
	logical                   stableTableInsert[stableLogicalResourceKey, int]
	physical                  stableTableInsert[stablePhysicalIdentityKey, int]
	collision                 stableTableInsert[stablePhysicalResourceKey, int]
	hasPhysical, hasCollision bool
}

func (lookup *stableResourceEntryLookup) prepareAdd(entries []stableResourceEntry, index int, account StableMetadataAccount) (preparedStableResourceLookupAdd, error) {
	var p preparedStableResourceLookupAdd
	token := entries[index].token
	identityKey := token.physicalIdentityKey()
	coalescingKey := token.physicalCoalescingKey()
	logicalBytes, err := lookup.logical.plannedInsertBytes(token.logicalKey())
	if err != nil && account != nil {
		return p, err
	}
	existingPosition := lookup.physical.get(identityKey)
	p.hasPhysical = existingPosition == 0 || entries[existingPosition-1].token.physicalCoalescingKey() == coalescingKey
	p.hasCollision = !p.hasPhysical
	if p.hasCollision && lookup.physicalCollisions.less == nil {
		lookup.physicalCollisions = newStableTable[stablePhysicalResourceKey, int](stablePhysicalResourceKeyLess)
	}
	var secondBytes uint64
	if p.hasPhysical {
		secondBytes, err = lookup.physical.plannedInsertBytes(identityKey)
	} else {
		secondBytes, err = lookup.physicalCollisions.plannedInsertBytes(coalescingKey)
	}
	if err != nil && account != nil {
		return p, err
	}
	if account != nil {
		bytes, err := finiteStableAdd(logicalBytes, secondBytes)
		if err != nil {
			return p, err
		}
		if err = account.ReserveStableMetadata(bytes); err != nil {
			return p, err
		}
	}
	// All credit is already acquired. These constructors allocate only their
	// independently stamped nodes; Apply performs no allocation or credit calls.
	p.logical, err = lookup.logical.prepare(token.logicalKey(), index, nil)
	if err != nil {
		return p, err
	}
	if p.hasPhysical {
		p.physical, err = lookup.physical.prepare(identityKey, index+1, nil)
	} else {
		p.collision, err = lookup.physicalCollisions.prepare(coalescingKey, index+1, nil)
	}
	return p, err
}
func (p preparedStableResourceLookupAdd) apply() {
	p.logical.apply()
	if p.hasPhysical {
		p.physical.apply()
	}
	if p.hasCollision {
		p.collision.apply()
	}
}
