package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type stableResourceEntry struct {
	token                *StableResourceToken
	pins                 []*StableResourceToken
	pinIndex             map[*StableResourceToken]struct{}
	logicalLane          string
	resourceID           string
	diagnosticPath       string
	frontier             DurableFrontier
	reachability         map[ReachabilityField]struct{}
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
	if view.index == nil && view.count != 0 {
		view = newStableLogicalObligationView(view.slice())
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

func (view stableLogicalObligationView) rangeValues(visit func(StableLogicalObligation) bool) {
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

func (view stableLogicalObligationView) slice() []StableLogicalObligation {
	if view.count == 0 {
		return nil
	}
	result := make([]StableLogicalObligation, 0, view.count)
	view.rangeValues(func(obligation StableLogicalObligation) bool {
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
	logical map[stableLogicalResourceKey]int
	// physical points back into the ordered entries for identity-wide conflict
	// metadata and the first exact candidate. Only identities with multiple
	// generations need the collision map.
	physical           map[stablePhysicalIdentityKey]int
	physicalCollisions map[stablePhysicalResourceKey]int
}

const stableResourceEntryLinearLookupLimit = 16

func newStableResourceEntryLookup(entries []stableResourceEntry) stableResourceEntryLookup {
	lookup := stableResourceEntryLookup{
		logical:  make(map[stableLogicalResourceKey]int, len(entries)),
		physical: make(map[stablePhysicalIdentityKey]int, len(entries)),
	}
	for i := range entries {
		lookup.add(entries, i)
	}
	return lookup
}

func (lookup *stableResourceEntryLookup) add(entries []stableResourceEntry, index int) {
	entry := entries[index]
	lookup.logical[entry.token.logicalKey()] = index
	identityKey := entry.token.physicalIdentityKey()
	coalescingKey := entry.token.physicalCoalescingKey()
	position := index + 1
	existingPosition := lookup.physical[identityKey]
	if existingPosition == 0 {
		lookup.physical[identityKey] = position
		return
	}
	if entries[existingPosition-1].token.physicalCoalescingKey() == coalescingKey {
		lookup.physical[identityKey] = position
		return
	}
	if lookup.physicalCollisions == nil {
		lookup.physicalCollisions = make(map[stablePhysicalResourceKey]int)
	}
	lookup.physicalCollisions[coalescingKey] = position
}

type stableResourceEntryLookupIterator struct {
	single int
}

func (lookup *stableResourceEntryLookup) physicalIterator(entries []stableResourceEntry, token *StableResourceToken) (stableResourceEntryLookupIterator, error) {
	position := lookup.physical[token.physicalIdentityKey()]
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
	return stableResourceEntryLookupIterator{single: lookup.physicalCollisions[coalescingKey]}, nil
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
	if lookup.logical[old.logicalKey()] == index {
		delete(lookup.logical, old.logicalKey())
	}
	lookup.logical[replacement.logicalKey()] = index
}

func cloneStableResourceEntry(entry stableResourceEntry) stableResourceEntry {
	reachability := make(map[ReachabilityField]struct{}, len(entry.reachability))
	for field := range entry.reachability {
		reachability[field] = struct{}{}
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
		entry.pinIndex = make(map[*StableResourceToken]struct{}, len(entry.pins)+len(incoming))
		for _, token := range entry.pins {
			entry.pinIndex[token] = struct{}{}
		}
	}
	for _, token := range incoming {
		if token == nil {
			continue
		}
		if _, ok := entry.pinIndex[token]; ok {
			continue
		}
		entry.pinIndex[token] = struct{}{}
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
	targetValues := target.slice()
	incomingValues := incoming.slice()
	if len(targetValues)+len(incomingValues) > stableLogicalObligationLinearLimit {
		byKey := make(map[stableLogicalObligationIndex]StableLogicalObligation, len(targetValues)+len(incomingValues))
		for _, obligation := range targetValues {
			byKey[stableLogicalObligationKey(obligation)] = obligation
		}
		// Preflight the complete incoming batch before mutating the live builder.
		// Add is atomic: a later conflict must not leave an earlier obligation
		// appended to the existing resource entry.
		for _, obligation := range incomingValues {
			key := stableLogicalObligationKey(obligation)
			if existing, ok := byKey[key]; ok {
				if existing != obligation {
					return fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
				}
			}
		}
		for _, obligation := range incomingValues {
			key := stableLogicalObligationKey(obligation)
			if _, ok := byKey[key]; ok {
				continue
			}
			byKey[key] = obligation
			targetValues = append(targetValues, obligation)
		}
		*target = newStableLogicalObligationView(targetValues)
		return nil
	}
	// The small-set path deliberately stays allocation-free. Its first pass is
	// conflict-only; the second pass applies additions after the whole batch is
	// known compatible.
	for _, obligation := range incomingValues {
		key := stableLogicalObligationKey(obligation)
		for _, existing := range targetValues {
			if stableLogicalObligationKey(existing) != key {
				continue
			}
			if existing != obligation {
				return fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
			}
			break
		}
	}
	for _, obligation := range incomingValues {
		key := stableLogicalObligationKey(obligation)
		duplicate := false
		for _, existing := range targetValues {
			if stableLogicalObligationKey(existing) == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			targetValues = append(targetValues, obligation)
		}
	}
	*target = newStableLogicalObligationView(targetValues)
	return nil
}

func stableLogicalObligationList(obligations []StableLogicalObligation) []StableLogicalObligation {
	return obligations[:len(obligations):len(obligations)]
}

type StableResourceSetBuilder struct {
	mu        sync.Mutex
	entries   []stableResourceEntry
	kindViews map[ResourceKind]stableResourceKindView
	required  map[ReachabilityField]struct{}
	closed    bool
	abandoned bool
	state     ResourceOwnerState
	indexed   *stableResourceBuilderIndexedState
}

type stableResourceBuilderIndexedState struct {
	// Keep scale-only state out of the common small builder allocation.
	lookup stableResourceEntryLookup
	work   StableResourceClosureWork
}

func NewStableResourceSetBuilder(required ...ReachabilityField) *StableResourceSetBuilder {
	requiredSet := make(map[ReachabilityField]struct{}, len(required))
	for _, field := range required {
		if field != "" {
			requiredSet[field] = struct{}{}
		}
	}
	return &StableResourceSetBuilder{required: requiredSet, state: ResourceOwnerBuilder}
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

func (builder *StableResourceSetBuilder) Add(token *StableResourceToken) error {
	if builder == nil || token == nil {
		return ErrResourceOwnership
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()
	if builder.closed || builder.abandoned {
		return ErrResourceOwnership
	}
	if builder.kindViews != nil {
		return builder.addToViewsLocked(token)
	}
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		return err
	}
	if builder.indexed == nil {
		if len(builder.entries) < stableResourceEntryLinearLookupLimit {
			err := mergeOwnedTokenLinear(&builder.entries, token)
			if err != nil {
				token.releaseFrom(ResourceOwnerBuilder)
			}
			return err
		}
		builder.indexed = &stableResourceBuilderIndexedState{
			lookup: newStableResourceEntryLookup(builder.entries),
		}
	}
	if err := mergeOwnedToken(&builder.entries, &builder.indexed.lookup, token, &builder.indexed.work); err != nil {
		token.releaseFrom(ResourceOwnerBuilder)
		return err
	}
	return nil
}

func (builder *StableResourceSetBuilder) addToViewsLocked(token *StableResourceToken) error {
	if err := token.namespace.validateStable(); err != nil {
		return err
	}
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		return err
	}
	incoming := []stableResourceEntry{{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	}}
	if !stableResourceViewsConflict(builder.kindViews, &incoming[0]) {
		views, err := buildStableResourceKindViews(incoming)
		if err != nil {
			token.releaseFrom(ResourceOwnerBuilder)
			return err
		}
		merged, ok := mergeDistinctStableResourceKindViews(builder.kindViews, views)
		if !ok {
			releaseStableResourceKindViews(views)
			return ErrResourceConflict
		}
		builder.kindViews = merged
		return nil
	}

	// Exact identity collisions retain the established coalescing rules.
	temporary := NewStableResourceSetBuilder()
	if err := cloneStableResourceViewsIntoBuilder(temporary, builder.kindViews); err != nil {
		temporary.Abandon()
		token.releaseFrom(ResourceOwnerBuilder)
		return err
	}
	temporary.mu.Lock()
	var err error
	if temporary.indexed == nil && len(temporary.entries) < stableResourceEntryLinearLookupLimit {
		err = mergeOwnedTokenLinear(&temporary.entries, token)
	} else {
		if temporary.indexed == nil {
			temporary.indexed = &stableResourceBuilderIndexedState{lookup: newStableResourceEntryLookup(temporary.entries)}
		}
		err = mergeOwnedToken(&temporary.entries, &temporary.indexed.lookup, token, &temporary.indexed.work)
	}
	if err != nil {
		temporary.mu.Unlock()
		temporary.Abandon()
		token.releaseFrom(ResourceOwnerBuilder)
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
	releaseStableResourceKindViews(oldViews)
	return nil
}

func mergeOwnedTokenLinear(entries *[]stableResourceEntry, token *StableResourceToken) error {
	logicalKey := token.logicalKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(token) {
			return fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
		coalesce, err := stableResourcesCoalesce(existing, token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, newStableLogicalObligationView(token.logicalObligations)); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability[token.reachability] = struct{}{}
		mergeStableResourceDescriptorIdentity(entry, token.logicalLane, token.resourceID, token.diagnosticPath)
		if existing.namespace == nil && token.namespace != nil {
			entry.token = token
			entry.pins = nil
			entry.pinIndex = nil
			existing.releaseFrom(ResourceOwnerBuilder)
		} else {
			token.releaseFrom(ResourceOwnerBuilder)
		}
		return nil
	}
	*entries = append(*entries, stableResourceEntry{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	})
	return nil
}

func mergeOwnedToken(entries *[]stableResourceEntry, lookup *stableResourceEntryLookup, token *StableResourceToken, work *StableResourceClosureWork) error {
	logicalKey := token.logicalKey()
	work.PhysicalEntryLookupProbes++
	if existingIndex, ok := lookup.logical[logicalKey]; ok {
		existing := (*entries)[existingIndex].token
		if !existing.samePhysicalIdentity(token) {
			return fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, logicalKey)
		}
	}
	iterator, err := lookup.physicalIterator(*entries, token)
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
		work.PhysicalEntryLookupComparisons++
		coalesce, err := stableResourcesCoalesce(existing, token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, newStableLogicalObligationView(token.logicalObligations)); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability[token.reachability] = struct{}{}
		mergeStableResourceDescriptorIdentity(entry, token.logicalLane, token.resourceID, token.diagnosticPath)
		if existing.namespace == nil && token.namespace != nil {
			// Preserve the one namespace-creation obligation independently of
			// insertion order by making its exact-handle token representative.
			entry.token = token
			lookup.replaceRepresentative(i, existing, token)
			entry.pins = nil
			entry.pinIndex = nil
			existing.releaseFrom(ResourceOwnerBuilder)
		} else {
			token.releaseFrom(ResourceOwnerBuilder)
		}
		return nil
	}
	*entries = append(*entries, stableResourceEntry{
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:         map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations:   newStableLogicalObligationView(token.logicalObligations),
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	})
	lookup.add(*entries, len(*entries)-1)
	work.PhysicalEntryLookupAdmissions++
	return nil
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

func mergeViewEntry(entries *[]stableResourceEntry, lookup *stableResourceEntryLookup, incoming stableResourceEntry, retainSourcePins bool, work *StableResourceClosureWork) error {
	logicalKey := incoming.token.logicalKey()
	if work != nil {
		work.PhysicalEntryLookupProbes++
	}
	if existingIndex, ok := lookup.logical[logicalKey]; ok {
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
		if !existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, incoming.logicalObligations); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
		mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
		for field := range incoming.reachability {
			entry.reachability[field] = struct{}{}
		}
		if retainSourcePins {
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
			if !retainSourcePins {
				entry.pins = nil
				entry.pinIndex = nil
			}
		}
		return nil
	}
	*entries = append(*entries, cloneStableResourceEntry(incoming))
	lookup.add(*entries, len(*entries)-1)
	if work != nil {
		work.PhysicalEntryLookupAdmissions++
	}
	return nil
}

func mergeViewEntryLinear(entries *[]stableResourceEntry, incoming stableResourceEntry, retainSourcePins bool, work *StableResourceClosureWork) error {
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
		if !existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %+v", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(&entry.logicalObligations, incoming.logicalObligations); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
		mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
		for field := range incoming.reachability {
			entry.reachability[field] = struct{}{}
		}
		if retainSourcePins {
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
			if !retainSourcePins {
				entry.pins = nil
				entry.pinIndex = nil
			}
		}
		return nil
	}
	*entries = append(*entries, cloneStableResourceEntry(incoming))
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
		entry.logicalObligations, err = entry.logicalObligations.appendCertified(incoming.logicalObligations.slice(), work)
		if err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
		mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
		for field := range incoming.reachability {
			entry.reachability[field] = struct{}{}
		}
		if existing.namespace == nil && incoming.token.namespace != nil {
			entry.token = incoming.token
			entry.pins = nil
			entry.pinIndex = nil
		}
		return nil
	}
	*entries = append(*entries, cloneStableResourceEntry(incoming))
	return nil
}

func cloneStableResourceEntryIntoBuilder(builder *StableResourceSetBuilder, source *stableResourceEntry) error {
	if builder == nil || source == nil {
		return ErrResourceOwnership
	}
	token := activeEntryToken(*source)
	if token == nil || token.released.Load() {
		return ErrResourceOwnership
	}
	if err := token.namespace.validateStable(); err != nil {
		return err
	}
	fields := make([]ReachabilityField, 0, len(source.reachability))
	for field := range source.reachability {
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return ErrUnresolvedResource
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	identity := token.identity
	var registry *IdentityPinRegistry
	if token.identityPin != nil {
		registry = token.identityPin.registry
		if err := registry.Observe(identity); err != nil {
			return err
		}
	}
	cloned, err := token.cloneSharedPinned(
		source.logicalLane, source.resourceID, source.diagnosticPath,
		source.frontier, fields[0], source.logicalObligations.slice(), func() {
			if registry != nil {
				_ = registry.Unobserve(identity)
			}
		},
	)
	if err != nil {
		if registry != nil {
			_ = registry.Unobserve(identity)
		}
		return err
	}
	before := len(builder.entries)
	if err := builder.Add(cloned); err != nil {
		cloned.Release()
		return err
	}
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
		destination.reachability[field] = struct{}{}
	}
	if len(builder.entries) > before {
		destination.logicalObligations = source.logicalObligations
		destination.dependencyManifestV1 = source.dependencyManifestV1
	}
	return nil
}

func cloneStableResourceViewsIntoBuilder(builder *StableResourceSetBuilder, views map[ResourceKind]stableResourceKindView) error {
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

func (builder *StableResourceSetBuilder) mergeViewSet(child *StableResourceSet) (bool, error) {
	if builder == nil || child == nil {
		return true, ErrResourceOwnership
	}
	builder.mu.Lock()
	if builder.closed || builder.abandoned {
		builder.mu.Unlock()
		return true, ErrResourceOwnership
	}
	child.mu.Lock()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder || child.kindViews == nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		return false, nil
	}
	if builder.kindViews == nil && len(builder.entries) != 0 {
		// A mutable flat builder keeps the established exact representative and
		// work accounting. Only the frozen child is materialized.
		temporary := NewStableResourceSetBuilder()
		if err := cloneStableResourceViewsIntoBuilder(temporary, child.kindViews); err != nil {
			child.mu.Unlock()
			builder.mu.Unlock()
			temporary.Abandon()
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
			if lookup.logical == nil {
				err = mergeViewEntryLinear(&merged, entry, false, nil)
			} else {
				err = mergeViewEntry(&merged, &lookup, entry, false, &indexedWork)
			}
			if err != nil {
				temporary.mu.Unlock()
				child.mu.Unlock()
				builder.mu.Unlock()
				temporary.Abandon()
				return true, err
			}
		}
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			temporary.mu.Unlock()
			child.mu.Unlock()
			builder.mu.Unlock()
			temporary.Abandon()
			return true, ErrResourceOwnership
		}
		dropped := droppedStableResourceTokens(merged, builder.entries, incoming)
		oldChildViews := child.kindViews
		builder.entries = merged
		if lookup.logical == nil {
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
		child.mu.Unlock()
		builder.mu.Unlock()
		releaseStableResourceKindViews(oldChildViews)
		for _, token := range dropped {
			token.releaseFrom(ResourceOwnerBuilder)
		}
		return true, nil
	}
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		return true, err
	}
	merged, distinct := mergeDistinctStableResourceKindViews(builder.kindViews, child.kindViews)
	if distinct {
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			child.mu.Unlock()
			builder.mu.Unlock()
			return true, ErrResourceOwnership
		}
		builder.kindViews = merged
		child.kindViews = nil
		child.entries = nil
		child.mu.Unlock()
		builder.mu.Unlock()
		return true, nil
	}

	// A collision is uncommon on the certified production path. Preserve every
	// existing coalescing and conflict rule by rebuilding an independently pinned
	// flat builder, then resume the ordinary exact implementation.
	currentViews := builder.kindViews
	childViews := child.kindViews

	temporary := NewStableResourceSetBuilder()
	if err := cloneStableResourceViewsIntoBuilder(temporary, currentViews); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
		return true, err
	}
	if err := cloneStableResourceViewsIntoBuilder(temporary, childViews); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
		return true, err
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
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
	child.mu.Unlock()
	builder.mu.Unlock()
	releaseStableResourceKindViews(oldBuilderViews)
	releaseStableResourceKindViews(oldChildViews)
	return true, nil
}

// Merge consumes a child builder-owned set only after the complete transitive
// union has passed conflict checks. This is the one-way child-to-parent
// transfer used before a parent installs a child root or catalog ID.
func (builder *StableResourceSetBuilder) Merge(child *StableResourceSet) error {
	if handled, err := builder.mergeViewSet(child); handled {
		return err
	}
	if builder == nil || child == nil {
		return ErrResourceOwnership
	}
	builder.mu.Lock()
	if builder.closed || builder.abandoned {
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	child.mu.Lock()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		child.mu.Unlock()
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
		if lookup.logical == nil {
			err = mergeViewEntryLinear(&merged, entry, false, nil)
		} else {
			err = mergeViewEntry(&merged, &lookup, entry, false, &indexedWork)
		}
		if err != nil {
			child.mu.Unlock()
			builder.mu.Unlock()
			return err
		}
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		child.mu.Unlock()
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	dropped := droppedStableResourceTokens(merged, builder.entries, child.entries)
	// Kept tokens remain in the builder ownership phase. Only duplicates omitted
	// from the committed merged view are released, and only after the child CAS
	// makes the ownership transfer irreversible.
	builder.entries = merged
	if lookup.logical == nil {
		builder.indexed = nil
	} else {
		builder.indexed = &stableResourceBuilderIndexedState{lookup: lookup, work: indexedWork}
	}
	child.entries = nil
	child.mu.Unlock()
	builder.mu.Unlock()
	for _, token := range dropped {
		token.releaseFrom(ResourceOwnerBuilder)
	}
	return nil
}

// MergeAppendOnlyLogicalObligations consumes a producer set using exact
// removal-free mutation evidence. Distinct immutable physical roots transfer
// directly; identity collisions retain the existing exact coalescing path.
func (builder *StableResourceSetBuilder) MergeAppendOnlyLogicalObligations(child *StableResourceSet, mutation StableLogicalObligationMutation) (StableResourceClosureWork, error) {
	if builder == nil || child == nil {
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	builder.mu.Lock()
	if builder.closed || builder.abandoned {
		builder.mu.Unlock()
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	child.mu.Lock()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		child.mu.Unlock()
		builder.mu.Unlock()
		return StableResourceClosureWork{}, ErrResourceOwnership
	}
	if child.kindViews == nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		return builder.mergeAppendOnlyLogicalObligationsFlat(child, mutation)
	}
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		return StableResourceClosureWork{}, err
	}

	merged, distinct := mergeDistinctStableResourceKindViews(builder.kindViews, child.kindViews)
	if distinct {
		normalized, work, err := validateAppendOnlyProducerViews(child.kindViews, mutation)
		_ = normalized
		if err != nil {
			child.mu.Unlock()
			builder.mu.Unlock()
			return work, err
		}
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			child.mu.Unlock()
			builder.mu.Unlock()
			return work, ErrResourceOwnership
		}
		builder.kindViews = merged
		child.kindViews = nil
		child.entries = nil
		child.mu.Unlock()
		builder.mu.Unlock()
		work.AppendOnlyFastPath = 1
		return work, nil
	}
	if plan, work, certified, err := certifiedAppendOnlyPhysicalCoalesce(builder.kindViews, child.kindViews, mutation); certified || err != nil {
		if err != nil {
			plan.abandon()
			child.mu.Unlock()
			builder.mu.Unlock()
			return work, err
		}
		if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
			plan.abandon()
			child.mu.Unlock()
			builder.mu.Unlock()
			return work, ErrResourceOwnership
		}
		builder.kindViews = plan.views
		child.kindViews = nil
		child.entries = nil
		child.mu.Unlock()
		builder.mu.Unlock()
		releaseStableResourceKindViews(plan.releaseIncoming)
		work.AppendOnlyFastPath = 1
		work.AppendOnlyCollisionFastPath = 1
		return work, nil
	}

	// Identity collisions are rare in the production append path. Materialize
	// an independently pinned exact candidate so every established coalescing
	// and conflict rule remains authoritative.
	temporary := NewStableResourceSetBuilder()
	if err := cloneStableResourceViewsIntoBuilder(temporary, builder.kindViews); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
		return StableResourceClosureWork{}, err
	}
	temporaryChildBuilder := NewStableResourceSetBuilder()
	if err := cloneStableResourceViewsIntoBuilder(temporaryChildBuilder, child.kindViews); err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
		temporaryChildBuilder.Abandon()
		return StableResourceClosureWork{}, err
	}
	temporaryChildBuilder.mu.Lock()
	temporaryChild := &StableResourceSet{entries: temporaryChildBuilder.entries, createdAt: child.createdAt}
	temporaryChild.owner.Store(uint32(ResourceOwnerBuilder))
	temporaryChildBuilder.entries = nil
	temporaryChildBuilder.closed = true
	temporaryChildBuilder.mu.Unlock()

	work, err := temporary.mergeAppendOnlyLogicalObligationsFlat(temporaryChild, mutation)
	work.AppendOnlyCollisionFallbacks = 1
	if err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
		temporaryChild.Release()
		return work, err
	}
	work.CopiedEntries += uint64(stableResourceKindViewCount(builder.kindViews) + stableResourceKindViewCount(child.kindViews))
	work.PhysicalHandleShares += uint64(stableResourceKindViewCount(builder.kindViews) + stableResourceKindViewCount(child.kindViews))
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		child.mu.Unlock()
		builder.mu.Unlock()
		temporary.Abandon()
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
	child.mu.Unlock()
	builder.mu.Unlock()
	releaseStableResourceKindViews(oldBuilderViews)
	releaseStableResourceKindViews(oldChildViews)
	return work, nil
}

type certifiedAppendOnlyPhysicalCoalescePlan struct {
	views           map[ResourceKind]stableResourceKindView
	releaseIncoming map[ResourceKind]stableResourceKindView
	staged          map[ResourceKind]stableResourceKindView
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
func certifiedAppendOnlyPhysicalCoalesce(target, incoming map[ResourceKind]stableResourceKindView, mutation StableLogicalObligationMutation) (*certifiedAppendOnlyPhysicalCoalescePlan, StableResourceClosureWork, bool, error) {
	if stableResourceKindViewCount(target)+stableResourceKindViewCount(incoming) <= stableResourceEntryLinearLookupLimit {
		return nil, StableResourceClosureWork{}, false, nil
	}
	if stableResourceKindViewCount(incoming) > stableResourceEntryLinearLookupLimit {
		return nil, StableResourceClosureWork{}, false, nil
	}
	_, work, err := validateAppendOnlyProducerViews(incoming, mutation)
	if err != nil {
		return nil, work, true, err
	}
	nextViews := make(map[ResourceKind]stableResourceKindView, len(target))
	for kind, current := range target {
		nextViews[kind] = current
	}
	distinct := make(map[ResourceKind][]*stableResourceEntry)
	collisions := 0
	var preflightErr error
	certified := true
	rangeStableResourceKindViews(incoming, func(child *stableResourceEntry) bool {
		view, hadKind := nextViews[child.token.kind]
		replacedLogicalCommitments := false
		if !hadKind {
			view.reachability = make(map[ReachabilityField]struct{})
		}
		existing := findStableResourceLogical(view.logical, child.token.logicalKey())
		if existing != nil && !existing.token.samePhysicalIdentity(child.token) {
			preflightErr = fmt.Errorf("%w: logical resource %+v changed stable identity", ErrResourceConflict, child.token.logicalKey())
			return false
		}

		physicalKey := child.token.physicalIdentityKey()
		var candidates []*stableResourceEntry
		for kind, other := range nextViews {
			matches := findStableResourcePhysical(other.physical, physicalKey)
			if len(matches) == 0 {
				continue
			}
			if kind != child.token.kind || candidates != nil {
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
			nextEntry.logicalObligations, preflightErr = nextEntry.logicalObligations.appendCertified(child.logicalObligations.slice(), &work)
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
			for field := range child.reachability {
				nextEntry.reachability[field] = struct{}{}
			}
			view.logical = insertStableResourceLogical(view.logical, &nextEntry)
			view.physical = replaceStableResourcePhysical(view.physical, existing, &nextEntry)
			collisions++
		default:
			certified = false
			return false
		}
		view.reachability = cloneReachabilityUnion(view.reachability, child.reachability)
		if !replacedLogicalCommitments {
			view.logicalCommitments = addStableLogicalObligationCommitments(view.logicalCommitments, child.logicalObligations.commitments)
			view.logicalObligationCount += child.logicalObligations.count
		}
		nextViews[child.token.kind] = view
		return true
	})
	if preflightErr != nil {
		return nil, work, true, preflightErr
	}
	if !certified || collisions == 0 {
		return nil, work, false, nil
	}
	plan := &certifiedAppendOnlyPhysicalCoalescePlan{
		views: nextViews, releaseIncoming: make(map[ResourceKind]stableResourceKindView),
		staged: make(map[ResourceKind]stableResourceKindView),
	}
	// Root concatenation allocates lineage but does not retain or mutate either
	// input. Mixed roots clone only their distinct delta before the ownership CAS.
	for kind, child := range incoming {
		view := plan.views[kind]
		current, hadTarget := target[kind]
		if !hadTarget && len(distinct[kind]) != child.count {
			plan.abandon()
			return nil, work, true, ErrUnresolvedResource
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
			plan.releaseIncoming[kind] = child
		default:
			staged, cloneErr := cloneStableResourceEntriesToKindView(distinct[kind])
			if cloneErr != nil {
				plan.abandon()
				return nil, work, true, cloneErr
			}
			plan.staged[kind] = staged
			plan.releaseIncoming[kind] = child
			for _, original := range distinct[kind] {
				replacement := findStableResourceLogical(staged.logical, original.token.logicalKey())
				view.logical = insertStableResourceLogical(view.logical, replacement)
				view.physical = replaceStableResourcePhysical(view.physical, original, replacement)
			}
			view.root = concatOwnedStableResourceEntryNodes(current.root, staged.root)
			work.CopiedEntries += uint64(len(distinct[kind]))
			work.PhysicalHandleShares += uint64(len(distinct[kind]))
		}
		plan.views[kind] = view
	}
	return plan, work, true, nil
}

func cloneStableResourceEntriesToKindView(entries []*stableResourceEntry) (stableResourceKindView, error) {
	builder := NewStableResourceSetBuilder()
	for _, entry := range entries {
		if err := cloneStableResourceEntryIntoBuilder(builder, entry); err != nil {
			builder.Abandon()
			return stableResourceKindView{}, err
		}
	}
	builder.mu.Lock()
	if err := builder.promoteEntriesToViewsLocked(); err != nil {
		builder.mu.Unlock()
		builder.Abandon()
		return stableResourceKindView{}, err
	}
	if len(entries) == 0 || len(builder.kindViews) != 1 {
		builder.mu.Unlock()
		builder.Abandon()
		return stableResourceKindView{}, ErrUnresolvedResource
	}
	view, ok := builder.kindViews[entries[0].token.kind]
	if !ok {
		builder.mu.Unlock()
		builder.Abandon()
		return stableResourceKindView{}, ErrUnresolvedResource
	}
	builder.kindViews = nil
	builder.closed = true
	builder.mu.Unlock()
	return view, nil
}

func cloneReachabilityUnion(left, right map[ReachabilityField]struct{}) map[ReachabilityField]struct{} {
	result := make(map[ReachabilityField]struct{}, len(left)+len(right))
	for field := range left {
		result[field] = struct{}{}
	}
	for field := range right {
		result[field] = struct{}{}
	}
	return result
}

func validateAppendOnlyProducerViews(views map[ResourceKind]stableResourceKindView, mutation StableLogicalObligationMutation) (StableLogicalObligationMutation, StableResourceClosureWork, error) {
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
	scoped := make(map[ReachabilityField]struct{}, len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped[field] = struct{}{}
	}
	seen := make(map[StableLogicalObligation]struct{}, len(desired))
	var validateErr error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		work.SourceEntriesInspected++
		for field := range entry.reachability {
			if _, applies := scoped[field]; applies && entry.logicalObligations.commitments[field].count == 0 {
				validateErr = fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
				return false
			}
		}
		entry.logicalObligations.rangeValues(func(obligation StableLogicalObligation) bool {
			work.SourceObligationsInspected++
			if _, applies := scoped[obligation.Reachability]; !applies {
				validateErr = fmt.Errorf("%w: append-only producer obligation uses unscoped field %q", ErrResourceConflict, obligation.Reachability)
				return false
			}
			if _, ok := desired[obligation]; !ok {
				validateErr = fmt.Errorf("%w: append-only producer supplied unannounced logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			if _, duplicate := seen[obligation]; duplicate {
				validateErr = fmt.Errorf("%w: append-only producer repeated logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			seen[obligation] = struct{}{}
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
	scoped := make(map[ReachabilityField]struct{}, len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped[field] = struct{}{}
	}

	builder.mu.Lock()
	if builder.closed || builder.abandoned {
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	child.mu.Lock()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		child.mu.Unlock()
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	work.SourceEntriesInspected += uint64(len(builder.entries) + len(child.entries))
	work.CopiedEntries += uint64(len(builder.entries))
	work.RetainedEntries += uint64(len(builder.entries))
	for _, entry := range builder.entries {
		work.RetainedObligations += uint64(entry.logicalObligations.count)
	}
	seen := make(map[StableLogicalObligation]struct{}, len(desired))
	for _, entry := range child.entries {
		for field := range entry.reachability {
			if _, applies := scoped[field]; !applies {
				continue
			}
			if entry.logicalObligations.commitments[field].count == 0 {
				child.mu.Unlock()
				builder.mu.Unlock()
				return work, fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
			}
		}
		entry.logicalObligations.rangeValues(func(obligation StableLogicalObligation) bool {
			work.SourceObligationsInspected++
			if _, applies := scoped[obligation.Reachability]; !applies {
				err = fmt.Errorf("%w: append-only producer obligation uses unscoped field %q", ErrResourceConflict, obligation.Reachability)
				return false
			}
			if _, ok := desired[obligation]; !ok {
				err = fmt.Errorf("%w: append-only producer supplied unannounced logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			if _, duplicate := seen[obligation]; duplicate {
				err = fmt.Errorf("%w: append-only producer repeated logical obligation %+v", ErrResourceConflict, obligation)
				return false
			}
			seen[obligation] = struct{}{}
			return true
		})
		if err != nil {
			child.mu.Unlock()
			builder.mu.Unlock()
			return work, err
		}
	}
	if len(seen) != len(desired) {
		child.mu.Unlock()
		builder.mu.Unlock()
		return work, fmt.Errorf("%w: append-only producer admitted %d of %d declared logical obligations", ErrUnresolvedResource, len(seen), len(desired))
	}

	merged := cloneStableResourceEntries(builder.entries)
	lookup := stableResourceEntryLookup{}
	if len(merged)+len(child.entries) > stableResourceEntryLinearLookupLimit {
		lookup = newStableResourceEntryLookup(merged)
	}
	for _, incoming := range child.entries {
		if lookup.logical == nil {
			err = mergeAppendOnlyViewEntryLinear(&merged, incoming, &work)
			if err != nil {
				break
			}
			continue
		}
		logicalKey := incoming.token.logicalKey()
		coalesced := false
		work.PhysicalEntryLookupProbes++
		if existingIndex, ok := lookup.logical[logicalKey]; ok {
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
			incomingValues := incoming.logicalObligations.slice()
			entry.logicalObligations, err = entry.logicalObligations.appendCertified(incomingValues, &work)
			if err != nil {
				break
			}
			entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
			mergeStableResourceDescriptorIdentity(entry, incoming.logicalLane, incoming.resourceID, incoming.diagnosticPath)
			for field := range incoming.reachability {
				entry.reachability[field] = struct{}{}
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
			merged = append(merged, cloneStableResourceEntry(incoming))
			lookup.add(merged, len(merged)-1)
			work.PhysicalEntryLookupAdmissions++
		}
	}
	if err != nil {
		child.mu.Unlock()
		builder.mu.Unlock()
		return work, err
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		child.mu.Unlock()
		builder.mu.Unlock()
		return work, ErrResourceOwnership
	}
	dropped := droppedStableResourceTokens(merged, builder.entries, child.entries)
	builder.entries = merged
	if lookup.logical == nil {
		builder.indexed = nil
	} else {
		if builder.indexed == nil {
			builder.indexed = &stableResourceBuilderIndexedState{}
		}
		builder.indexed.lookup = lookup
	}
	child.entries = nil
	child.mu.Unlock()
	builder.mu.Unlock()
	for _, token := range dropped {
		token.releaseFrom(ResourceOwnerBuilder)
	}
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
	if builder.closed || builder.abandoned {
		return nil, ErrResourceOwnership
	}
	covered := make(map[ReachabilityField]struct{})
	if builder.kindViews != nil {
		for _, view := range builder.kindViews {
			for field := range view.reachability {
				covered[field] = struct{}{}
			}
		}
	}
	for _, entry := range builder.entries {
		if err := entry.token.namespace.validateStable(); err != nil {
			return nil, err
		}
		for field := range entry.reachability {
			covered[field] = struct{}{}
		}
	}
	for required := range builder.required {
		if _, ok := covered[required]; !ok {
			return nil, fmt.Errorf("%w: missing reachability field %q", ErrUnresolvedResource, required)
		}
	}
	if builder.kindViews != nil {
		views := builder.kindViews
		set := &StableResourceSet{
			kindViews: views, createdAt: time.Now(),
			pinHighWater: stableResourcePinCountsFromViews(views),
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
		entries: entries, kindViews: views, createdAt: time.Now(),
		pinHighWater: stableResourcePinCounts(entries),
	}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	builder.entries = nil
	builder.closed = true
	return set, nil
}

func (builder *StableResourceSetBuilder) Abandon() {
	if builder == nil {
		return
	}
	builder.mu.Lock()
	if builder.closed || builder.abandoned {
		builder.mu.Unlock()
		return
	}
	entries := builder.entries
	views := builder.kindViews
	builder.entries = nil
	builder.kindViews = nil
	builder.abandoned = true
	builder.state = ResourceOwnerReleased
	builder.mu.Unlock()
	releaseStableResourceKindViews(views)
	for _, entry := range entries {
		entry.token.releaseFrom(ResourceOwnerBuilder)
	}
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

type StableResourceSet struct {
	mu           sync.Mutex
	entries      []stableResourceEntry
	kindViews    map[ResourceKind]stableResourceKindView
	pinHighWater map[ResourceKind]uint64
	owner        atomic.Uint32
	createdAt    time.Time
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
	if source == nil {
		return nil, true, nil
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		if kind != "" {
			excludedKinds[kind] = struct{}{}
		}
	}
	source.mu.Lock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		source.mu.Unlock()
		return nil, false, ErrResourceOwnership
	}
	if source.kindViews == nil {
		source.mu.Unlock()
		return nil, false, nil
	}
	views, ok := cloneStableResourceKindViews(source.kindViews, excludedKinds)
	createdAt := source.createdAt
	source.mu.Unlock()
	if !ok {
		return nil, false, ErrResourceOwnership
	}
	set := &StableResourceSet{
		kindViews: views, createdAt: createdAt,
		pinHighWater: stableResourcePinCountsFromViews(views),
	}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set, true, nil
}

// StableLogicalObligationRequirements scopes an exact logical-reference
// closure for a later root publication. ScopedFields distinguishes "this
// publication has no references for this field" from "this publication did
// not supply requirements for this field". Obligations must belong to one of
// the scoped fields.
type StableLogicalObligationRequirements struct {
	ScopedFields []ReachabilityField
	Obligations  []StableLogicalObligation
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
	requirementFields := make(map[ReachabilityField]struct{}, len(requirements.ScopedFields))
	for _, field := range requirements.ScopedFields {
		requirementFields[field] = struct{}{}
	}
	for _, field := range normalized.ScopedFields {
		if _, ok := requirementFields[field]; !ok {
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
		if !contains(obligation) {
			return fmt.Errorf("%w: added mutation obligation absent from final requirements %+v", ErrUnresolvedResource, obligation)
		}
	}
	for _, obligation := range normalized.Removed {
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
	normalizedMutation, err := NormalizeStableLogicalObligationMutation(mutation)
	if err != nil {
		return false, err
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
// retained obligation history. A false result requires the caller's exact
// requirements fallback before either set changes ownership.
func CertifyStableLogicalObligationAppendMutation(source, producer *StableResourceSet, mutation StableLogicalObligationMutation, excluded ...ResourceKind) (StableResourceClosureWork, bool, error) {
	var normalized StableLogicalObligationMutation
	var work StableResourceClosureWork
	var err error
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
		producer.mu.Unlock()
	}
	if err != nil || len(normalized.ScopedFields) == 0 || len(normalized.Removed) != 0 {
		return work, false, nil
	}
	baseCommitments, complete, err := stableResourceSetLogicalObligationCommitments(source, normalized.ScopedFields, excluded...)
	if err != nil || !complete {
		return work, false, err
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

func stableResourceSetLogicalObligationCommitments(source *StableResourceSet, fields []ReachabilityField, excluded ...ResourceKind) (map[ReachabilityField]stableLogicalObligationCommitment, bool, error) {
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
		for kind, view := range source.kindViews {
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
	if len(uniqueFields) == 0 {
		if len(requirements.Obligations) != 0 {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: logical obligations have no scoped fields", ErrUnresolvedResource)
		}
		return StableLogicalObligationRequirements{}, nil
	}
	scoped := make(map[ReachabilityField]struct{}, len(uniqueFields))
	byField := make(map[ReachabilityField][]StableLogicalObligation, len(uniqueFields))
	for _, field := range uniqueFields {
		scoped[field] = struct{}{}
	}
	for _, obligation := range requirements.Obligations {
		if _, ok := scoped[obligation.Reachability]; !ok {
			return StableLogicalObligationRequirements{}, fmt.Errorf("%w: logical obligation field %q is not scoped", ErrResourceConflict, obligation.Reachability)
		}
		byField[obligation.Reachability] = append(byField[obligation.Reachability], obligation)
	}
	normalized := make([]StableLogicalObligation, 0, len(requirements.Obligations))
	for _, field := range uniqueFields {
		obligations, err := normalizeStableLogicalObligations(byField[field], field)
		if err != nil {
			return StableLogicalObligationRequirements{}, err
		}
		normalized = append(normalized, obligations...)
	}
	return StableLogicalObligationRequirements{
		ScopedFields: append([]ReachabilityField(nil), uniqueFields...),
		Obligations:  append([]StableLogicalObligation(nil), normalized...),
		commitments:  stableLogicalObligationRequirementCommitments(uniqueFields, normalized),
	}, nil
}

// MergeStableLogicalObligationRequirements forms the exact union of two
// independently supplied requirement sets.
func MergeStableLogicalObligationRequirements(left, right StableLogicalObligationRequirements) (StableLogicalObligationRequirements, error) {
	return NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: append(append([]ReachabilityField(nil), left.ScopedFields...), right.ScopedFields...),
		Obligations:  append(append([]StableLogicalObligation(nil), left.Obligations...), right.Obligations...),
	})
}

type stableLogicalObligationRequirementIndex struct {
	scoped  map[ReachabilityField]struct{}
	desired map[ReachabilityField]map[StableLogicalObligation]struct{}
}

func indexStableLogicalObligationRequirements(requirements StableLogicalObligationRequirements) (stableLogicalObligationRequirementIndex, error) {
	normalized, err := NormalizeStableLogicalObligationRequirements(requirements)
	if err != nil {
		return stableLogicalObligationRequirementIndex{}, err
	}
	index := stableLogicalObligationRequirementIndex{
		scoped:  make(map[ReachabilityField]struct{}, len(normalized.ScopedFields)),
		desired: make(map[ReachabilityField]map[StableLogicalObligation]struct{}, len(normalized.ScopedFields)),
	}
	for _, field := range normalized.ScopedFields {
		index.scoped[field] = struct{}{}
		index.desired[field] = make(map[StableLogicalObligation]struct{})
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

type StableResourcePhysicalDescriptor struct {
	Kind       ResourceKind
	Generation uint64
}

// PhysicalDescriptors returns only fields needed for dependency routing. It
// intentionally does not materialize logical obligations.
func (set *StableResourceSet) PhysicalDescriptors() []StableResourcePhysicalDescriptor {
	if set == nil {
		return nil
	}
	result := make([]StableResourcePhysicalDescriptor, 0, set.Len())
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		result = append(result, StableResourcePhysicalDescriptor{Kind: entry.token.kind, Generation: entry.token.generation})
		return true
	})
	return result
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

func stableResourcePinCountsFromViews(views map[ResourceKind]stableResourceKindView) map[ResourceKind]uint64 {
	counts := make(map[ResourceKind]uint64, len(views))
	for kind, view := range views {
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

func (set *StableResourceSet) Tokens() []*StableResourceToken {
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

// Descriptors returns immutable-by-copy views of the coalesced physical
// obligations. Callers that need publication or recovery metadata should use
// these views rather than representative Tokens.
func (set *StableResourceSet) Descriptors() []StableResourceDescriptor {
	if set == nil {
		return nil
	}
	descriptors := make([]StableResourceDescriptor, 0, set.Len())
	set.rangeEntries(func(entry *stableResourceEntry) bool {
		fields := make([]ReachabilityField, 0, len(entry.reachability))
		for field := range entry.reachability {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		logicalObligations := entry.logicalObligations.slice()
		sort.Slice(logicalObligations, func(i, j int) bool {
			return stableLogicalObligationLess(logicalObligations[i], logicalObligations[j])
		})
		descriptor := StableResourceDescriptor{
			kind: entry.token.kind, logicalLane: entry.logicalLane, resourceID: entry.resourceID,
			diagnosticPath: entry.diagnosticPath, identity: entry.token.identity, generation: entry.token.generation,
			digest: entry.token.digest, frontier: cloneDurableFrontier(entry.frontier), reachability: fields,
			logicalObligations: logicalObligations,
		}
		if namespace := entry.token.namespace; namespace != nil {
			descriptor.namespace = &StableNamespaceDescriptor{
				ParentIdentity: namespace.parentIdentity,
				Operation:      namespace.operation,
				OldName:        namespace.oldName,
				NewName:        namespace.newName,
				DiagnosticPath: namespace.diagnosticPath,
			}
		}
		descriptors = append(descriptors, descriptor)
		return true
	})
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
	return descriptors
}

// DependencyManifestV1 builds the unchanged durable V1 stream while reusing
// canonical encodings for immutable retained entries. A coalesced entry clears
// only its own cache before a later Freeze.
func (set *StableResourceSet) DependencyManifestV1() (*DependencyManifestV1, DependencyManifestBuildWorkV1, error) {
	if set == nil {
		return NewDependencyManifestV1WithWork(nil)
	}
	work := DependencyManifestBuildWorkV1{}
	encoded := make([]dependencyManifestEncodedEntryV1, 0, set.Len())
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
		encoded = append(encoded, *cache.value)
		cache.mu.Unlock()
		return true
	})
	if buildErr != nil {
		return nil, work, buildErr
	}
	manifest, err := newDependencyManifestV1FromEncoded(encoded)
	return manifest, work, err
}

func dependencyManifestEntryV1FromStableResourceEntry(entry stableResourceEntry) DependencyManifestEntryV1 {
	fields := make([]ReachabilityField, 0, len(entry.reachability))
	for field := range entry.reachability {
		fields = append(fields, field)
	}
	logicalObligations := entry.logicalObligations.slice()
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
	work := StableResourceClosureWork{CloneOperations: 1}
	if source == nil {
		return nil, work, nil
	}
	if len(requirements.ScopedFields) == 0 && len(requirements.Obligations) == 0 {
		cloned, shared, err := cloneStableResourceSetKindView(source, excluded...)
		if err != nil {
			return nil, work, err
		}
		if shared {
			work.PhysicalRootShares = uint64(len(cloned.kindViews))
			return cloned, work, nil
		}
	}
	work.RequirementFieldsInspected = uint64(len(requirements.ScopedFields))
	work.RequirementObligationsInspected = uint64(len(requirements.Obligations))
	requirementIndex, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return nil, work, err
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
		fields := make([]ReachabilityField, 0, len(entry.reachability))
		allFieldsUnscoped := true
		for field := range entry.reachability {
			fields = append(fields, field)
			if _, scoped := requirementIndex.scoped[field]; scoped {
				allFieldsUnscoped = false
			}
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		for _, field := range fields {
			_, scoped := requirementIndex.scoped[field]
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
				entry.logicalObligations.rangeValues(func(obligation StableLogicalObligation) bool {
					work.SourceObligationsInspected++
					if obligation.Reachability != field {
						return true
					}
					if scoped {
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
			var namespace *StableNamespaceToken
			if allFieldsUnscoped {
				if err := token.namespace.validateStable(); err != nil {
					return nil, work, err
				}
				identity := token.identity
				var registry *IdentityPinRegistry
				if token.identityPin != nil {
					registry = token.identityPin.registry
					if err := registry.Observe(identity); err != nil {
						return nil, work, err
					}
				}
				cloned, err = token.cloneSharedPinned(
					entry.logicalLane, entry.resourceID, entry.diagnosticPath,
					entry.frontier, field, obligations, func() {
						if registry != nil {
							_ = registry.Unobserve(identity)
						}
					})
				if err != nil {
					if registry != nil {
						_ = registry.Unobserve(identity)
					}
					return nil, work, err
				}
				work.PhysicalHandleShares++
			} else {
				work.PhysicalHandleCopies++
				namespace, err = token.namespace.cloneStable()
				if err != nil {
					return nil, work, err
				}
				var registry *IdentityPinRegistry
				if token.identityPin != nil {
					registry = token.identityPin.registry
					if err := registry.Observe(token.identity); err != nil {
						namespace.Release()
						return nil, work, err
					}
				}
				observed := registry != nil
				err = token.WithPinnedFile(func(file *os.File) error {
					var constructErr error
					cloned, constructErr = newStableResourceToken(StableResourceSpec{
						Kind: token.kind, LogicalLane: entry.logicalLane, ResourceID: entry.resourceID,
						Generation: token.generation, DiagnosticPath: entry.diagnosticPath,
						File: file, Frontier: cloneDurableFrontier(entry.frontier), Digest: token.digest,
						Reachability: field, Namespace: namespace, LogicalObligations: obligations,
						ContentSynced: true, PinRegistry: registry,
						StableIdentityOverride: token.identity,
						OnRelease: func() {
							if registry != nil {
								_ = registry.Unobserve(token.identity)
							}
						},
					}, obligations)
					return constructErr
				})
				if err != nil {
					if observed {
						_ = registry.Unobserve(token.identity)
					}
					namespace.Release()
					return nil, work, err
				}
			}
			if err := builder.Add(cloned); err != nil {
				cloned.Release()
				namespace.Release()
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
				destination.reachability = make(map[ReachabilityField]struct{}, len(entry.reachability))
				for retainedField := range entry.reachability {
					destination.reachability[retainedField] = struct{}{}
				}
			}
			namespace.Release()
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
	if len(normalized.Removed) == 0 {
		cloned, shared, cloneErr := cloneStableResourceSetKindView(source, excluded...)
		if cloneErr != nil {
			return nil, work, cloneErr
		}
		if shared {
			work.CloneOperations = 1
			if cloned != nil {
				work.PhysicalRootShares = uint64(len(cloned.kindViews))
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
	scoped := make(map[ReachabilityField]struct{}, len(normalized.ScopedFields))
	for _, field := range normalized.ScopedFields {
		scoped[field] = struct{}{}
	}
	found := make(map[StableLogicalObligation]struct{}, len(removed))
	if source != nil {
		for _, descriptor := range source.Descriptors() {
			for _, obligation := range descriptor.LogicalObligations() {
				if _, ok := scoped[obligation.Reachability]; !ok {
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
	work := StableResourceClosureWork{
		RequirementFieldsInspected:      uint64(len(requirements.ScopedFields)),
		RequirementObligationsInspected: uint64(len(requirements.Obligations)),
		LogicalObligationNormalizations: uint64(len(requirements.Obligations)),
		FullClosureValidations:          1,
	}
	index, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return work, err
	}
	if len(index.scoped) == 0 {
		return work, nil
	}
	actual := make(map[ReachabilityField]map[StableLogicalObligation]struct{}, len(index.scoped))
	for field := range index.scoped {
		actual[field] = make(map[StableLogicalObligation]struct{})
	}
	if resources != nil {
		resources.mu.Lock()
		defer resources.mu.Unlock()
		for _, entry := range resources.entrySnapshotLocked() {
			work.SourceEntriesInspected++
			fields := make([]ReachabilityField, 0, len(entry.reachability))
			for field := range entry.reachability {
				fields = append(fields, field)
			}
			for _, field := range fields {
				if _, scoped := index.scoped[field]; !scoped {
					continue
				}
				foundForField := false
				var visitErr error
				entry.logicalObligations.rangeValues(func(obligation StableLogicalObligation) bool {
					work.SourceObligationsInspected++
					if obligation.Reachability != field {
						return true
					}
					foundForField = true
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
	if ResourceOwnerState(set.owner.Load()) != ResourceOwnerBuilder {
		return false
	}
	covered := false
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if _, ok := entry.reachability[field]; ok {
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
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if ResourceOwnerState(set.owner.Load()) == ResourceOwnerReleased {
		return ErrResourceOwnership
	}
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
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if ResourceOwnerState(set.owner.Load()) == ResourceOwnerReleased {
		return ErrResourceOwnership
	}
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

func (set *StableResourceSet) releaseFrom(owner ResourceOwnerState) {
	if set == nil {
		return
	}
	set.mu.Lock()
	if !set.owner.CompareAndSwap(uint32(owner), uint32(ResourceOwnerReleased)) {
		set.mu.Unlock()
		return
	}
	// entries is immutable after Freeze. Retain the diagnostic evidence after
	// release so concurrent readers never race a destructive slice update and
	// PinHighWater remains observable after ActivePins falls to zero.
	entries := append([]stableResourceEntry(nil), set.entries...)
	views := set.kindViews
	set.mu.Unlock()
	if views != nil {
		releaseStableResourceKindViews(views)
		return
	}
	for _, entry := range entries {
		entry.token.releaseFrom(owner)
	}
}

func (set *StableResourceSet) Release() {
	if set == nil {
		return
	}
	switch set.Owner() {
	case ResourceOwnerBuilder:
		set.releaseFrom(ResourceOwnerBuilder)
	case ResourceOwnerRecovery:
		set.releaseFrom(ResourceOwnerRecovery)
	}
}

func UnionStableResourceSets(sets ...*StableResourceSet) (*StableResourceSet, error) {
	view := &StableResourceSet{createdAt: time.Now()}
	view.owner.Store(uint32(ResourceOwnerView))
	lookup := stableResourceEntryLookup{}
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		var mergeErr error
		set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			var err error
			if lookup.logical == nil && len(view.entries) < stableResourceEntryLinearLookupLimit {
				err = mergeViewEntryLinear(&view.entries, *entry, true, nil)
			} else {
				if lookup.logical == nil {
					lookup = newStableResourceEntryLookup(view.entries)
				}
				err = mergeViewEntry(&view.entries, &lookup, *entry, true, nil)
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
	sortStableResourceEntries(view.entries)
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
	entries []stableResourceEntry
}

func (set *StableResourceSet) DeletionGuard() StableResourceDeletionGuard {
	if set == nil {
		return StableResourceDeletionGuard{}
	}
	set.mu.Lock()
	defer set.mu.Unlock()
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

func (set *StableResourceSet) Stats(now time.Time) []ResourceKindStats {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	byKind := make(map[ResourceKind]*ResourceKindStats)
	seenNamespaces := make(map[*StableNamespaceToken]struct{})
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil {
			return true
		}
		stats := byKind[token.kind]
		if stats == nil {
			stats = &ResourceKindStats{Kind: token.kind}
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
