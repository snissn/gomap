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
	token              *StableResourceToken
	pins               []*StableResourceToken
	pinIndex           map[*StableResourceToken]struct{}
	logicalLane        string
	resourceID         string
	diagnosticPath     string
	frontier           DurableFrontier
	reachability       map[ReachabilityField]struct{}
	logicalObligations stableLogicalObligationView
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
	for _, obligation := range values {
		var err error
		index, err = insertStableLogicalObligationIndex(index, obligation, work)
		if err != nil {
			return stableLogicalObligationView{}, err
		}
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
		return nil, fmt.Errorf("%w: append-only mutation repeats logical obligation %+v", ErrResourceConflict, key)
	}
	next := *root
	if work != nil {
		work.RetainedIndexNodeCopies++
	}
	if stableLogicalObligationIndexLess(key, root.key) {
		child, err := insertStableLogicalObligationIndex(root.left, obligation, work)
		if err != nil {
			return nil, err
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
	CloneOperations                 uint64
	FreezeOperations                uint64
	RequirementFieldsInspected      uint64
	RequirementObligationsInspected uint64
	SourceEntriesInspected          uint64
	SourceObligationsInspected      uint64
	RetainedEntries                 uint64
	RetainedObligations             uint64
	DroppedEntries                  uint64
	DroppedObligations              uint64
	CopiedEntries                   uint64
	CopiedObligations               uint64
	PhysicalHandleCopies            uint64
	LogicalObligationNormalizations uint64
	RetainedIndexNodeVisits         uint64
	RetainedIndexNodeCopies         uint64
	LogicalIndexNodesAdmitted       uint64
	NewlyAdmittedEntries            uint64
	NewlyAdmittedObligations        uint64
	RemovedObligations              uint64
	AppendOnlyFastPath              uint64
	AppendOnlyFallbacks             uint64
	DestructiveFallbacks            uint64
	FullClosureValidations          uint64
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
	work.LogicalObligationNormalizations += other.LogicalObligationNormalizations
	work.RetainedIndexNodeVisits += other.RetainedIndexNodeVisits
	work.RetainedIndexNodeCopies += other.RetainedIndexNodeCopies
	work.LogicalIndexNodesAdmitted += other.LogicalIndexNodesAdmitted
	work.NewlyAdmittedEntries += other.NewlyAdmittedEntries
	work.NewlyAdmittedObligations += other.NewlyAdmittedObligations
	work.RemovedObligations += other.RemovedObligations
	work.AppendOnlyFastPath += other.AppendOnlyFastPath
	work.AppendOnlyFallbacks += other.AppendOnlyFallbacks
	work.DestructiveFallbacks += other.DestructiveFallbacks
	work.FullClosureValidations += other.FullClosureValidations
	work.PhysicalEntryLookupProbes += other.PhysicalEntryLookupProbes
	work.PhysicalEntryLookupComparisons += other.PhysicalEntryLookupComparisons
	work.PhysicalEntryLookupAdmissions += other.PhysicalEntryLookupAdmissions
}

// stableResourceEntryLookup is transient builder state. The frozen set remains
// an ordered slice so deterministic publication and ownership behavior stay
// unchanged.
type stableResourceEntryLookup struct {
	logical            map[stableLogicalResourceKey]int
	physical           map[stablePhysicalIdentityKey]int
	physicalCollisions map[stablePhysicalIdentityKey][]int
}

const stableResourceEntryLinearLookupLimit = 16

func newStableResourceEntryLookup(entries []stableResourceEntry) stableResourceEntryLookup {
	lookup := stableResourceEntryLookup{
		logical:  make(map[stableLogicalResourceKey]int, len(entries)),
		physical: make(map[stablePhysicalIdentityKey]int, len(entries)),
	}
	for i := range entries {
		lookup.add(entries[i], i)
	}
	return lookup
}

func (lookup *stableResourceEntryLookup) add(entry stableResourceEntry, index int) {
	lookup.logical[entry.token.logicalKey()] = index
	key := entry.token.physicalIdentityKey()
	position := index + 1
	if existing := lookup.physical[key]; existing == 0 {
		lookup.physical[key] = position
		return
	} else if existing > 0 {
		if lookup.physicalCollisions == nil {
			lookup.physicalCollisions = make(map[stablePhysicalIdentityKey][]int)
		}
		lookup.physicalCollisions[key] = []int{existing, position}
		lookup.physical[key] = -1
		return
	}
	lookup.physicalCollisions[key] = append(lookup.physicalCollisions[key], position)
}

type stableResourceEntryLookupIterator struct {
	single     int
	collisions []int
	next       int
}

func (lookup *stableResourceEntryLookup) physicalIterator(token *StableResourceToken) stableResourceEntryLookupIterator {
	position := lookup.physical[token.physicalIdentityKey()]
	if position > 0 {
		return stableResourceEntryLookupIterator{single: position}
	}
	return stableResourceEntryLookupIterator{collisions: lookup.physicalCollisions[token.physicalIdentityKey()]}
}

func (iterator *stableResourceEntryLookupIterator) Next() (int, bool) {
	if iterator.single != 0 {
		position := iterator.single
		iterator.single = 0
		return position - 1, true
	}
	if iterator.next == len(iterator.collisions) {
		return 0, false
	}
	position := iterator.collisions[iterator.next]
	iterator.next++
	return position - 1, true
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
		reachability:       map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations: newStableLogicalObligationView(token.logicalObligations),
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
	for iterator := lookup.physicalIterator(token); ; {
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
		reachability:       map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations: newStableLogicalObligationView(token.logicalObligations),
	})
	lookup.add((*entries)[len(*entries)-1], len(*entries)-1)
	work.PhysicalEntryLookupAdmissions++
	return nil
}

func mergeStableResourceDescriptorIdentity(entry *stableResourceEntry, lane, resourceID, diagnosticPath string) {
	if entry == nil {
		return
	}
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
	for iterator := lookup.physicalIterator(incoming.token); ; {
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
	lookup.add((*entries)[len(*entries)-1], len(*entries)-1)
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

// Merge consumes a child builder-owned set only after the complete transitive
// union has passed conflict checks. This is the one-way child-to-parent
// transfer used before a parent installs a child root or catalog ID.
func (builder *StableResourceSetBuilder) Merge(child *StableResourceSet) error {
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
// removal-free mutation evidence. Existing physical entries retain their
// immutable obligation prefix and append only the newly admitted producer
// obligations. Any repeated, conflicting, missing, or out-of-scope producer
// obligation fails closed before ownership transfer.
func (builder *StableResourceSetBuilder) MergeAppendOnlyLogicalObligations(child *StableResourceSet, mutation StableLogicalObligationMutation) (StableResourceClosureWork, error) {
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
		for iterator := lookup.physicalIterator(incoming.token); ; {
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
			lookup.add(merged[len(merged)-1], len(merged)-1)
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
	entries := cloneStableResourceEntries(builder.entries)
	sortStableResourceEntries(entries)
	set := &StableResourceSet{
		entries: entries, createdAt: time.Now(),
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
	builder.entries = nil
	builder.abandoned = true
	builder.state = ResourceOwnerReleased
	builder.mu.Unlock()
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
	pinHighWater map[ResourceKind]uint64
	owner        atomic.Uint32
	createdAt    time.Time
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
		for _, entry := range source.entries {
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
	set.mu.Lock()
	defer set.mu.Unlock()
	result := make([]StableResourcePhysicalDescriptor, len(set.entries))
	for i, entry := range set.entries {
		result[i] = StableResourcePhysicalDescriptor{Kind: entry.token.kind, Generation: entry.token.generation}
	}
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

func (set *StableResourceSet) Len() int {
	if set == nil {
		return 0
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return len(set.entries)
}

func (set *StableResourceSet) Tokens() []*StableResourceToken {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	tokens := make([]*StableResourceToken, len(set.entries))
	for i, entry := range set.entries {
		tokens[i] = activeEntryToken(entry)
	}
	return tokens
}

// IdentityPinRegistryStats reports the exact DB-scoped registries retained by
// this closure's physical pins. Registries are de-duplicated, including when
// several logical obligations coalesce onto one physical resource.
func (set *StableResourceSet) IdentityPinRegistryStats() []IdentityPinRegistryStats {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	registries := make(map[*IdentityPinRegistry]struct{})
	for _, entry := range set.entries {
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
	}
	set.mu.Unlock()

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
	set.mu.Lock()
	defer set.mu.Unlock()
	descriptors := make([]StableResourceDescriptor, len(set.entries))
	for i, entry := range set.entries {
		fields := make([]ReachabilityField, 0, len(entry.reachability))
		for field := range entry.reachability {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		logicalObligations := entry.logicalObligations.slice()
		sort.Slice(logicalObligations, func(i, j int) bool {
			return stableLogicalObligationLess(logicalObligations[i], logicalObligations[j])
		})
		descriptors[i] = StableResourceDescriptor{
			kind: entry.token.kind, logicalLane: entry.logicalLane, resourceID: entry.resourceID,
			diagnosticPath: entry.diagnosticPath, identity: entry.token.identity, generation: entry.token.generation,
			digest: entry.token.digest, frontier: cloneDurableFrontier(entry.frontier), reachability: fields,
			logicalObligations: logicalObligations,
		}
		if namespace := entry.token.namespace; namespace != nil {
			descriptors[i].namespace = &StableNamespaceDescriptor{
				ParentIdentity: namespace.parentIdentity,
				Operation:      namespace.operation,
				OldName:        namespace.oldName,
				NewName:        namespace.newName,
				DiagnosticPath: namespace.diagnosticPath,
			}
		}
	}
	return descriptors
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
	for _, entry := range source.entries {
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
				// shared while one independently duplicated handle preserves the
				// candidate's ownership. Multi-field entries must not re-walk the
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
			work.PhysicalHandleCopies++
			namespace, err := token.namespace.cloneStable()
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
			var cloned *StableResourceToken
			err = token.WithPinnedFile(func(file *os.File) error {
				var constructErr error
				cloned, constructErr = newStableResourceToken(StableResourceSpec{
					Kind: token.kind, LogicalLane: entry.logicalLane, ResourceID: entry.resourceID,
					Generation: token.generation, DiagnosticPath: entry.diagnosticPath,
					File: file, Frontier: cloneDurableFrontier(entry.frontier), Digest: token.digest,
					Reachability: field, Namespace: namespace, LogicalObligations: obligations,
					ContentSynced: true, PinRegistry: registry,
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
		for _, entry := range resources.entries {
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
	for _, entry := range set.entries {
		if _, ok := entry.reachability[field]; ok {
			return true
		}
	}
	return false
}

func (set *StableResourceSet) FrontierFor(identity StableIdentity, generation uint64) DurableFrontier {
	if set == nil {
		return DurableFrontier{}
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, entry := range set.entries {
		if sameStableObject(entry.token.identity, identity) && entry.token.generation == generation {
			return cloneDurableFrontier(entry.frontier)
		}
	}
	return DurableFrontier{}
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
	for _, entry := range set.entries {
		token := activeEntryToken(entry)
		if token == nil {
			errs = append(errs, ErrResourceOwnership)
			continue
		}
		if err := token.flushThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("flush stable resource %+v: %w", token.logicalKey(), err))
		}
	}
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
	for _, entry := range set.entries {
		token := activeEntryToken(entry)
		if token == nil {
			errs = append(errs, ErrResourceOwnership)
			continue
		}
		if err := token.syncThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("sync stable resource %+v: %w", token.logicalKey(), err))
		}
	}
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
	set.mu.Unlock()
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
		for _, entry := range set.entries {
			var err error
			if lookup.logical == nil && len(view.entries) < stableResourceEntryLinearLookupLimit {
				err = mergeViewEntryLinear(&view.entries, entry, true, nil)
			} else {
				if lookup.logical == nil {
					lookup = newStableResourceEntryLookup(view.entries)
				}
				err = mergeViewEntry(&view.entries, &lookup, entry, true, nil)
			}
			if err != nil {
				set.mu.Unlock()
				return nil, err
			}
		}
		set.mu.Unlock()
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
	return StableResourceDeletionGuard{entries: cloneStableResourceEntries(set.entries)}
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
	for _, entry := range set.entries {
		token := activeEntryToken(entry)
		if token == nil {
			continue
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
	}
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
	for _, entry := range set.entries {
		token := activeEntryToken(entry)
		if token == nil {
			continue
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
			continue
		}
		if add {
			counts[token.kind] = saturatingAdd(counts[token.kind], 1)
			continue
		}
		if counts[token.kind] <= 1 {
			delete(counts, token.kind)
		} else {
			counts[token.kind]--
		}
	}
}

func (set *StableResourceSet) validateResolved() error {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, entry := range set.entries {
		if err := entry.token.namespace.validateStable(); err != nil {
			return err
		}
		stat, err := entry.token.pinned.Stat()
		if err != nil {
			return err
		}
		if entry.frontier.Bytes > uint64(stat.Size()) {
			return ErrFrontierBeyondResource
		}
	}
	return nil
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
