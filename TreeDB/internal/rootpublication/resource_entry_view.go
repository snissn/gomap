package rootpublication

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
)

// stableResourceEntryNode is a persistent rope over immutable frozen entry
// chunks. A set owns one reference to each kind root; concatenation consumes
// two owned roots without copying their retained entries or physical handles.
type stableResourceEntryNode struct {
	refs                        atomic.Int64
	cleanupMu                   sync.Mutex
	cleanupRunning, cleanupDone bool
	cleanupLeft, cleanupRight   bool
	cleanupEntry                int
	left, right                 *stableResourceEntryNode
	entries                     []stableResourceEntry
}

func newStableResourceEntryLeaf(entries []stableResourceEntry) *stableResourceEntryNode {
	if len(entries) == 0 {
		return nil
	}
	node := &stableResourceEntryNode{entries: entries[:len(entries):len(entries)]}
	node.refs.Store(1)
	return node
}

func concatOwnedStableResourceEntryNodes(left, right *stableResourceEntryNode) *stableResourceEntryNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	node := &stableResourceEntryNode{left: left, right: right}
	node.refs.Store(1)
	return node
}

func (node *stableResourceEntryNode) retain() bool {
	if node == nil {
		return false
	}
	for refs := node.refs.Load(); refs > 0; refs = node.refs.Load() {
		if node.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
	return false
}

func (node *stableResourceEntryNode) release() { _, _ = node.releaseCheckedV1(false) }

// resumed is holder-local: a failed last release never decrements refs twice.
func (node *stableResourceEntryNode) releaseCheckedV1(resumed bool) (done bool, result error) {
	if node == nil {
		return true, nil
	}
	if !resumed {
		last := false
		for refs := node.refs.Load(); refs > 0; refs = node.refs.Load() {
			if node.refs.CompareAndSwap(refs, refs-1) {
				last = refs == 1
				break
			}
		}
		if !last {
			return true, nil
		}
	}
	node.cleanupMu.Lock()
	if node.cleanupDone {
		node.cleanupMu.Unlock()
		return true, nil
	}
	if node.cleanupRunning {
		node.cleanupMu.Unlock()
		return false, ErrStableResourceOperationBusy
	}
	node.cleanupRunning = true
	node.cleanupMu.Unlock()
	defer func() {
		node.cleanupMu.Lock()
		node.cleanupRunning = false
		if done {
			node.cleanupDone = true
		}
		node.cleanupMu.Unlock()
	}()
	if node.left != nil || node.right != nil {
		if !node.cleanupLeft {
			node.cleanupLeft = true
			var childDone bool
			var childErr error
			childDone, childErr = node.left.releaseCheckedV1(false)
			result = errors.Join(result, childErr)
			if !childDone {
				return false, result
			}
		}
		if node.left != nil && !node.left.cleanupCompleteV1() {
			var childDone bool
			var childErr error
			childDone, childErr = node.left.releaseCheckedV1(true)
			result = errors.Join(result, childErr)
			if !childDone {
				return false, result
			}
		}
		if !node.cleanupRight {
			node.cleanupRight = true
			var childDone bool
			var childErr error
			childDone, childErr = node.right.releaseCheckedV1(false)
			result = errors.Join(result, childErr)
			if !childDone {
				return false, result
			}
		}
		if node.right != nil && !node.right.cleanupCompleteV1() {
			var childDone bool
			var childErr error
			childDone, childErr = node.right.releaseCheckedV1(true)
			result = errors.Join(result, childErr)
			if !childDone {
				return false, result
			}
		}
		return true, result
	}
	for node.cleanupEntry < len(node.entries) {
		token := node.entries[node.cleanupEntry].token
		err := token.releaseFrom(ResourceOwnerShared)
		result = errors.Join(result, err)
		if !token.CleanupCompleteV1() {
			return false, result
		}
		node.cleanupEntry++
	}
	return true, result
}
func (node *stableResourceEntryNode) cleanupCompleteV1() bool {
	if node == nil {
		return true
	}
	node.cleanupMu.Lock()
	defer node.cleanupMu.Unlock()
	return node.cleanupDone || node.refs.Load() > 0
}

func (node *stableResourceEntryNode) rangeEntries(visit func(*stableResourceEntry) bool) bool {
	stack := []*stableResourceEntryNode{node}
	for len(stack) != 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == nil {
			continue
		}
		if current.left != nil || current.right != nil {
			stack = append(stack, current.right, current.left)
			continue
		}
		for i := range current.entries {
			if !visit(&current.entries[i]) {
				return false
			}
		}
	}
	return true
}

type stableResourceLogicalIndexNode struct {
	key         stableLogicalResourceKey
	entry       *stableResourceEntry
	priority    uint64
	left, right *stableResourceLogicalIndexNode
}

type stableResourcePhysicalIndexNode struct {
	key         stablePhysicalIdentityKey
	entries     []*stableResourceEntry
	priority    uint64
	left, right *stableResourcePhysicalIndexNode
}

type stableResourceKindView struct {
	root                   *stableResourceEntryNode
	logical                *stableResourceLogicalIndexNode
	physical               *stableResourcePhysicalIndexNode
	logicalMembership      *stableLogicalObligationIndexNode
	directory              *DependencyDirectoryV2
	reachability           stableReachabilitySet
	logicalCommitments     map[ReachabilityField]stableLogicalObligationCommitment
	logicalObligationCount int
	logicalMembershipCount int
	count                  int
}

// stableLogicalMembershipEvidence is read-only semantic evidence. Its index
// nodes contain obligation values only, so sharing it neither retains nor owns
// physical resource roots.
type stableLogicalMembershipEvidence struct {
	root                   *stableLogicalObligationIndexNode
	directory              *DependencyDirectoryV2
	commitments            map[ReachabilityField]stableLogicalObligationCommitment
	logicalMembershipCount int
	logicalObligationCount int
}

func stableLogicalMembershipEvidenceComplete(evidence stableLogicalMembershipEvidence) bool {
	// Flat union evidence cannot borrow a directory without its owner index.
	if evidence.directory != nil {
		return false
	}
	return evidence.logicalMembershipCount == evidence.logicalObligationCount && (evidence.directory != nil || evidence.root != nil || evidence.logicalMembershipCount == 0)
}

func stableLogicalMembershipEvidenceFromKindView(view stableResourceKindView) stableLogicalMembershipEvidence {
	return stableLogicalMembershipEvidence{
		root:                   view.logicalMembership,
		directory:              view.directory,
		commitments:            view.logicalCommitments,
		logicalMembershipCount: view.logicalMembershipCount,
		logicalObligationCount: view.logicalObligationCount,
	}
}

func insertFreshStableLogicalMembership(root *stableLogicalObligationIndexNode, obligation StableLogicalObligation) (*stableLogicalObligationIndexNode, bool) {
	if _, exists := findStableLogicalObligationIndex(root, obligation, nil); exists {
		return root, false
	}
	next, err := insertFreshStableLogicalObligationIndex(root, obligation, nil)
	if err != nil {
		return root, false
	}
	return next, true
}

func insertStableLogicalMembership(root *stableLogicalObligationIndexNode, obligation StableLogicalObligation, work *StableResourceClosureWork) (*stableLogicalObligationIndexNode, bool) {
	var indexWork StableResourceClosureWork
	next, err := insertStableLogicalObligationIndex(root, obligation, &indexWork)
	if work != nil {
		work.AggregateMembershipProbes++
		work.AggregateMembershipNodeVisits += indexWork.RetainedIndexNodeVisits
		work.AggregateMembershipNodeCopies += indexWork.RetainedIndexNodeCopies
	}
	if err != nil || next == root {
		return root, false
	}
	if work != nil {
		work.AggregateMembershipAdmissions++
	}
	return next, true
}

func stableLogicalResourceKeyLess(left, right stableLogicalResourceKey) bool {
	if left.kind != right.kind {
		return left.kind < right.kind
	}
	if left.lane != right.lane {
		return left.lane < right.lane
	}
	if left.resourceID != right.resourceID {
		return left.resourceID < right.resourceID
	}
	return left.generation < right.generation
}

func stablePhysicalIdentityKeyLess(left, right stablePhysicalIdentityKey) bool {
	if left.platform != right.platform {
		return left.platform < right.platform
	}
	if left.volumeID != right.volumeID {
		return left.volumeID < right.volumeID
	}
	for i := range left.objectID {
		if left.objectID[i] != right.objectID[i] {
			return left.objectID[i] < right.objectID[i]
		}
	}
	return false
}

func stableResourceLogicalPriority(key stableLogicalResourceKey) uint64 {
	h := fnv.New64a()
	writeString := func(value string) {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], uint64(len(value)))
		_, _ = h.Write(raw[:])
		_, _ = h.Write([]byte(value))
	}
	writeString(string(key.kind))
	writeString(key.lane)
	writeString(key.resourceID)
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], key.generation)
	_, _ = h.Write(raw[:])
	return h.Sum64()
}

func stableResourcePhysicalPriority(key stablePhysicalIdentityKey) uint64 {
	h := fnv.New64a()
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(key.platform)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(key.platform))
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], key.volumeID)
	_, _ = h.Write(raw[:])
	_, _ = h.Write(key.objectID[:])
	return h.Sum64()
}

func findStableResourceLogical(root *stableResourceLogicalIndexNode, key stableLogicalResourceKey) *stableResourceEntry {
	for root != nil {
		if key == root.key {
			return root.entry
		}
		if stableLogicalResourceKeyLess(key, root.key) {
			root = root.left
		} else {
			root = root.right
		}
	}
	return nil
}

func findStableResourcePhysical(root *stableResourcePhysicalIndexNode, key stablePhysicalIdentityKey) []*stableResourceEntry {
	for root != nil {
		if key == root.key {
			return root.entries
		}
		if stablePhysicalIdentityKeyLess(key, root.key) {
			root = root.left
		} else {
			root = root.right
		}
	}
	return nil
}

func insertFreshStableResourceLogical(root *stableResourceLogicalIndexNode, entry *stableResourceEntry) *stableResourceLogicalIndexNode {
	key := entry.token.logicalKey()
	if root == nil {
		return &stableResourceLogicalIndexNode{key: key, entry: entry, priority: stableResourceLogicalPriority(key)}
	}
	if key == root.key {
		root.entry = entry
		return root
	}
	if stableLogicalResourceKeyLess(key, root.key) {
		root.left = insertFreshStableResourceLogical(root.left, entry)
		if root.left.priority < root.priority {
			promoted := root.left
			root.left = promoted.right
			promoted.right = root
			return promoted
		}
		return root
	}
	root.right = insertFreshStableResourceLogical(root.right, entry)
	if root.right.priority < root.priority {
		promoted := root.right
		root.right = promoted.left
		promoted.left = root
		return promoted
	}
	return root
}

func insertStableResourceLogical(root *stableResourceLogicalIndexNode, entry *stableResourceEntry) *stableResourceLogicalIndexNode {
	key := entry.token.logicalKey()
	if root == nil {
		return &stableResourceLogicalIndexNode{key: key, entry: entry, priority: stableResourceLogicalPriority(key)}
	}
	next := *root
	if key == root.key {
		next.entry = entry
		return &next
	}
	if stableLogicalResourceKeyLess(key, root.key) {
		next.left = insertStableResourceLogical(root.left, entry)
		result := &next
		if next.left.priority < next.priority {
			promoted := *next.left
			result.left = promoted.right
			promoted.right = result
			return &promoted
		}
		return result
	}
	next.right = insertStableResourceLogical(root.right, entry)
	result := &next
	if next.right.priority < next.priority {
		promoted := *next.right
		result.right = promoted.left
		promoted.left = result
		return &promoted
	}
	return result
}

func insertFreshStableResourcePhysical(root *stableResourcePhysicalIndexNode, entry *stableResourceEntry) *stableResourcePhysicalIndexNode {
	key := entry.token.physicalIdentityKey()
	if root == nil {
		return &stableResourcePhysicalIndexNode{key: key, entries: []*stableResourceEntry{entry}, priority: stableResourcePhysicalPriority(key)}
	}
	if key == root.key {
		root.entries = append(root.entries, entry)
		return root
	}
	if stablePhysicalIdentityKeyLess(key, root.key) {
		root.left = insertFreshStableResourcePhysical(root.left, entry)
		if root.left.priority < root.priority {
			promoted := root.left
			root.left = promoted.right
			promoted.right = root
			return promoted
		}
		return root
	}
	root.right = insertFreshStableResourcePhysical(root.right, entry)
	if root.right.priority < root.priority {
		promoted := root.right
		root.right = promoted.left
		promoted.left = root
		return promoted
	}
	return root
}

func insertStableResourcePhysical(root *stableResourcePhysicalIndexNode, entry *stableResourceEntry) *stableResourcePhysicalIndexNode {
	key := entry.token.physicalIdentityKey()
	if root == nil {
		return &stableResourcePhysicalIndexNode{key: key, entries: []*stableResourceEntry{entry}, priority: stableResourcePhysicalPriority(key)}
	}
	next := *root
	if key == root.key {
		next.entries = append(append([]*stableResourceEntry(nil), root.entries...), entry)
		return &next
	}
	if stablePhysicalIdentityKeyLess(key, root.key) {
		next.left = insertStableResourcePhysical(root.left, entry)
		result := &next
		if next.left.priority < next.priority {
			promoted := *next.left
			result.left = promoted.right
			promoted.right = result
			return &promoted
		}
		return result
	}
	next.right = insertStableResourcePhysical(root.right, entry)
	result := &next
	if next.right.priority < next.priority {
		promoted := *next.right
		result.right = promoted.left
		promoted.left = result
		return &promoted
	}
	return result
}

// replaceStableResourcePhysical path-copies the one physical-index branch that
// names old. Certified append-only coalescing keeps the retained rope as the
// token-owner lineage, so its current entry view must move with the logical
// index without rebuilding that lineage.
func replaceStableResourcePhysical(root *stableResourcePhysicalIndexNode, old, replacement *stableResourceEntry) *stableResourcePhysicalIndexNode {
	if root == nil {
		return nil
	}
	key := old.token.physicalIdentityKey()
	next := *root
	if key == root.key {
		next.entries = append([]*stableResourceEntry(nil), root.entries...)
		for i, entry := range next.entries {
			if entry == old {
				next.entries[i] = replacement
				return &next
			}
		}
		return root
	}
	if stablePhysicalIdentityKeyLess(key, root.key) {
		next.left = replaceStableResourcePhysical(root.left, old, replacement)
	} else {
		next.right = replaceStableResourcePhysical(root.right, old, replacement)
	}
	return &next
}

func buildStableResourceKindViews(entries []stableResourceEntry) (stableKindViews, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	views := newStableKindViews(0)
	directories := make(map[ResourceKind]*DependencyDirectoryV2)
	for i := range entries {
		entry := &entries[i]
		if directory := entry.logicalObligations.directory; directory != nil {
			if prior := directories[entry.token.kind]; prior != nil && prior != directory {
				return nil, ErrResourceConflict
			}
			directories[entry.token.kind] = directory
		}
	}
	transferred := make([]*StableResourceToken, 0, len(entries))
	for i := range entries {
		if err := entries[i].token.transfer(ResourceOwnerBuilder, ResourceOwnerShared); err != nil {
			for _, token := range transferred {
				_ = token.transfer(ResourceOwnerShared, ResourceOwnerBuilder)
			}
			return nil, err
		}
		transferred = append(transferred, entries[i].token)
	}
	for start := 0; start < len(entries); {
		kind := entries[start].token.kind
		end := start + 1
		for end < len(entries) && entries[end].token.kind == kind {
			end++
		}
		chunk := entries[start:end:end]
		view := stableResourceKindView{
			root: newStableResourceEntryLeaf(chunk), count: len(chunk),
			reachability: newStableReachabilitySet(0),
		}
		for i := range chunk {
			entry := &chunk[i]
			view.logical = insertFreshStableResourceLogical(view.logical, entry)
			view.physical = insertFreshStableResourcePhysical(view.physical, entry)
			if directory := entry.logicalObligations.directory; directory != nil {
				view.directory = directory
				// Directory counts were admitted while constructing this exact
				// root. Retain summaries, never rebuild its membership treap.
				view.logicalMembershipCount += entry.logicalObligations.count
			} else {
				entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
					var admitted bool
					view.logicalMembership, admitted = insertFreshStableLogicalMembership(view.logicalMembership, obligation)
					if admitted {
						view.logicalMembershipCount++
					}
					return true
				})
			}
			view.logicalCommitments = addStableLogicalObligationCommitments(view.logicalCommitments, entry.logicalObligations.commitments)
			view.logicalObligationCount += entry.logicalObligations.count
			for _, stableBinding1 := range entry.reachability.records() {
				field := stableBinding1.key
				view.reachability.set(field, struct{}{})
			}
		}
		views.set(kind, view)
		start = end
	}
	return views, nil
}

func cloneStableResourceKindViews(source stableKindViews, excluded map[ResourceKind]struct{}) (stableKindViews, bool) {
	if source.len() == 0 {
		return nil, true
	}
	clone := newStableKindViews(source.len())
	for _, stableBinding1 := range source.records() {
		kind := stableBinding1.key
		view := stableBinding1.value
		if _, skip := excluded[kind]; skip {
			continue
		}
		if view.root == nil || !view.root.retain() {
			releaseStableResourceKindViews(clone)
			return nil, false
		}
		clone.set(kind, view)
	}
	return clone, true
}

func releaseStableResourceKindViews(views stableKindViews) {
	var cursor int
	var started bool
	_ = releaseStableResourceKindViewsCursorV1(views, &cursor, &started)
}
func releaseStableResourceKindViewsCursorV1(views stableKindViews, cursor *int, started *bool) (result error) {
	records := views.records()
	for *cursor < len(records) {
		view := records[*cursor].value
		done, err := view.root.releaseCheckedV1(*started)
		*started = true
		result = errors.Join(result, err)
		if !done {
			return errors.Join(result, ErrStableResourceOperationBusy)
		}
		*cursor++
		*started = false
	}
	return
}

func stableResourceKindViewCount(views stableKindViews) int {
	count := 0
	for _, stableBinding1 := range views.records() {
		view := stableBinding1.value
		count += view.count
	}
	return count
}

func rangeStableResourceKindViews(views stableKindViews, visit func(*stableResourceEntry) bool) bool {
	for _, kind := range stableResourceKindsSorted(views) {
		if !rangeStableResourceLogicalIndex(views.get(kind).logical, visit) {
			return false
		}
	}
	return true
}

// rangeStableResourceLogicalIndex is the authoritative frozen-entry view.
// Roots retain token ownership; path-copied logical entries may therefore
// differ from the immutable rope entry after certified append-only coalescing.
func rangeStableResourceLogicalIndex(root *stableResourceLogicalIndexNode, visit func(*stableResourceEntry) bool) bool {
	stack := make([]*stableResourceLogicalIndexNode, 0, 16)
	for root != nil || len(stack) != 0 {
		for root != nil {
			stack = append(stack, root)
			root = root.left
		}
		last := len(stack) - 1
		root = stack[last]
		stack = stack[:last]
		if !visit(root.entry) {
			return false
		}
		root = root.right
	}
	return true
}

func stableResourceKindsSorted(views stableKindViews) []ResourceKind {
	kinds := make([]ResourceKind, 0, views.len())
	for _, stableBinding1 := range views.records() {
		kind := stableBinding1.key
		kinds = append(kinds, kind)
	}
	for i := 1; i < len(kinds); i++ {
		for j := i; j > 0 && kinds[j] < kinds[j-1]; j-- {
			kinds[j], kinds[j-1] = kinds[j-1], kinds[j]
		}
	}
	return kinds
}

func stableResourceViewsConflict(target stableKindViews, incoming *stableResourceEntry) bool {
	if view, ok := target.lookup(incoming.token.kind); ok && findStableResourceLogical(view.logical, incoming.token.logicalKey()) != nil {
		return true
	}
	physicalKey := incoming.token.physicalIdentityKey()
	for _, stableBinding1 := range target.records() {
		view := stableBinding1.value
		if len(findStableResourcePhysical(view.physical, physicalKey)) != 0 {
			return true
		}
	}
	return false
}

func stableResourceViewLogicalMembershipComplete(view stableResourceKindView) bool {
	return view.logicalMembershipCount == view.logicalObligationCount && (view.directory != nil || view.logicalMembership != nil || view.logicalMembershipCount == 0)
}

// stableResourceViewsAdmitLogicalObligations proves that entry adds no logical
// obligation already owned by another resource. A same-physical predecessor
// may repeat its own obligations; every kind is still probed so overlap cannot
// hide behind a different resource kind.
func stableResourceViewsAdmitLogicalObligations(views stableKindViews, entry, predecessor *stableResourceEntry, excluded map[ResourceKind]struct{}, work *StableResourceClosureWork) (bool, bool, error) {
	kinds := stableResourceKindsSorted(views)
	var predecessorKind ResourceKind
	if predecessor != nil {
		if token := activeEntryToken(*predecessor); token != nil {
			predecessorKind = token.kind
		}
	}
	for _, stableBinding1 := range views.records() {
		kind := stableBinding1.key
		view := stableBinding1.value
		if _, skip := excluded[kind]; !skip && !stableResourceViewLogicalMembershipComplete(view) {
			return false, false, nil
		}
	}
	admissible := true
	var lookupErr error
	walkErr := entry.logicalObligations.walk(func(obligation StableLogicalObligation) bool {
		for _, kind := range kinds {
			if _, skip := excluded[kind]; skip {
				continue
			}
			existing, found, err := lookupStableLogicalMembershipV2(views.get(kind).logicalMembership, views.get(kind).directory, views.get(kind).logical, kind, obligation, work)
			if err != nil {
				lookupErr = err
				return false
			}
			if !found {
				continue
			}
			if predecessor != nil && predecessorKind == kind {
				owned, ownedByPredecessor, err := predecessor.logicalObligations.lookup(obligation, nil)
				if err != nil {
					lookupErr = err
					return false
				}
				if ownedByPredecessor && owned == existing && existing == obligation {
					continue
				}
			}
			admissible = false
			return false
		}
		return true
	})
	if walkErr != nil {
		return false, true, walkErr
	}
	if lookupErr != nil {
		return false, true, lookupErr
	}
	return admissible, true, nil
}

func stableLogicalMembershipEvidenceAdmits(evidence map[ResourceKind]stableLogicalMembershipEvidence, sourceKinds map[ResourceKind]uint64, entry, predecessor *stableResourceEntry, excluded map[ResourceKind]struct{}, work *StableResourceClosureWork) (bool, bool, error) {
	if len(sourceKinds) == 0 {
		return false, false, nil
	}
	var predecessorKind ResourceKind
	if predecessor != nil {
		if token := activeEntryToken(*predecessor); token != nil {
			predecessorKind = token.kind
		}
	}
	kinds := make([]ResourceKind, 0, len(sourceKinds))
	for kind := range sourceKinds {
		if _, skip := excluded[kind]; skip {
			continue
		}
		candidate, exists := evidence[kind]
		if !exists {
			return false, false, nil
		}
		if !stableLogicalMembershipEvidenceComplete(candidate) {
			return false, false, nil
		}
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	admissible := true
	var lookupErr error
	walkErr := entry.logicalObligations.walk(func(obligation StableLogicalObligation) bool {
		for _, kind := range kinds {
			existing, found, err := lookupStableLogicalMembershipV2(evidence[kind].root, evidence[kind].directory, nil, kind, obligation, work)
			if err != nil {
				lookupErr = err
				return false
			}
			if !found {
				continue
			}
			if predecessor != nil && predecessorKind == kind {
				owned, ownedByPredecessor, err := predecessor.logicalObligations.lookup(obligation, nil)
				if err != nil {
					lookupErr = err
					return false
				}
				if ownedByPredecessor && owned == existing && existing == obligation {
					continue
				}
			}
			admissible = false
			return false
		}
		return true
	})
	if walkErr != nil {
		return false, true, walkErr
	}
	if lookupErr != nil {
		return false, true, lookupErr
	}
	return admissible, true, nil
}

// mergeDistinctStableResourceKindViews consumes both input root references on
// success. It declines before ownership mutation when any logical or physical
// identity needs the existing exact coalescing path.
func mergeDistinctStableResourceKindViews(target, incoming stableKindViews, work *StableResourceClosureWork) (stableKindViews, bool) {
	if target.len() == 0 {
		return incoming, true
	}
	if incoming.len() == 0 {
		return target, true
	}
	compatible := true
	for _, stableBinding1 := range incoming.records() {
		kind := stableBinding1.key
		child := stableBinding1.value
		if current, ok := target.lookup(kind); ok && current.directory != nil && child.directory != nil && current.directory != child.directory {
			return nil, false
		}
	}
	rangeStableResourceKindViews(incoming, func(entry *stableResourceEntry) bool {
		compatible = !stableResourceViewsConflict(target, entry)
		return compatible
	})
	if !compatible {
		return nil, false
	}
	next := newStableKindViews(target.len() + incoming.len())
	for _, stableBinding2 := range target.records() {
		kind := stableBinding2.key
		view := stableBinding2.value
		next.set(kind, view)
	}
	for _, stableBinding3 := range incoming.records() {
		kind := stableBinding3.key
		child := stableBinding3.value
		current, ok := next.lookup(kind)
		if !ok {
			next.set(kind, child)
			continue
		}
		logical, physical := current.logical, current.physical
		logicalMembership := current.logicalMembership
		logicalMembershipCount := current.logicalMembershipCount
		directory := current.directory
		if directory == nil {
			directory = child.directory
		}
		rangeStableResourceLogicalIndex(child.logical, func(entry *stableResourceEntry) bool {
			logical = insertStableResourceLogical(logical, entry)
			physical = insertStableResourcePhysical(physical, entry)
			if entry.logicalObligations.directory != nil {
				logicalMembershipCount += entry.logicalObligations.count
				return true
			}
			entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
				var admitted bool
				logicalMembership, admitted = insertStableLogicalMembership(logicalMembership, obligation, work)
				if admitted {
					logicalMembershipCount++
				}
				return true
			})
			return true
		})
		reachability := newStableReachabilitySet(current.reachability.len() + child.reachability.len())
		for _, stableBinding4 := range current.reachability.records() {
			field := stableBinding4.key
			reachability.set(field, struct{}{})
		}
		for _, stableBinding5 := range child.reachability.records() {
			field := stableBinding5.key
			reachability.set(field, struct{}{})
		}
		next.set(kind, stableResourceKindView{
			root:                   concatOwnedStableResourceEntryNodes(current.root, child.root),
			logical:                logical,
			physical:               physical,
			logicalMembership:      logicalMembership,
			directory:              directory,
			reachability:           reachability,
			logicalCommitments:     addStableLogicalObligationCommitments(current.logicalCommitments, child.logicalCommitments),
			logicalObligationCount: current.logicalObligationCount + child.logicalObligationCount,
			logicalMembershipCount: logicalMembershipCount,
			count:                  current.count + child.count,
		})
	}
	return next, true
}
