package rootpublication

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync/atomic"
)

// stableResourceEntryNode is a persistent rope over immutable frozen entry
// chunks. A set owns one reference to each kind root; concatenation consumes
// two owned roots without copying their retained entries or physical handles.
type stableResourceEntryNode struct {
	metadata    *resourceRopeMetadata
	refs        atomic.Int64
	left, right *stableResourceEntryNode
	entries     []stableResourceEntry
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

func (node *stableResourceEntryNode) release() {
	if node != nil && node.metadata != nil {
		releaseOwnedResourceRope(node)
		return
	}
	stack := []*stableResourceEntryNode{node}
	for len(stack) != 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == nil {
			continue
		}
		lastReference := false
		for refs := current.refs.Load(); refs > 0; refs = current.refs.Load() {
			if current.refs.CompareAndSwap(refs, refs-1) {
				lastReference = refs == 1
				break
			}
		}
		if !lastReference {
			continue
		}
		if current.left != nil || current.right != nil {
			stack = append(stack, current.left, current.right)
		} else {
			for i := range current.entries {
				current.entries[i].token.releaseFrom(ResourceOwnerShared)
			}
		}
		if current.metadata != nil {
			// Existing physical root refs govern this node allocation. Last
			// release clears its actual metadata aliases before refund.
			metadata := current.metadata
			current.left, current.right, current.entries, current.metadata = nil, nil, nil, nil
			owner, charge, backing := metadata.owner, metadata.charge, metadata.backing
			metadata.owner, metadata.charge, metadata.backing = nil, 0, nil
			backing.release()
			owner.RemovePending(charge)
		}
	}
}

func (node *stableResourceEntryNode) rangeEntries(visit func(*stableResourceEntry) bool) bool {
	if node != nil && node.metadata != nil {
		if node.left != nil && !node.left.rangeEntries(visit) {
			return false
		}
		if node.right != nil && !node.right.rangeEntries(visit) {
			return false
		}
		for i := range node.entries {
			if !visit(&node.entries[i]) {
				return false
			}
		}
		return true
	}

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
	allocation  *resourceAllocation
	key         stableLogicalResourceKey
	entry       *stableResourceEntry
	priority    uint64
	left, right *stableResourceLogicalIndexNode
}

type stableResourcePhysicalIndexNode struct {
	allocation  *resourceAllocation
	key         stablePhysicalIdentityKey
	entries     []*stableResourceEntry
	priority    uint64
	left, right *stableResourcePhysicalIndexNode
}

type stableResourceKindView struct {
	fields                 *resourceFieldBacking
	root                   *stableResourceEntryNode
	logical                *stableResourceLogicalIndexNode
	physical               *stableResourcePhysicalIndexNode
	logicalMembership      *stableLogicalObligationIndexNode
	directory              *DependencyDirectoryV2
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
	fields                 *resourceFieldBacking
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
		fields:                 view.fields,
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

func buildStableResourceKindViews(entries []stableResourceEntry) (*resourceKindBacking, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	views := newResourceKindViews(0)
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
			view.fields = appendGenericResourceFieldSummary(view.fields, entry.reachability, entry.logicalObligations.commitments)
			view.logicalObligationCount += entry.logicalObligations.count
		}
		views.set(kind, view)
		start = end
	}
	return views, nil
}

// Retain the same independent physical and metadata edges used by ordinary
// clones. Refusal unwinds only edges actually acquired by this call.
func retainStableResourceKindView(view stableResourceKindView) bool {
	if view.root == nil || !view.root.retain() {
		return false
	}
	if !view.logical.retainAllocation() {
		view.root.release()
		return false
	}
	if !view.physical.retainAllocation() {
		releaseOwnedResourceLogical(view.logical)
		view.root.release()
		return false
	}
	if !view.logicalMembership.retainAllocation() {
		releaseOwnedResourcePhysical(view.physical)
		releaseOwnedResourceLogical(view.logical)
		view.root.release()
		return false
	}
	if !view.fields.retain() {
		releaseOwnedResourceObligationIndex(view.logicalMembership)
		releaseOwnedResourcePhysical(view.physical)
		releaseOwnedResourceLogical(view.logical)
		view.root.release()
		return false
	}
	return true
}

func cloneStableResourceKindViews(source *resourceKindBacking, excluded map[ResourceKind]struct{}) (*resourceKindBacking, bool) {
	if source.len() == 0 {
		return nil, true
	}
	if source.allocation != nil {
		clone, err := cloneResourceKindBacking(source, excluded)
		return clone, err == nil
	}
	clone := newResourceKindViews(source.len())
	for kind, view := range source.all() {
		if _, skip := excluded[kind]; skip {
			continue
		}
		if !retainStableResourceKindView(view) {
			releaseStableResourceKindViews(clone)
			return nil, false
		}
		clone.set(kind, view)
	}
	return clone, true
}

func releaseStableResourceKindViews(views *resourceKindBacking) {
	if views != nil && views.allocation != nil {
		views.release()
		return
	}
	for _, view := range views.all() {
		releaseStableResourceKindView(view)
	}
}

func releaseStableResourceKindView(view stableResourceKindView) {
	// Metadata indexes own independent entry-array aliases. A shared physical
	// rope is not their release authority.
	releaseOwnedResourceLogical(view.logical)
	releaseOwnedResourcePhysical(view.physical)
	releaseOwnedResourceObligationIndex(view.logicalMembership)
	view.fields.release()
	view.root.release()
}

func stableResourceKindViewCount(views *resourceKindBacking) int {
	count := 0
	for _, view := range views.all() {
		count += view.count
	}
	return count
}

func rangeStableResourceKindViews(views *resourceKindBacking, visit func(*stableResourceEntry) bool) bool {
	if views != nil && views.allocation != nil {
		// Selected cells are already canonical and independently retained.
		for _, cell := range views.cells {
			if !rangeStableResourceLogicalIndex(cell.view.logical, visit) {
				return false
			}
		}
		return true
	}
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
	if root != nil && root.allocation != nil {
		return rangeOwnedResourceLogicalIndex(root, visit)
	}
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

func stableResourceKindsSorted(views *resourceKindBacking) []ResourceKind {
	kinds := make([]ResourceKind, 0, views.len())
	for kind := range views.all() {
		kinds = append(kinds, kind)
	}
	for i := 1; i < len(kinds); i++ {
		for j := i; j > 0 && kinds[j] < kinds[j-1]; j-- {
			kinds[j], kinds[j-1] = kinds[j-1], kinds[j]
		}
	}
	return kinds
}

func stableResourceViewsConflict(target *resourceKindBacking, incoming *stableResourceEntry) bool {
	if view, ok := target.lookup(incoming.token.kind); ok && findStableResourceLogical(view.logical, incoming.token.logicalKey()) != nil {
		return true
	}
	physicalKey := incoming.token.physicalIdentityKey()
	for _, view := range target.all() {
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
func stableResourceViewsAdmitLogicalObligations(views *resourceKindBacking, entry, predecessor *stableResourceEntry, excluded map[ResourceKind]struct{}, work *StableResourceClosureWork) (bool, bool, error) {
	if views != nil && views.allocation != nil && len(excluded) == 0 {
		return selectedViewsAdmit(views, entry, predecessor, nil, work)
	}
	kinds := stableResourceKindsSorted(views)
	var predecessorKind ResourceKind
	if predecessor != nil {
		if token := activeEntryToken(*predecessor); token != nil {
			predecessorKind = token.kind
		}
	}
	for kind, view := range views.all() {
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
func mergeDistinctStableResourceKindViews(target, incoming *resourceKindBacking, work *StableResourceClosureWork) (*resourceKindBacking, bool, error) {
	if (target != nil && target.allocation != nil) || (incoming != nil && incoming.allocation != nil) {
		merged, err := mergeOwnedResourceKindBackings(target, incoming)
		return merged, err == nil, err
	}
	if target.len() == 0 {
		return incoming, true, nil
	}
	if incoming.len() == 0 {
		return target, true, nil
	}
	compatible := true
	for kind, child := range incoming.all() {
		if current, ok := target.lookup(kind); ok && current.directory != nil && child.directory != nil && current.directory != child.directory {
			return nil, false, nil
		}
	}
	rangeStableResourceKindViews(incoming, func(entry *stableResourceEntry) bool {
		compatible = !stableResourceViewsConflict(target, entry)
		return compatible
	})
	if !compatible {
		return nil, false, nil
	}
	next := newResourceKindViews(target.len() + incoming.len())
	for kind, view := range target.all() {
		next.set(kind, view)
	}
	for kind, child := range incoming.all() {
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
		next.set(kind, stableResourceKindView{
			root:                   concatOwnedStableResourceEntryNodes(current.root, child.root),
			logical:                logical,
			physical:               physical,
			logicalMembership:      logicalMembership,
			directory:              directory,
			fields:                 mergeGenericResourceFieldSummaries(current.fields, child.fields),
			logicalObligationCount: current.logicalObligationCount + child.logicalObligationCount,
			logicalMembershipCount: logicalMembershipCount,
			count:                  current.count + child.count,
		})
	}
	return next, true, nil
}
