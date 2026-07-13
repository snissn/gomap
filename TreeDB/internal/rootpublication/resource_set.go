package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type stableResourceEntry struct {
	token        *StableResourceToken
	frontier     DurableFrontier
	reachability map[ReachabilityField]struct{}
}

func cloneStableResourceEntry(entry stableResourceEntry) stableResourceEntry {
	reachability := make(map[ReachabilityField]struct{}, len(entry.reachability))
	for field := range entry.reachability {
		reachability[field] = struct{}{}
	}
	return stableResourceEntry{token: entry.token, frontier: entry.frontier, reachability: reachability}
}

type StableResourceSetBuilder struct {
	mu        sync.Mutex
	entries   []stableResourceEntry
	required  map[ReachabilityField]struct{}
	closed    bool
	abandoned bool
	state     ResourceOwnerState
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
	if err := mergeOwnedToken(&builder.entries, token); err != nil {
		token.releaseFrom(ResourceOwnerBuilder)
		return err
	}
	return nil
}

func mergeOwnedToken(entries *[]stableResourceEntry, token *StableResourceToken) error {
	logicalKey := token.logicalKey()
	identityKey := token.identityKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && existing.identity != token.identity {
			return fmt.Errorf("%w: logical resource %q changed stable identity", ErrResourceConflict, logicalKey)
		}
		if existing.identityKey() != identityKey {
			continue
		}
		if existing.logicalKey() != logicalKey || existing.digest != token.digest ||
			!existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %q", ErrResourceConflict, identityKey)
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability[token.reachability] = struct{}{}
		if existing.namespace == nil && token.namespace != nil {
			// Preserve the one namespace-creation obligation independently of
			// insertion order by making its exact-handle token representative.
			entry.token = token
			existing.releaseFrom(ResourceOwnerBuilder)
		} else {
			token.releaseFrom(ResourceOwnerBuilder)
		}
		return nil
	}
	*entries = append(*entries, stableResourceEntry{
		token: token, frontier: token.frontier,
		reachability: map[ReachabilityField]struct{}{token.reachability: {}},
	})
	return nil
}

func mergeViewEntry(entries *[]stableResourceEntry, incoming stableResourceEntry) error {
	logicalKey := incoming.token.logicalKey()
	identityKey := incoming.token.identityKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && existing.identity != incoming.token.identity {
			return fmt.Errorf("%w: logical resource %q changed stable identity", ErrResourceConflict, logicalKey)
		}
		if existing.identityKey() != identityKey {
			continue
		}
		if existing.logicalKey() != logicalKey || existing.digest != incoming.token.digest ||
			!existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %q", ErrResourceConflict, identityKey)
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
		for field := range incoming.reachability {
			entry.reachability[field] = struct{}{}
		}
		if existing.namespace == nil && incoming.token.namespace != nil {
			entry.token = incoming.token
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
	defer builder.mu.Unlock()
	if builder.closed || builder.abandoned {
		return ErrResourceOwnership
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if ResourceOwnerState(child.owner.Load()) != ResourceOwnerBuilder {
		return ErrResourceOwnership
	}
	merged := cloneStableResourceEntries(builder.entries)
	for _, entry := range child.entries {
		if err := mergeViewEntry(&merged, entry); err != nil {
			return err
		}
	}
	if !child.owner.CompareAndSwap(uint32(ResourceOwnerBuilder), uint32(ResourceOwnerTransferred)) {
		return ErrResourceOwnership
	}
	// No token owner transition is necessary: both child and parent are in the
	// builder ownership phase, and only the parent can release them now.
	builder.entries = merged
	child.entries = nil
	return nil
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
}

type StableResourceSet struct {
	mu           sync.Mutex
	entries      []stableResourceEntry
	pinHighWater map[ResourceKind]uint64
	owner        atomic.Uint32
	createdAt    time.Time
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
		tokens[i] = entry.token
	}
	return tokens
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
		if entry.token.identity == identity && entry.token.generation == generation {
			return entry.frontier
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
		if err := entry.token.flushThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("flush stable resource %q: %w", entry.token.logicalKey(), err))
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
		if err := entry.token.syncThrough(entry.frontier); err != nil {
			errs = append(errs, fmt.Errorf("sync stable resource %q: %w", entry.token.logicalKey(), err))
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
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		for _, entry := range set.entries {
			if err := mergeViewEntry(&view.entries, entry); err != nil {
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
		if entry.token.identity == identity && entry.token.generation == generation && !entry.token.released.Load() {
			return ErrResourcePinned
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
	for _, entry := range set.entries {
		token := entry.token
		stats := byKind[token.kind]
		if stats == nil {
			stats = &ResourceKindStats{Kind: token.kind}
			byKind[token.kind] = stats
		}
		stats.PendingCount++
		stats.PendingBytes = saturatingAdd(stats.PendingBytes, entry.frontier.Bytes)
		registered := time.Unix(0, token.metrics.registeredNanos)
		age := now.Sub(registered)
		if age > stats.PendingAge {
			stats.PendingAge = age
		}
		stats.Flushes += token.metrics.flushes.Load()
		stats.FlushDuration += time.Duration(token.metrics.flushNanos.Load())
		stats.Syncs += token.metrics.syncs.Load()
		stats.SyncDuration += time.Duration(token.metrics.syncNanos.Load())
		if token.namespace != nil {
			stats.NamespaceSyncs += token.namespace.syncs.Load()
			stats.NamespaceSyncDuration += time.Duration(token.namespace.syncNanos.Load())
		}
		if !token.released.Load() {
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
