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
	token              *StableResourceToken
	pins               []*StableResourceToken
	pinIndex           map[*StableResourceToken]struct{}
	frontier           DurableFrontier
	reachability       map[ReachabilityField]struct{}
	logicalObligations map[string]StableLogicalObligation
}

func cloneStableResourceEntry(entry stableResourceEntry) stableResourceEntry {
	reachability := make(map[ReachabilityField]struct{}, len(entry.reachability))
	for field := range entry.reachability {
		reachability[field] = struct{}{}
	}
	logicalObligations := make(map[string]StableLogicalObligation, len(entry.logicalObligations))
	for key, obligation := range entry.logicalObligations {
		logicalObligations[key] = obligation
	}
	clone := stableResourceEntry{
		token: entry.token, frontier: cloneDurableFrontier(entry.frontier),
		reachability: reachability, logicalObligations: logicalObligations,
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

func mergeStableLogicalObligations(target map[string]StableLogicalObligation, incoming []StableLogicalObligation) error {
	for _, obligation := range incoming {
		key := stableLogicalObligationKey(obligation)
		if existing, ok := target[key]; ok && existing != obligation {
			return fmt.Errorf("%w: logical obligation %q has conflicting immutable checksum or digest", ErrResourceConflict, key)
		}
	}
	for _, obligation := range incoming {
		key := stableLogicalObligationKey(obligation)
		target[key] = obligation
	}
	return nil
}

func stableLogicalObligationMap(obligations []StableLogicalObligation) map[string]StableLogicalObligation {
	out := make(map[string]StableLogicalObligation, len(obligations))
	for _, obligation := range obligations {
		out[stableLogicalObligationKey(obligation)] = obligation
	}
	return out
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
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(token) {
			return fmt.Errorf("%w: logical resource %q changed stable identity", ErrResourceConflict, logicalKey)
		}
		coalesce, err := stableResourcesCoalesce(existing, token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(token) || !frontierCompatible(entry.frontier, token.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %q", ErrResourceConflict, existing.identityKey())
		}
		if err := mergeStableLogicalObligations(entry.logicalObligations, token.logicalObligations); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability[token.reachability] = struct{}{}
		if existing.namespace == nil && token.namespace != nil {
			// Preserve the one namespace-creation obligation independently of
			// insertion order by making its exact-handle token representative.
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
		token: token, frontier: cloneDurableFrontier(token.frontier),
		reachability:       map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations: stableLogicalObligationMap(token.logicalObligations),
	})
	return nil
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

func mergeViewEntry(entries *[]stableResourceEntry, incoming stableResourceEntry, retainSourcePins bool) error {
	logicalKey := incoming.token.logicalKey()
	for i := range *entries {
		entry := &(*entries)[i]
		existing := entry.token
		if existing.logicalKey() == logicalKey && !existing.samePhysicalIdentity(incoming.token) {
			return fmt.Errorf("%w: logical resource %q changed stable identity", ErrResourceConflict, logicalKey)
		}
		coalesce, err := stableResourcesCoalesce(existing, incoming.token)
		if err != nil {
			return err
		}
		if !coalesce {
			continue
		}
		if !existing.namespaceCompatible(incoming.token) || !frontierCompatible(entry.frontier, incoming.frontier) {
			return fmt.Errorf("%w: incompatible duplicate stable identity %q", ErrResourceConflict, existing.identityKey())
		}
		incomingObligations := make([]StableLogicalObligation, 0, len(incoming.logicalObligations))
		for _, obligation := range incoming.logicalObligations {
			incomingObligations = append(incomingObligations, obligation)
		}
		if err := mergeStableLogicalObligations(entry.logicalObligations, incomingObligations); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, incoming.frontier)
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
	for _, entry := range child.entries {
		if err := mergeViewEntry(&merged, entry, false); err != nil {
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
	child.entries = nil
	child.mu.Unlock()
	builder.mu.Unlock()
	for _, token := range dropped {
		token.releaseFrom(ResourceOwnerBuilder)
	}
	return nil
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

// StableResourceDescriptor is an immutable-by-copy view of one coalesced
// physical resource obligation. Unlike Tokens, it reports the unioned frontier,
// every reachability field, and every logical obligation retained by the set.
type StableResourceDescriptor struct {
	kind               ResourceKind
	identity           StableIdentity
	generation         uint64
	digest             [32]byte
	frontier           DurableFrontier
	reachability       []ReachabilityField
	logicalObligations []StableLogicalObligation
}

func (descriptor StableResourceDescriptor) Kind() ResourceKind       { return descriptor.kind }
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
		logicalObligations := make([]StableLogicalObligation, 0, len(entry.logicalObligations))
		for _, obligation := range entry.logicalObligations {
			logicalObligations = append(logicalObligations, obligation)
		}
		sort.Slice(logicalObligations, func(i, j int) bool {
			return stableLogicalObligationKey(logicalObligations[i]) < stableLogicalObligationKey(logicalObligations[j])
		})
		descriptors[i] = StableResourceDescriptor{
			kind: entry.token.kind, identity: entry.token.identity, generation: entry.token.generation,
			digest: entry.token.digest, frontier: cloneDurableFrontier(entry.frontier), reachability: fields,
			logicalObligations: logicalObligations,
		}
	}
	return descriptors
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
			errs = append(errs, fmt.Errorf("flush stable resource %q: %w", token.logicalKey(), err))
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
			errs = append(errs, fmt.Errorf("sync stable resource %q: %w", token.logicalKey(), err))
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
			if err := mergeViewEntry(&view.entries, entry, true); err != nil {
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
		stats.LogicalObligationCount = saturatingAdd(stats.LogicalObligationCount, uint64(len(entry.logicalObligations)))
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
			syncs, duration := token.namespace.physicalSyncStats()
			stats.NamespaceSyncs += syncs
			stats.NamespaceSyncDuration += duration
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
