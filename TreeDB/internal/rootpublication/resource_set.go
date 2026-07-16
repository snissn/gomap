package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
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
	logicalObligations []StableLogicalObligation
}

func cloneStableResourceEntry(entry stableResourceEntry) stableResourceEntry {
	reachability := make(map[ReachabilityField]struct{}, len(entry.reachability))
	for field := range entry.reachability {
		reachability[field] = struct{}{}
	}
	clone := stableResourceEntry{
		token: entry.token, logicalLane: entry.logicalLane, resourceID: entry.resourceID,
		diagnosticPath: entry.diagnosticPath, frontier: cloneDurableFrontier(entry.frontier),
		// Logical obligations are immutable after normalization. The full slice
		// expression makes every clone copy-on-append while avoiding a redundant
		// backing-array allocation at each ownership transfer.
		reachability: reachability, logicalObligations: entry.logicalObligations[:len(entry.logicalObligations):len(entry.logicalObligations)],
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

func mergeStableLogicalObligations(target *[]StableLogicalObligation, incoming []StableLogicalObligation) error {
	if len(*target)+len(incoming) > stableLogicalObligationLinearLimit {
		byKey := make(map[stableLogicalObligationIndex]StableLogicalObligation, len(*target)+len(incoming))
		for _, obligation := range *target {
			byKey[stableLogicalObligationKey(obligation)] = obligation
		}
		// Preflight the complete incoming batch before mutating the live builder.
		// Add is atomic: a later conflict must not leave an earlier obligation
		// appended to the existing resource entry.
		for _, obligation := range incoming {
			key := stableLogicalObligationKey(obligation)
			if existing, ok := byKey[key]; ok {
				if existing != obligation {
					return fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
				}
			}
		}
		for _, obligation := range incoming {
			key := stableLogicalObligationKey(obligation)
			if _, ok := byKey[key]; ok {
				continue
			}
			byKey[key] = obligation
			*target = append(*target, obligation)
		}
		return nil
	}
	// The small-set path deliberately stays allocation-free. Its first pass is
	// conflict-only; the second pass applies additions after the whole batch is
	// known compatible.
	for _, obligation := range incoming {
		key := stableLogicalObligationKey(obligation)
		for _, existing := range *target {
			if stableLogicalObligationKey(existing) != key {
				continue
			}
			if existing != obligation {
				return fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
			}
			break
		}
	}
	for _, obligation := range incoming {
		key := stableLogicalObligationKey(obligation)
		duplicate := false
		for _, existing := range *target {
			if stableLogicalObligationKey(existing) == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			*target = append(*target, obligation)
		}
	}
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
		if err := mergeStableLogicalObligations(&entry.logicalObligations, token.logicalObligations); err != nil {
			return err
		}
		entry.frontier = maxFrontier(entry.frontier, token.frontier)
		entry.reachability[token.reachability] = struct{}{}
		mergeStableResourceDescriptorIdentity(entry, token.logicalLane, token.resourceID, token.diagnosticPath)
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
		token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: cloneDurableFrontier(token.frontier),
		reachability:       map[ReachabilityField]struct{}{token.reachability: {}},
		logicalObligations: stableLogicalObligationList(token.logicalObligations),
	})
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

func mergeViewEntry(entries *[]stableResourceEntry, incoming stableResourceEntry, retainSourcePins bool) error {
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
		logicalObligations := cloneStableLogicalObligations(entry.logicalObligations)
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
	if source == nil {
		return nil, nil
	}
	requirementIndex, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return nil, err
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
		return nil, ErrResourceOwnership
	}
	for _, entry := range source.entries {
		token := activeEntryToken(entry)
		if token == nil || token.released.Load() {
			return nil, ErrResourceOwnership
		}
		if _, skip := excludedKinds[token.kind]; skip {
			continue
		}
		fields := make([]ReachabilityField, 0, len(entry.reachability))
		for field := range entry.reachability {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		for _, field := range fields {
			obligations := make([]StableLogicalObligation, 0, len(entry.logicalObligations))
			for _, obligation := range entry.logicalObligations {
				if obligation.Reachability != field {
					continue
				}
				if _, scoped := requirementIndex.scoped[field]; scoped {
					if _, desired := requirementIndex.desired[field][obligation]; !desired {
						continue
					}
				}
				obligations = append(obligations, obligation)
			}
			if _, scoped := requirementIndex.scoped[field]; scoped && len(obligations) == 0 {
				continue
			}
			namespace, err := token.namespace.cloneStable()
			if err != nil {
				return nil, err
			}
			var registry *IdentityPinRegistry
			if token.identityPin != nil {
				registry = token.identityPin.registry
				if err := registry.Observe(token.identity); err != nil {
					namespace.Release()
					return nil, err
				}
			}
			observed := registry != nil
			var cloned *StableResourceToken
			err = token.WithPinnedFile(func(file *os.File) error {
				var constructErr error
				cloned, constructErr = NewStableResourceToken(StableResourceSpec{
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
				})
				return constructErr
			})
			if err != nil {
				if observed {
					_ = registry.Unobserve(token.identity)
				}
				namespace.Release()
				return nil, err
			}
			if err := builder.Add(cloned); err != nil {
				cloned.Release()
				namespace.Release()
				return nil, err
			}
			namespace.Release()
		}
	}
	cloned, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	abandon = false
	return cloned, nil
}

// ValidateStableResourceSetLogicalObligations proves that the scoped fields in
// resources contain exactly the candidate root's desired logical references:
// no missing obligation and no stale extra obligation.
func ValidateStableResourceSetLogicalObligations(resources *StableResourceSet, requirements StableLogicalObligationRequirements) error {
	index, err := indexStableLogicalObligationRequirements(requirements)
	if err != nil {
		return err
	}
	if len(index.scoped) == 0 {
		return nil
	}
	actual := make(map[ReachabilityField]map[StableLogicalObligation]struct{}, len(index.scoped))
	for field := range index.scoped {
		actual[field] = make(map[StableLogicalObligation]struct{})
	}
	if resources != nil {
		for _, descriptor := range resources.Descriptors() {
			fields := descriptor.ReachabilityFields()
			obligations := descriptor.LogicalObligations()
			for _, field := range fields {
				if _, scoped := index.scoped[field]; !scoped {
					continue
				}
				foundForField := false
				for _, obligation := range obligations {
					if obligation.Reachability != field {
						continue
					}
					foundForField = true
					if _, desired := index.desired[field][obligation]; !desired {
						return fmt.Errorf("%w: stale logical obligation %+v", ErrResourceConflict, obligation)
					}
					if _, duplicate := actual[field][obligation]; duplicate {
						return fmt.Errorf("%w: duplicate logical obligation %+v", ErrResourceConflict, obligation)
					}
					actual[field][obligation] = struct{}{}
				}
				if !foundForField {
					return fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, field)
				}
			}
		}
	}
	for field, desired := range index.desired {
		for obligation := range desired {
			if _, ok := actual[field][obligation]; !ok {
				return fmt.Errorf("%w: missing logical obligation %+v", ErrUnresolvedResource, obligation)
			}
		}
	}
	return nil
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
