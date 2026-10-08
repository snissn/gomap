package rootpublication

import (
	"slices"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Selected encodings own their metadata independently of the entry cache,
// physical token, and publication seal. No token/callback edge is prolonged
// merely to keep a durable encoding alive. Generic diagnostic lifetimes remain
// unchanged; selected manifests have the existing publisher's explicit scope.
func newOwnedDependencyManifestEntryV1(entry *stableResourceEntry, owner *retainedalloc.Owner) (*dependencyManifestEncodedEntryV1, error) {
	if entry == nil || entry.outgoing == nil || entry.outgoing.allocation.owner != owner || entry.token.metadata == nil || entry.token.metadata.owner != owner {
		return nil, ErrResourceOwnership
	}
	token, out := entry.token, entry.outgoing
	if token.kind == "" || entry.resourceID == "" || token.generation == 0 || !token.identity.valid() || token.identity.Generation != token.generation {
		return nil, ErrDependencyManifestFormat
	}
	if err := validateDiagnosticPath(entry.diagnosticPath); err != nil {
		return nil, err
	}
	if err := validateDurableFrontier(entry.frontier); err != nil {
		return nil, err
	}
	normalized := DependencyManifestEntryV1{Kind: token.kind, LogicalLane: entry.logicalLane, ResourceID: entry.resourceID, DiagnosticPath: entry.diagnosticPath, Identity: token.identity, Generation: token.generation, Digest: token.digest, Frontier: entry.frontier}
	fields := out.fields.reachableCount()
	if fields == 0 {
		return nil, ErrDependencyManifestFormat
	}
	values, scratch, err := acquireOwnedEntryValues(entry, owner)
	if err != nil {
		return nil, err
	}
	defer func() { clear(values); scratch.drop(); scratch.refund() }()
	for i, value := range values {
		if !out.fields.hasReachable(value.Reachability) {
			return nil, ErrDependencyManifestFormat
		}
		if err := validateStableLogicalObligation(value, value.Reachability); err != nil {
			return nil, err
		}
		if i > 0 && (!stableLogicalObligationLess(values[i-1], value) || stableLogicalObligationKey(values[i-1]) == stableLogicalObligationKey(value)) {
			return nil, ErrDependencyManifestFormat
		}
	}
	var ns DependencyManifestNamespaceV1
	if token.namespace != nil {
		n := token.namespace
		ns = DependencyManifestNamespaceV1{ParentIdentity: n.parentIdentity, Operation: n.operation, OldName: n.oldName, NewName: n.newName, DiagnosticPath: n.diagnosticPath}
		if !ns.ParentIdentity.valid() || ns.ParentIdentity.Generation == 0 || (ns.Operation != NamespaceCreate && ns.Operation != NamespaceRename) || !stableChildBaseName(ns.NewName) || (ns.Operation == NamespaceRename && !stableChildBaseName(ns.OldName)) {
			return nil, ErrDependencyManifestFormat
		}
		if err := validateDiagnosticPath(ns.DiagnosticPath); err != nil {
			return nil, err
		}
		normalized.Namespace = &ns
	}
	var rids []uint64
	if entry.frontier.exactRIDs != nil {
		rids = entry.frontier.exactRIDs.values
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dependencyManifestEncodedEntryV1{})))
	add := func(size uint64) bool {
		size = retainedalloc.AllocationCharge(size)
		if size > ^uint64(0)-charge {
			return false
		}
		charge += size
		return true
	}
	addString := func(text string) bool { return add(uint64(len(text))) }
	for _, text := range [...]string{string(normalized.Kind), normalized.LogicalLane, normalized.ResourceID, normalized.DiagnosticPath, normalized.Identity.Platform} {
		if !addString(text) {
			return nil, retainedalloc.ErrCapacity
		}
	}
	if !add(uint64(fields)*uint64(unsafe.Sizeof(ReachabilityField("")))) || !add(uint64(len(values))*uint64(unsafe.Sizeof(StableLogicalObligation{}))) {
		return nil, retainedalloc.ErrCapacity
	}
	for field := range out.fields.reachableFields() {
		if !addString(string(field)) {
			return nil, retainedalloc.ErrCapacity
		}
	}
	for _, v := range values {
		for _, text := range [...]string{v.Class, v.Kind, v.Namespace, string(v.Reachability)} {
			if !addString(text) {
				return nil, retainedalloc.ErrCapacity
			}
		}
	}
	if entry.frontier.exactRIDs != nil && (!add(uint64(unsafe.Sizeof(exactRIDMembership{}))) || !add(uint64(len(rids))*8)) {
		return nil, retainedalloc.ErrCapacity
	}
	if normalized.Namespace != nil {
		if !add(uint64(unsafe.Sizeof(DependencyManifestNamespaceV1{}))) {
			return nil, retainedalloc.ErrCapacity
		}
		for _, text := range [...]string{ns.ParentIdentity.Platform, ns.OldName, ns.NewName, ns.DiagnosticPath} {
			if !addString(text) {
				return nil, retainedalloc.ErrCapacity
			}
		}
	}
	// Exact source counts and byte lengths price the final encoding BEFORE make.
	rawBytes := dependencyManifestEntrySizeV1(normalized, rids)
	for field := range out.fields.reachableFields() {
		rawBytes += 4 + len(field)
	}
	for _, v := range values {
		rawBytes += 92 + len(v.Class) + len(v.Kind) + len(v.Namespace) + len(v.Reachability)
	}
	if rawBytes < 0 || rawBytes > maxDependencyManifestBytesV1 || !add(uint64(rawBytes)) {
		return nil, retainedalloc.ErrCapacity
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	normalized.Kind = ResourceKind(strings.Clone(string(normalized.Kind)))
	normalized.LogicalLane, normalized.ResourceID, normalized.DiagnosticPath = strings.Clone(normalized.LogicalLane), strings.Clone(normalized.ResourceID), strings.Clone(normalized.DiagnosticPath)
	normalized.Identity.Platform = strings.Clone(normalized.Identity.Platform)
	normalized.Frontier = cloneDurableFrontier(normalized.Frontier)
	normalized.Reachability = make([]ReachabilityField, 0, fields)
	for field := range out.fields.reachableFields() {
		normalized.Reachability = append(normalized.Reachability, ReachabilityField(strings.Clone(string(field))))
	}
	slices.Sort(normalized.Reachability)
	normalized.LogicalObligations = make([]StableLogicalObligation, len(values))
	for i, v := range values {
		v.Class, v.Kind, v.Namespace = strings.Clone(v.Class), strings.Clone(v.Kind), strings.Clone(v.Namespace)
		v.Reachability = ReachabilityField(strings.Clone(string(v.Reachability)))
		normalized.LogicalObligations[i] = v
	}
	if normalized.Namespace != nil {
		copied := ns
		copied.ParentIdentity.Platform, copied.OldName, copied.NewName, copied.DiagnosticPath = strings.Clone(ns.ParentIdentity.Platform), strings.Clone(ns.OldName), strings.Clone(ns.NewName), strings.Clone(ns.DiagnosticPath)
		normalized.Namespace = &copied
	}
	rids = nil
	if normalized.Frontier.exactRIDs != nil {
		rids = normalized.Frontier.exactRIDs.values
	}
	raw := appendDependencyManifestEntryV1(make([]byte, 0, rawBytes), normalized, rids)
	return &dependencyManifestEncodedEntryV1{allocation: allocation, entry: normalized, encoded: raw}, nil
}

func (value *dependencyManifestEncodedEntryV1) releaseOwned() {
	if value == nil || value.allocation == nil || !value.allocation.drop() {
		return
	}
	clear(value.encoded)
	value.encoded = nil
	clear(value.entry.Reachability)
	clear(value.entry.LogicalObligations)
	if value.entry.Frontier.exactRIDs != nil {
		clear(value.entry.Frontier.exactRIDs.values)
	}
	value.entry = DependencyManifestEntryV1{}
	value.cacheCandidate = nil
	value.allocation.refund()
}

// ReleaseOwnedMetadataV1 ends only a selected publisher-scoped manifest. Its
// caller must finish all synchronous encoding/materialization/diagnostic uses
// before release. Generic manifests retain their canonical diagnostic lifetime.
func (manifest *DependencyManifestV1) ReleaseOwnedMetadataV1() {
	if manifest == nil {
		return
	}
	manifest.metadataMu.Lock()
	defer manifest.metadataMu.Unlock()
	if !manifest.selected || manifest.released {
		return
	}
	manifest.released = true
	manifest.dropOwnedMetadataLockedV1()
}

// Every scoped read owns the same immutable allocation edge. Ending the public
// owner while a callback runs cannot clear its strings, arrays or encoding.
func (manifest *DependencyManifestV1) dropOwnedMetadataLockedV1() {
	allocation := manifest.allocation
	if allocation == nil || !allocation.drop() {
		return
	}
	manifest.allocation = nil
	for i, value := range manifest.entries {
		manifest.entries[i] = nil
		value.releaseOwned()
	}
	clear(manifest.loadedEntries)
	manifest.loadedEntries = nil
	manifest.entries, manifest.payload = nil, nil
	manifest.byteLength, manifest.digest = 0, [32]byte{}
	allocation.refund()
}
func (manifest *DependencyManifestV1) HasOwnedMetadataV1() bool {
	if manifest == nil {
		return false
	}
	manifest.metadataMu.Lock()
	defer manifest.metadataMu.Unlock()
	return manifest.selected
}

// WithEntriesV1 supplies a synchronous, read-only diagnostic/validation scope.
// The callback must not mutate or retain any slice, string or pointer supplied
// by the scope, or pass them to a constructor retaining borrowed metadata.
// Selected scratch is reserved before allocation; the same immutable manifest
// edge stays charged until the callback and its scratch have actually ended.
// Generic Entries and its independent diagnostic lifetime are unchanged.
func (manifest *DependencyManifestV1) WithEntriesV1(visit func([]DependencyManifestEntryV1) error) error {
	if manifest == nil || visit == nil {
		return ErrDependencyManifestFormat
	}
	manifest.metadataMu.Lock()
	if !manifest.selected {
		manifest.metadataMu.Unlock()
		return visit(manifest.Entries())
	}
	if manifest.released || manifest.allocation == nil || !manifest.allocation.retain() {
		manifest.metadataMu.Unlock()
		return ErrResourceOwnership
	}
	allocation, encoded := manifest.allocation, manifest.entries
	manifest.metadataMu.Unlock()
	defer func() {
		manifest.metadataMu.Lock()
		manifest.dropOwnedMetadataLockedV1()
		manifest.metadataMu.Unlock()
	}()
	width := uint64(unsafe.Sizeof(DependencyManifestEntryV1{}))
	if uint64(len(encoded)) > ^uint64(0)/width {
		return retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(len(encoded)) * width)
	if err := allocation.owner.AddPending(charge); err != nil {
		return err
	}
	out := make([]DependencyManifestEntryV1, len(encoded))
	defer func() { clear(out); out = nil; allocation.owner.RemovePending(charge) }()
	for i, value := range encoded {
		out[i] = value.entry
	}
	return visit(out)
}

func (set *StableResourceSet) ownedDependencyManifestV1() (*DependencyManifestV1, DependencyManifestBuildWorkV1, error) {
	borrow, err := set.borrowSelectedKindViews()
	if err != nil {
		return nil, DependencyManifestBuildWorkV1{}, err
	}
	defer borrow.release()
	owner := borrow.backing.allocation.owner
	count := stableResourceKindViewCount(borrow.backing)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyManifestV1{}))) + retainedalloc.AllocationCharge(uint64(count)*uint64(unsafe.Sizeof((*dependencyManifestEncodedEntryV1)(nil))))
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, DependencyManifestBuildWorkV1{}, err
	}
	encoded := make([]*dependencyManifestEncodedEntryV1, 0, count)
	work := DependencyManifestBuildWorkV1{}
	var failure error
	for _, view := range borrow.backing.all() {
		// Immutable logical-index recursion uses no temporary growing entry stack.
		rangeOwnedManifestEntriesV1(view.logical, func(entry *stableResourceEntry) bool {
			work.EntriesVisited++
			cache := entry.dependencyManifestV1
			if cache == nil || entry.outgoing == nil {
				failure = ErrResourceOwnership
				return false
			}
			cache.mu.Lock()
			defer cache.mu.Unlock()
			value := cache.value
			if value == nil {
				value, failure = newOwnedDependencyManifestEntryV1(entry, owner)
				if failure != nil {
					return false
				}
				// Its initial reference belongs to the private manifest construction.
				// Cache installation follows complete manifest admission/validation.
				value.cacheCandidate = cache
				work.EntriesEncoded++
				work.BytesEncoded += uint64(len(value.encoded))
			} else if value.allocation == nil || value.allocation.owner != owner || !value.allocation.retain() {
				failure = ErrResourceOwnership
				return false
			}
			encoded = append(encoded, value)
			return true
		})
		if failure != nil {
			break
		}
	}
	if failure == nil {
		var manifest *DependencyManifestV1
		manifest, failure = newDependencyManifestV1FromEncoded(encoded, true)
		if failure == nil {
			manifest.allocation = allocation
			manifest.selected = true
			for _, value := range encoded {
				if cache := value.cacheCandidate; cache != nil {
					cache.mu.Lock()
					if cache.value == nil && value.allocation.retain() {
						cache.value = value
					}
					value.cacheCandidate = nil
					cache.mu.Unlock()
				}
			}
			return manifest, work, nil
		}
	}
	for i, value := range encoded {
		encoded[i] = nil
		value.releaseOwned()
	}
	encoded = nil
	allocation.drop()
	allocation.refund()
	return nil, work, failure
}

// Only the selected immutable constructor path uses this recursive traversal.
// Each visit remains real work; recursion replaces the unadmitted growable
// traversal slice, without adding a second stored index or native cost claim.
func rangeOwnedManifestEntriesV1(root *stableResourceLogicalIndexNode, visit func(*stableResourceEntry) bool) bool {
	if root == nil {
		return true
	}
	return rangeOwnedManifestEntriesV1(root.left, visit) && visit(root.entry) && rangeOwnedManifestEntriesV1(root.right, visit)
}

// Durable runtime needs the selected record/ref, not a consumed encoding view.
// Generic manifest diagnostic ownership remains unchanged.
func (manifest *DependencyManifestV1) DiagnosticProjectionV1() *DependencyManifestV1 {
	if manifest.HasOwnedMetadataV1() {
		return nil
	}
	return manifest
}
