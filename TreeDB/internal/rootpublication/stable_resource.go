package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var stableOwnershipMu sync.Mutex
var stableNamespaceSyncMu sync.Mutex

var (
	ErrInvalidStableResource              = errors.New("invalid stable resource")
	ErrResourceFrontierBeyondLength       = errors.New("stable resource frontier beyond length")
	ErrResourceConflict                   = errors.New("conflicting stable resource identity or digest")
	ErrResourcePinned                     = errors.New("stable resource identity is pinned")
	ErrMissingResourceDependency          = errors.New("missing stable resource dependency")
	ErrNamespacePersistenceUnsupported    = errors.New("stable namespace persistence unsupported")
	ErrStableResourceOwnershipTransferred = errors.New("stable resource ownership already transferred")
)

// StableIdentity is the platform identity captured from an already-open file
// or namespace handle. Diagnostic paths are intentionally absent.
type StableIdentity struct {
	Device uint64
	File   uint64
}

func (id StableIdentity) valid() bool { return id.Device != 0 || id.File != 0 }

// ResourceKind names every physical resource class that may become reachable
// from an authoritative root, catalog, or command frame.
type ResourceKind string

const (
	ResourceValueLogSegment       ResourceKind = "value_log_segment"
	ResourceValueLogReference     ResourceKind = "value_log_reference"
	ResourceOuterLeafSegment      ResourceKind = "outer_leaf_segment"
	ResourceOuterLeafReference    ResourceKind = "outer_leaf_reference"
	ResourceOuterLeafGeneration   ResourceKind = "outer_leaf_generation"
	ResourceOuterLeafManifest     ResourceKind = "outer_leaf_manifest"
	ResourceDictionaryGeneration  ResourceKind = "dictionary_generation"
	ResourceTemplateCatalog       ResourceKind = "template_catalog"
	ResourceColumnAssetSegment    ResourceKind = "column_asset_segment"
	ResourceColumnAsset           ResourceKind = "column_asset"
	ResourceTypedColumnValueAsset ResourceKind = "typed_column_value_asset"
	ResourceTypedColumnCodeAsset  ResourceKind = "typed_column_code_asset"
	ResourceVectorGraphPack       ResourceKind = "vector_graph_pack"
	ResourceCommandWALSegment     ResourceKind = "command_wal_segment"
	ResourceCommandWALReference   ResourceKind = "command_wal_reference"
	ResourceCommandWALExternalRID ResourceKind = "command_wal_external_rid"
)

// StableResourceHandle is an identity-pinned producer-owned handle. It never
// accepts a path. Pin/Release protect the identity from deletion while a token
// owns it; flush and sync are bound to the same handle.
type StableResourceHandle interface {
	StableIdentity() (StableIdentity, error)
	StableLength() (uint64, error)
	FlushThrough(uint64) error
	SyncThrough(uint64) error
	Pin() error
	Release() error
}

type NamespaceOperation string

const (
	NamespaceCreate NamespaceOperation = "create"
	NamespaceRename NamespaceOperation = "rename"
)

// StableNamespaceHandle separates non-mutating capability validation from the
// actual structural sync. Registration must never perform SyncNamespace.
type StableNamespaceHandle interface {
	StableIdentity() (StableIdentity, error)
	StableGeneration() (uint64, error)
	ValidateNamespacePersistence() error
	SyncNamespace() error
	Pin() error
	Release() error
}

type StableNamespaceSpec struct {
	Operation            NamespaceOperation
	ParentDiagnosticPath string
	ParentGeneration     uint64
	Parent               StableNamespaceHandle
}

// StableNamespaceToken pins the already-open parent namespace adapter and
// records whether the requested create/rename boundary has actually synced.
type StableNamespaceToken struct {
	operation            NamespaceOperation
	parentDiagnosticPath string
	parentIdentity       StableIdentity
	parentGeneration     uint64
	parent               StableNamespaceHandle
	syncMu               sync.Mutex
	stable               atomic.Bool
	owner                *stableNamespacePinOwner
	lease                *stableNamespacePinOwner
}

type stableNamespacePinOwner struct {
	once     sync.Once
	mu       sync.RWMutex
	handle   StableNamespaceHandle
	released bool
	err      error
}

func (o *stableNamespacePinOwner) release() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.once.Do(func() {
		o.err = o.handle.Release()
		o.released = true
	})
	return o.err
}

func (o *stableNamespacePinOwner) use(fn func() error) error {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.released {
		return ErrStableResourceOwnershipTransferred
	}
	return fn()
}

func NewStableNamespaceToken(spec StableNamespaceSpec) (*StableNamespaceToken, error) {
	if spec.Parent == nil || spec.ParentGeneration == 0 || (spec.Operation != NamespaceCreate && spec.Operation != NamespaceRename) {
		return nil, fmt.Errorf("%w: incomplete namespace token", ErrInvalidStableResource)
	}
	identity, err := spec.Parent.StableIdentity()
	if err != nil {
		return nil, fmt.Errorf("stable namespace identity: %w", err)
	}
	if !identity.valid() {
		return nil, fmt.Errorf("%w: empty namespace identity", ErrInvalidStableResource)
	}
	generation, err := spec.Parent.StableGeneration()
	if err != nil {
		return nil, fmt.Errorf("stable namespace generation: %w", err)
	}
	if generation != spec.ParentGeneration {
		return nil, fmt.Errorf("%w: namespace generation=%d required=%d", ErrResourceConflict, generation, spec.ParentGeneration)
	}
	if err := spec.Parent.ValidateNamespacePersistence(); err != nil {
		if errors.Is(err, ErrNamespacePersistenceUnsupported) {
			return nil, err
		}
		return nil, fmt.Errorf("validate stable namespace persistence: %w", err)
	}
	if err := spec.Parent.Pin(); err != nil {
		return nil, fmt.Errorf("pin stable namespace: %w", err)
	}
	postIdentity, identityErr := spec.Parent.StableIdentity()
	postGeneration, generationErr := spec.Parent.StableGeneration()
	if identityErr != nil || generationErr != nil || postIdentity != identity || postGeneration != generation {
		_ = spec.Parent.Release()
		return nil, fmt.Errorf("%w: namespace identity/generation changed during registration", ErrResourceConflict)
	}
	owner := &stableNamespacePinOwner{handle: spec.Parent}
	return &StableNamespaceToken{
		operation: spec.Operation, parentDiagnosticPath: spec.ParentDiagnosticPath,
		parentIdentity: identity, parentGeneration: generation, parent: spec.Parent,
		owner: owner, lease: owner,
	}, nil
}

func (t *StableNamespaceToken) Identity() StableIdentity      { return t.parentIdentity }
func (t *StableNamespaceToken) Generation() uint64            { return t.parentGeneration }
func (t *StableNamespaceToken) Operation() NamespaceOperation { return t.operation }
func (t *StableNamespaceToken) Stable() bool                  { return t != nil && t.stable.Load() }

func (t *StableNamespaceToken) Sync() error {
	if t == nil {
		return nil
	}
	t.syncMu.Lock()
	defer t.syncMu.Unlock()
	if t.stable.Load() {
		return nil
	}
	err := t.lease.use(func() error {
		identity, err := t.parent.StableIdentity()
		if err != nil {
			return err
		}
		generation, err := t.parent.StableGeneration()
		if err != nil {
			return err
		}
		if identity != t.parentIdentity || generation != t.parentGeneration {
			return fmt.Errorf("%w: namespace identity/generation changed", ErrResourceConflict)
		}
		return t.parent.SyncNamespace()
	})
	if err != nil {
		return err
	}
	t.stable.Store(true)
	return nil
}

func (t *StableNamespaceToken) Release() error {
	if t == nil {
		return nil
	}
	stableOwnershipMu.Lock()
	owner := t.owner
	t.owner = nil
	stableOwnershipMu.Unlock()
	return owner.release()
}

type StableResourceSpec struct {
	Kind              ResourceKind
	LogicalNamespace  string
	ResourceID        string
	DiagnosticPath    string
	Generation        uint64
	Handle            StableResourceHandle
	RequiredFrontier  uint64
	RangeStart        uint64
	BackingKind       ResourceKind
	Digest            []byte
	Namespace         *StableNamespaceToken
	ReachabilityField string
}

type stablePinOwner struct {
	once     sync.Once
	mu       sync.RWMutex
	handle   StableResourceHandle
	released bool
	err      error
}

func (o *stablePinOwner) release() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.once.Do(func() {
		o.err = o.handle.Release()
		o.released = true
	})
	return o.err
}

func (o *stablePinOwner) use(frontier uint64, fn func(uint64) error) error {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.released {
		return ErrStableResourceOwnershipTransferred
	}
	return fn(frontier)
}

// StableResourceToken is immutable apart from its one transferable ownership
// cell. Identity and operations remain bound to the handle captured here.
type StableResourceToken struct {
	kind              ResourceKind
	logicalNamespace  string
	resourceID        string
	diagnosticPath    string
	generation        uint64
	identity          StableIdentity
	handle            StableResourceHandle
	requiredFrontier  uint64
	rangeStart        uint64
	backingKind       ResourceKind
	digest            []byte
	namespace         *StableNamespaceToken
	reachabilityField string

	owner *stablePinOwner
	lease *stablePinOwner
}

func NewStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	if !validResourceKind(spec.Kind) || spec.LogicalNamespace == "" || spec.ResourceID == "" || spec.Generation == 0 ||
		spec.Handle == nil || spec.ReachabilityField == "" || !validDiagnosticPath(spec.DiagnosticPath) {
		return nil, fmt.Errorf("%w: incomplete resource token", ErrInvalidStableResource)
	}
	if !resourceKindMutable(spec.Kind) && len(spec.Digest) == 0 {
		return nil, fmt.Errorf("%w: immutable resource or range %s requires a digest", ErrInvalidStableResource, spec.Kind)
	}
	if spec.RequiredFrontier == 0 || (resourceKindClass(spec.Kind) == resourceClassLogicalRange && spec.RangeStart >= spec.RequiredFrontier) {
		return nil, fmt.Errorf("%w: invalid required frontier/range", ErrInvalidStableResource)
	}
	if resourceKindClass(spec.Kind) == resourceClassLogicalRange {
		if !validResourceBackingKind(spec.Kind, spec.BackingKind) {
			return nil, fmt.Errorf("%w: logical range requires mutable backing kind", ErrInvalidStableResource)
		}
	} else if spec.BackingKind != "" {
		return nil, fmt.Errorf("%w: physical resource cannot declare a backing kind", ErrInvalidStableResource)
	}
	identity, err := spec.Handle.StableIdentity()
	if err != nil {
		return nil, fmt.Errorf("stable resource identity: %w", err)
	}
	if !identity.valid() {
		return nil, fmt.Errorf("%w: empty resource identity", ErrInvalidStableResource)
	}
	length, err := spec.Handle.StableLength()
	if err != nil {
		return nil, fmt.Errorf("stable resource length: %w", err)
	}
	if spec.RequiredFrontier > length {
		return nil, fmt.Errorf("%w: required=%d length=%d", ErrResourceFrontierBeyondLength, spec.RequiredFrontier, length)
	}
	if err := spec.Handle.Pin(); err != nil {
		return nil, fmt.Errorf("pin stable resource: %w", err)
	}
	postIdentity, identityErr := spec.Handle.StableIdentity()
	postLength, lengthErr := spec.Handle.StableLength()
	if identityErr != nil || lengthErr != nil || postIdentity != identity {
		_ = spec.Handle.Release()
		return nil, fmt.Errorf("%w: identity changed during registration", ErrResourceConflict)
	}
	if spec.RequiredFrontier > postLength {
		_ = spec.Handle.Release()
		return nil, fmt.Errorf("%w: required=%d length=%d after pin", ErrResourceFrontierBeyondLength, spec.RequiredFrontier, postLength)
	}
	owner := &stablePinOwner{handle: spec.Handle}
	return &StableResourceToken{
		kind: spec.Kind, logicalNamespace: spec.LogicalNamespace, resourceID: spec.ResourceID,
		diagnosticPath: spec.DiagnosticPath, generation: spec.Generation, identity: identity,
		handle: spec.Handle, requiredFrontier: spec.RequiredFrontier, rangeStart: spec.RangeStart,
		backingKind: spec.BackingKind,
		digest:      append([]byte(nil), spec.Digest...), namespace: spec.Namespace,
		reachabilityField: spec.ReachabilityField, owner: owner, lease: owner,
	}, nil
}

func validDiagnosticPath(path string) bool {
	if path == "" || filepath.IsAbs(path) {
		return false
	}
	clean := filepath.Clean(path)
	return clean != "." && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func validResourceKind(kind ResourceKind) bool {
	switch kind {
	case ResourceValueLogSegment, ResourceValueLogReference,
		ResourceOuterLeafSegment, ResourceOuterLeafReference, ResourceOuterLeafGeneration,
		ResourceOuterLeafManifest, ResourceDictionaryGeneration, ResourceTemplateCatalog,
		ResourceColumnAssetSegment, ResourceColumnAsset,
		ResourceTypedColumnValueAsset, ResourceTypedColumnCodeAsset, ResourceVectorGraphPack,
		ResourceCommandWALSegment, ResourceCommandWALReference, ResourceCommandWALExternalRID:
		return true
	default:
		return false
	}
}

func resourceKindMutable(kind ResourceKind) bool {
	switch kind {
	case ResourceValueLogSegment, ResourceOuterLeafSegment, ResourceColumnAssetSegment, ResourceCommandWALSegment:
		return true
	default:
		return false
	}
}

func resourceKindImmutable(kind ResourceKind) bool {
	return resourceKindClass(kind) == resourceClassImmutableFile
}

type resourceClass uint8

const (
	resourceClassMutableSegment resourceClass = iota + 1
	resourceClassImmutableFile
	resourceClassLogicalRange
)

func resourceKindClass(kind ResourceKind) resourceClass {
	if resourceKindMutable(kind) {
		return resourceClassMutableSegment
	}
	switch kind {
	case ResourceColumnAsset, ResourceTypedColumnValueAsset, ResourceTypedColumnCodeAsset,
		ResourceVectorGraphPack, ResourceValueLogReference, ResourceOuterLeafReference,
		ResourceCommandWALReference, ResourceCommandWALExternalRID,
		ResourceDictionaryGeneration, ResourceTemplateCatalog:
		return resourceClassLogicalRange
	default:
		return resourceClassImmutableFile
	}
}

func validResourceBackingKind(kind, backing ResourceKind) bool {
	switch kind {
	case ResourceValueLogReference, ResourceDictionaryGeneration, ResourceTemplateCatalog:
		return backing == ResourceValueLogSegment
	case ResourceOuterLeafReference:
		return backing == ResourceOuterLeafSegment
	case ResourceColumnAsset, ResourceTypedColumnValueAsset, ResourceTypedColumnCodeAsset, ResourceVectorGraphPack:
		return backing == ResourceColumnAssetSegment
	case ResourceCommandWALReference:
		return backing == ResourceCommandWALSegment
	case ResourceCommandWALExternalRID:
		return backing == ResourceValueLogSegment || backing == ResourceOuterLeafSegment
	default:
		return false
	}
}

func (t *StableResourceToken) Kind() ResourceKind        { return t.kind }
func (t *StableResourceToken) LogicalNamespace() string  { return t.logicalNamespace }
func (t *StableResourceToken) ResourceID() string        { return t.resourceID }
func (t *StableResourceToken) DiagnosticPath() string    { return t.diagnosticPath }
func (t *StableResourceToken) Generation() uint64        { return t.generation }
func (t *StableResourceToken) Identity() StableIdentity  { return t.identity }
func (t *StableResourceToken) RequiredFrontier() uint64  { return t.requiredFrontier }
func (t *StableResourceToken) RangeStart() uint64        { return t.rangeStart }
func (t *StableResourceToken) BackingKind() ResourceKind { return t.backingKind }
func (t *StableResourceToken) Digest() []byte            { return append([]byte(nil), t.digest...) }
func (t *StableResourceToken) ReachabilityField() string { return t.reachabilityField }
func (t *StableResourceToken) NamespaceStable() bool {
	return t.namespace == nil || t.namespace.Stable()
}

func (t *StableResourceToken) FlushThrough() error {
	return t.lease.use(t.requiredFrontier, t.handle.FlushThrough)
}

func (t *StableResourceToken) SyncThrough() error {
	return t.lease.use(t.requiredFrontier, t.handle.SyncThrough)
}

func (t *StableResourceToken) SyncNamespace() error {
	if t.namespace == nil {
		return nil
	}
	return t.namespace.Sync()
}

func (t *StableResourceToken) Release() error {
	stableOwnershipMu.Lock()
	owner := t.owner
	t.owner = nil
	var namespaceOwner *stableNamespacePinOwner
	if t.namespace != nil {
		namespaceOwner = t.namespace.owner
		t.namespace.owner = nil
	}
	stableOwnershipMu.Unlock()
	return errors.Join(owner.release(), namespaceOwner.release())
}

type stableResourceEntry struct {
	token  *StableResourceToken
	owners []*stablePinOwner
}

// StableResourceSet is an immutable normalized token union with one
// transferable release owner.
type StableResourceSet struct {
	entries         []stableResourceEntry
	namespaceOwners []*stableNamespacePinOwner
	owned           bool
	released        bool
}

func NewStableResourceSet(tokens ...*StableResourceToken) (*StableResourceSet, error) {
	entries, err := normalizeStableResourceTokens(tokens)
	if err != nil {
		return nil, err
	}
	stableOwnershipMu.Lock()
	defer stableOwnershipMu.Unlock()
	uniqueNamespaces := make(map[*StableNamespaceToken]struct{})
	for i := range entries {
		first := entries[i].firstSource
		if first.owner == nil {
			return nil, ErrStableResourceOwnershipTransferred
		}
		if first.namespace != nil {
			uniqueNamespaces[first.namespace] = struct{}{}
		}
		for _, token := range entries[i].sourceTokens {
			if token.owner == nil {
				return nil, ErrStableResourceOwnershipTransferred
			}
			if token.namespace != nil {
				uniqueNamespaces[token.namespace] = struct{}{}
			}
		}
	}
	for namespace := range uniqueNamespaces {
		if namespace.owner == nil {
			return nil, ErrStableResourceOwnershipTransferred
		}
	}
	for i := range entries {
		first := entries[i].firstSource
		entries[i].owners = append(entries[i].owners, first.owner)
		first.owner = nil
		for _, token := range entries[i].sourceTokens {
			owner := token.owner
			token.owner = nil
			entries[i].owners = append(entries[i].owners, owner)
		}
	}
	namespaceOwners := make([]*stableNamespacePinOwner, 0, len(uniqueNamespaces))
	for namespace := range uniqueNamespaces {
		namespaceOwners = append(namespaceOwners, namespace.owner)
		namespace.owner = nil
	}
	out := make([]stableResourceEntry, len(entries))
	for i := range entries {
		out[i] = stableResourceEntry{token: entries[i].token, owners: entries[i].owners}
	}
	return &StableResourceSet{entries: out, namespaceOwners: namespaceOwners, owned: true}, nil
}

type normalizedResourceEntry struct {
	token        *StableResourceToken
	firstSource  *StableResourceToken
	sourceTokens []*StableResourceToken
	owners       []*stablePinOwner
}

func appendNormalizedResourceSource(entry *normalizedResourceEntry, token *StableResourceToken) {
	entry.sourceTokens = append(entry.sourceTokens, token)
}

func normalizeStableResourceTokens(tokens []*StableResourceToken) ([]normalizedResourceEntry, error) {
	entries := make([]normalizedResourceEntry, 0, len(tokens))
	logical := make(map[stableResourceLogicalKey]*StableResourceToken, len(tokens))
	logicalEntries := make(map[stableResourceLogicalKey]int, len(tokens))
	physical := make(map[stableResourcePhysicalKey]int, len(tokens))
	for _, token := range tokens {
		if token == nil {
			return nil, fmt.Errorf("%w: nil token", ErrInvalidStableResource)
		}
		logicalKey := stableResourceLogicalKeyOf(token)
		if existing := logical[logicalKey]; existing != nil {
			if existing.identity != token.identity ||
				(resourceKindClass(token.kind) == resourceClassLogicalRange && !stableResourceMetadataCompatible(existing, token)) {
				return nil, fmt.Errorf("%w: logical resource %s/%s changed contract", ErrResourceConflict, token.logicalNamespace, token.resourceID)
			}
		} else {
			logical[logicalKey] = token
		}
		if resourceKindClass(token.kind) == resourceClassLogicalRange {
			if i, ok := logicalEntries[logicalKey]; ok {
				entry := &entries[i]
				namespace, err := mergeNamespaceToken(entry.token.namespace, token.namespace)
				if err != nil {
					return nil, fmt.Errorf("%w: logical resource %s/%s: %v", ErrResourceConflict, token.logicalNamespace, token.resourceID, err)
				}
				representative := entry.token
				if representative.namespace != namespace {
					clone := *representative
					clone.namespace = namespace
					clone.owner = nil
					representative = &clone
				}
				appendNormalizedResourceSource(entry, token)
				entry.token = representative
			} else {
				logicalEntries[logicalKey] = len(entries)
				entries = append(entries, normalizedResourceEntry{token: token, firstSource: token})
			}
			continue
		}
		physicalKey := stableResourcePhysicalKeyOf(token)
		if i, ok := physical[physicalKey]; ok {
			entry := &entries[i]
			if !stableResourcePhysicalContractCompatible(entry.token, token) {
				return nil, fmt.Errorf("%w: kind=%s identity=%+v", ErrResourceConflict, token.kind, token.identity)
			}
			namespace, err := mergeNamespaceToken(entry.token.namespace, token.namespace)
			if err != nil {
				return nil, fmt.Errorf("%w: kind=%s identity=%+v: %v", ErrResourceConflict, token.kind, token.identity, err)
			}
			representative := entry.token
			if resourceKindMutable(token.kind) {
				if token.requiredFrontier > entry.token.requiredFrontier {
					representative = token
				}
			} else if resourceKindClass(token.kind) == resourceClassImmutableFile && token.requiredFrontier != entry.token.requiredFrontier {
				return nil, fmt.Errorf("%w: immutable resource frontier changed", ErrResourceConflict)
			}
			if representative.namespace != namespace {
				clone := *representative
				clone.namespace = namespace
				clone.owner = nil
				representative = &clone
			}
			appendNormalizedResourceSource(entry, token)
			entry.token = representative
		} else {
			physical[physicalKey] = len(entries)
			entries = append(entries, normalizedResourceEntry{token: token, firstSource: token})
		}
	}
	for _, entry := range entries {
		token := entry.token
		if resourceKindClass(token.kind) != resourceClassLogicalRange {
			continue
		}
		backingKey := stableResourcePhysicalKey{
			kind: token.backingKind, identity: token.identity, generation: token.generation,
			class: resourceClassMutableSegment,
		}
		backingIndex, found := physical[backingKey]
		if !found || entries[backingIndex].token.requiredFrontier < token.requiredFrontier {
			return nil, fmt.Errorf("%w: logical resource %s/%s lacks %s backing through %d", ErrMissingResourceDependency, token.logicalNamespace, token.resourceID, token.backingKind, token.requiredFrontier)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return stableResourceLess(entries[i].token, entries[j].token) })
	return entries, nil
}

type stableResourceLogicalKey struct {
	kind             ResourceKind
	logicalNamespace string
	resourceID       string
	generation       uint64
}

func stableResourceLogicalKeyOf(token *StableResourceToken) stableResourceLogicalKey {
	return stableResourceLogicalKey{token.kind, token.logicalNamespace, token.resourceID, token.generation}
}

type stableResourcePhysicalKey struct {
	kind       ResourceKind
	identity   StableIdentity
	generation uint64
	rangeStart uint64
	frontier   uint64
	class      resourceClass
}

func stableResourcePhysicalKeyOf(token *StableResourceToken) stableResourcePhysicalKey {
	key := stableResourcePhysicalKey{kind: token.kind, identity: token.identity, class: resourceKindClass(token.kind)}
	switch key.class {
	case resourceClassMutableSegment:
		key.generation = token.generation
	case resourceClassLogicalRange:
		key.generation = token.generation
		key.rangeStart = token.rangeStart
		key.frontier = token.requiredFrontier
	}
	return key
}

func stableResourceLess(a, b *StableResourceToken) bool {
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	if a.identity.Device != b.identity.Device {
		return a.identity.Device < b.identity.Device
	}
	if a.identity.File != b.identity.File {
		return a.identity.File < b.identity.File
	}
	if a.generation != b.generation {
		return a.generation < b.generation
	}
	if a.rangeStart != b.rangeStart {
		return a.rangeStart < b.rangeStart
	}
	if a.requiredFrontier != b.requiredFrontier {
		return a.requiredFrontier < b.requiredFrontier
	}
	if a.logicalNamespace != b.logicalNamespace {
		return a.logicalNamespace < b.logicalNamespace
	}
	return a.resourceID < b.resourceID
}

func stableResourceMetadataCompatible(a, b *StableResourceToken) bool {
	return a.logicalNamespace == b.logicalNamespace && a.resourceID == b.resourceID &&
		a.generation == b.generation && a.rangeStart == b.rangeStart && a.requiredFrontier == b.requiredFrontier &&
		a.backingKind == b.backingKind && a.reachabilityField == b.reachabilityField &&
		bytes.Equal(a.digest, b.digest)
}

func stableResourcePhysicalContractCompatible(a, b *StableResourceToken) bool {
	if resourceKindMutable(a.kind) {
		return true
	}
	return stableResourceMetadataCompatible(a, b)
}

func mergeNamespaceToken(a, b *StableNamespaceToken) (*StableNamespaceToken, error) {
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}
	if !sameNamespaceToken(a, b) {
		return nil, errors.New("contradictory namespace obligation")
	}
	return a, nil
}

func sameNamespaceToken(a, b *StableNamespaceToken) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.operation == b.operation && a.parentIdentity == b.parentIdentity && a.parentGeneration == b.parentGeneration
}

func (s *StableResourceSet) Tokens() []*StableResourceToken {
	if s == nil {
		return nil
	}
	out := make([]*StableResourceToken, len(s.entries))
	for i := range s.entries {
		out[i] = s.entries[i].token
	}
	return out
}

func (s *StableResourceSet) SyncThrough() error {
	return s.syncPhysical(func(token *StableResourceToken) error { return token.SyncThrough() })
}

func (s *StableResourceSet) FlushThrough() error {
	return s.syncPhysical(func(token *StableResourceToken) error { return token.FlushThrough() })
}

func (s *StableResourceSet) SyncNamespaces() error {
	type namespaceKey struct {
		identity   StableIdentity
		generation uint64
		operation  NamespaceOperation
	}
	groups := make(map[namespaceKey][]*StableNamespaceToken)
	for _, entry := range s.entries {
		if entry.token.namespace == nil {
			continue
		}
		namespace := entry.token.namespace
		key := namespaceKey{namespace.parentIdentity, namespace.parentGeneration, namespace.operation}
		groups[key] = append(groups[key], namespace)
	}
	stableNamespaceSyncMu.Lock()
	defer stableNamespaceSyncMu.Unlock()
	keys := make([]namespaceKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].identity.Device != keys[j].identity.Device {
			return keys[i].identity.Device < keys[j].identity.Device
		}
		if keys[i].identity.File != keys[j].identity.File {
			return keys[i].identity.File < keys[j].identity.File
		}
		if keys[i].generation != keys[j].generation {
			return keys[i].generation < keys[j].generation
		}
		return keys[i].operation < keys[j].operation
	})
	for _, key := range keys {
		group := groups[key]
		stable := false
		for _, token := range group {
			stable = stable || token.Stable()
		}
		if !stable {
			if err := group[0].Sync(); err != nil {
				return err
			}
		}
		for _, token := range group {
			token.stable.Store(true)
		}
	}
	return nil
}

func (s *StableResourceSet) syncPhysical(op func(*StableResourceToken) error) error {
	// Logical range obligations may share one append file. Sync each physical
	// identity/generation once at the greatest referenced byte frontier.
	type physicalKey struct {
		kind       ResourceKind
		identity   StableIdentity
		generation uint64
	}
	selected := make(map[physicalKey]*StableResourceToken)
	for _, entry := range s.entries {
		token := entry.token
		if resourceKindClass(token.kind) == resourceClassLogicalRange {
			// The matching physical token is the producer-owned sync adapter.
			// Logical references preserve authority and digest obligations only.
			continue
		}
		key := physicalKey{token.kind, token.identity, token.generation}
		if current := selected[key]; current == nil || token.requiredFrontier > current.requiredFrontier {
			selected[key] = token
		}
	}
	keys := make([]physicalKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		if keys[i].identity.Device != keys[j].identity.Device {
			return keys[i].identity.Device < keys[j].identity.Device
		}
		if keys[i].identity.File != keys[j].identity.File {
			return keys[i].identity.File < keys[j].identity.File
		}
		return keys[i].generation < keys[j].generation
	})
	for _, key := range keys {
		token := selected[key]
		if err := op(token); err != nil {
			return err
		}
	}
	return nil
}

func (s *StableResourceSet) ValidateReachabilityFields(required []string) error {
	got := make(map[string]struct{}, len(s.entries))
	for _, entry := range s.entries {
		got[entry.token.reachabilityField] = struct{}{}
	}
	for _, field := range required {
		if _, ok := got[field]; !ok {
			return fmt.Errorf("%w: %s", ErrMissingResourceDependency, field)
		}
	}
	return nil
}

func (s *StableResourceSet) Release() error {
	if s == nil {
		return nil
	}
	stableOwnershipMu.Lock()
	if !s.owned || s.released {
		stableOwnershipMu.Unlock()
		return nil
	}
	s.released = true
	owners := make([]*stablePinOwner, 0)
	for i := range s.entries {
		owners = append(owners, s.entries[i].owners...)
	}
	namespaceOwners := append([]*stableNamespacePinOwner(nil), s.namespaceOwners...)
	stableOwnershipMu.Unlock()
	var err error
	for _, owner := range owners {
		err = errors.Join(err, owner.release())
	}
	for _, owner := range namespaceOwners {
		err = errors.Join(err, owner.release())
	}
	return err
}

// TransferUnionStableResourceSets atomically transfers every source owner into
// one deterministic union. Source Release calls become inert.
func TransferUnionStableResourceSets(sets ...*StableResourceSet) (*StableResourceSet, error) {
	uniqueSets := make([]*StableResourceSet, 0, len(sets))
	seenSets := make(map[*StableResourceSet]struct{}, len(sets))
	for _, set := range sets {
		if set == nil {
			continue
		}
		if _, ok := seenSets[set]; ok {
			continue
		}
		seenSets[set] = struct{}{}
		uniqueSets = append(uniqueSets, set)
	}
	allTokens := make([]*StableResourceToken, 0)
	for _, set := range uniqueSets {
		for _, entry := range set.entries {
			allTokens = append(allTokens, entry.token)
		}
	}
	normalized, err := normalizeStableResourceTokens(allTokens)
	if err != nil {
		return nil, err
	}
	stableOwnershipMu.Lock()
	defer stableOwnershipMu.Unlock()
	for _, set := range uniqueSets {
		if !set.owned || set.released {
			return nil, ErrStableResourceOwnershipTransferred
		}
	}
	ownersByToken := make(map[*StableResourceToken][]*stablePinOwner)
	namespaceOwners := make([]*stableNamespacePinOwner, 0)
	for _, set := range uniqueSets {
		for _, entry := range set.entries {
			ownersByToken[entry.token] = append(ownersByToken[entry.token], entry.owners...)
		}
		namespaceOwners = append(namespaceOwners, set.namespaceOwners...)
	}
	outEntries := make([]stableResourceEntry, len(normalized))
	for i := range normalized {
		owners := make([]*stablePinOwner, 0)
		owners = append(owners, ownersByToken[normalized[i].firstSource]...)
		for _, source := range normalized[i].sourceTokens {
			owners = append(owners, ownersByToken[source]...)
		}
		outEntries[i] = stableResourceEntry{token: normalized[i].token, owners: owners}
	}
	for _, set := range uniqueSets {
		set.owned = false
	}
	return &StableResourceSet{entries: outEntries, namespaceOwners: namespaceOwners, owned: true}, nil
}

func (s *StableResourceSet) union(other immutableExtension) immutableExtension {
	rhs, ok := other.(*StableResourceSet)
	if !ok {
		panic("rootpublication: resource extension type mismatch")
	}
	union, err := borrowUnionStableResourceSets(s, rhs)
	if err != nil {
		panic(err)
	}
	return union
}

func borrowUnionStableResourceSets(sets ...*StableResourceSet) (*StableResourceSet, error) {
	allTokens := make([]*StableResourceToken, 0)
	for _, set := range sets {
		if set == nil {
			continue
		}
		for _, entry := range set.entries {
			allTokens = append(allTokens, entry.token)
		}
	}
	normalized, err := normalizeStableResourceTokens(allTokens)
	if err != nil {
		return nil, err
	}
	entries := make([]stableResourceEntry, len(normalized))
	for i := range normalized {
		entries[i] = stableResourceEntry{token: normalized[i].token}
	}
	return &StableResourceSet{entries: entries}, nil
}
