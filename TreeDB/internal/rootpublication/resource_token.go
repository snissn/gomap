package rootpublication

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrStableIdentityUnsupported       = errors.New("stable resource identity unsupported")
	ErrNamespacePersistenceUnsupported = errors.New("namespace persistence unsupported")
	ErrNamespaceUnstable               = errors.New("stable resource namespace unresolved")
	ErrFrontierBeyondResource          = errors.New("stable resource frontier beyond file length")
	ErrResourceConflict                = errors.New("stable resource conflict")
	ErrResourcePinned                  = errors.New("stable resource identity pinned")
	ErrUnresolvedResource              = errors.New("stable resource dependency unresolved")
	ErrResourceOwnership               = errors.New("stable resource ownership violation")
	ErrRecoveryHandoffUnavailable      = errors.New("stable resource recovery handoff unavailable")
)

// ResourceKind identifies the physical durability domain of a token. The
// string values are also used by the checked inventory and metrics output.
type ResourceKind string

const (
	ResourceIndex                 ResourceKind = "index"
	ResourceMeta                  ResourceKind = "meta"
	ResourceValueLog              ResourceKind = "value-log"
	ResourceOuterLeafLog          ResourceKind = "outer-leaf-log"
	ResourceOuterLeafManifest     ResourceKind = "outer-leaf-manifest"
	ResourceOuterLeafPack         ResourceKind = "outer-leaf-pack"
	ResourceDictionary            ResourceKind = "dictionary"
	ResourceTemplate              ResourceKind = "template"
	ResourceCollectionRoot        ResourceKind = "collection-root"
	ResourceColumnAsset           ResourceKind = "column-asset"
	ResourceTypedColumnAsset      ResourceKind = "typed-column-asset"
	ResourceVectorGraphPack       ResourceKind = "vector-graph-pack"
	ResourceLegacyVectorSnapshot  ResourceKind = "legacy-vector-snapshot"
	ResourceTextAsset             ResourceKind = "text-asset"
	ResourceCommandWAL            ResourceKind = "command-wal"
	ResourceCommandWALExternalRID ResourceKind = "command-wal-external-rid"
	ResourceQueryReadyAsset       ResourceKind = "query-ready-asset"
	ResourceSeparateDurability    ResourceKind = "separate-durability-domain"
	ResourceLegacyTreeDBField     ResourceKind = "legacy-treedb-field"
)

// ReachabilityField names the root, catalog, or frame field that makes a
// resource reachable. It is deliberately independent from a diagnostic path.
type ReachabilityField string

const (
	ReachabilityIndexFile                  ReachabilityField = "meta.index_file"
	ReachabilityMetaPage                   ReachabilityField = "meta.target_page"
	ReachabilityUserRoot                   ReachabilityField = "meta.user_root_page_id"
	ReachabilitySystemRoot                 ReachabilityField = "meta.system_root_page_id"
	ReachabilityFreelist                   ReachabilityField = "meta.freelist_head_id"
	ReachabilityValueLogPointer            ReachabilityField = "leaf.value_ptr"
	ReachabilityOuterLeafRawPointer        ReachabilityField = "leaf.outer_raw_ref"
	ReachabilityOuterLeafPackedPointer     ReachabilityField = "leaf.outer_packed_ref"
	ReachabilityOuterLeafGeneration        ReachabilityField = "system.outer_leaf_generation_manifest"
	ReachabilityDictionaryGeneration       ReachabilityField = "frame.dictionary_generation"
	ReachabilityTemplateGeneration         ReachabilityField = "frame.template_generation"
	ReachabilityCollectionSystemRoot       ReachabilityField = "system.collection_root_descriptor"
	ReachabilityCollectionPrimaryRoot      ReachabilityField = "collection.primary_root"
	ReachabilityCollectionTemplateRoot     ReachabilityField = "collection.template_root"
	ReachabilityCollectionIndexStateRoot   ReachabilityField = "collection.index_state_root"
	ReachabilityCollectionColumnRoot       ReachabilityField = "collection.column_manifest_root"
	ReachabilityCollectionSecondaryRoot    ReachabilityField = "collection.secondary_root"
	ReachabilityCollectionVectorRoot       ReachabilityField = "collection.vector_root"
	ReachabilityCollectionTextDictionary   ReachabilityField = "collection.text_dictionary_root"
	ReachabilityCollectionTextPosting      ReachabilityField = "collection.text_posting_root"
	ReachabilityCollectionTextPosition     ReachabilityField = "collection.text_position_root"
	ReachabilityColumnManifest             ReachabilityField = "column.manifest_asset_ref"
	ReachabilityTypedColumnMultipart       ReachabilityField = "column.typed_multipart_ref"
	ReachabilityTypedColumnValue           ReachabilityField = "column.typed_value_ref"
	ReachabilityTypedColumnCode            ReachabilityField = "column.typed_code_ref"
	ReachabilityHNSWSearchPack             ReachabilityField = "column.hnsw_search_pack_ref"
	ReachabilityVectorGraphPack            ReachabilityField = "column.vector_graph_pack_ref"
	ReachabilityLegacyVectorSnapshot       ReachabilityField = "collection.legacy_vector_manifest"
	ReachabilityCommandWALActive           ReachabilityField = "command_wal.active_segment"
	ReachabilityCommandWALRotated          ReachabilityField = "command_wal.rotated_segment"
	ReachabilityCommandWALExternalRIDFence ReachabilityField = "command_wal_v2.external_rid_fence"
	ReachabilityQueryReadyBase             ReachabilityField = "column.query_ready_base_v1"
	ReachabilityQueryReadyDelta            ReachabilityField = "column.query_ready_delta_v1"
	ReachabilityQueryReadyConsolidatedBase ReachabilityField = "column.query_ready_consolidated_base_v1"
	ReachabilityLegacyActiveSlab           ReachabilityField = "meta.legacy_active_slab"
	ReachabilityRaftSnapshot               ReachabilityField = "raft.snapshot_manifest"
)

type NamespaceOperation uint8

const (
	NamespaceNone NamespaceOperation = iota
	NamespaceCreate
	NamespaceRename
)

func (operation NamespaceOperation) String() string {
	switch operation {
	case NamespaceNone:
		return "none"
	case NamespaceCreate:
		return "create"
	case NamespaceRename:
		return "rename"
	default:
		return fmt.Sprintf("unknown(%d)", operation)
	}
}

// StableIdentity is captured from an already-open handle. ObjectID is a
// device/inode pair on Unix and a native file ID on Windows. Generation is the
// producer's immutable logical generation and prevents identity reuse.
type StableIdentity struct {
	Platform   string
	VolumeID   uint64
	ObjectID   [16]byte
	Generation uint64
}

func (identity StableIdentity) valid() bool {
	return identity.Platform != "" && identity.ObjectID != [16]byte{}
}

// DurableFrontier binds an append byte frontier and, for command-WAL V2, the
// canonical sparse external-RID set. A maximum RID by itself is not proof of a
// sparse set.
type DurableFrontier struct {
	Bytes        uint64
	MaxLSN       uint64
	MaxRID       uint64
	RIDSetDigest [32]byte
	RIDCount     uint64
	RIDMin       uint64
	RIDMax       uint64
}

func NewRIDFrontier(rids []uint64) DurableFrontier {
	ordered := append([]uint64(nil), rids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unique := ordered[:0]
	for _, rid := range ordered {
		if len(unique) == 0 || unique[len(unique)-1] != rid {
			unique = append(unique, rid)
		}
	}
	frontier := DurableFrontier{RIDCount: uint64(len(unique))}
	if len(unique) == 0 {
		return frontier
	}
	frontier.RIDMin = unique[0]
	frontier.RIDMax = unique[len(unique)-1]
	frontier.MaxRID = frontier.RIDMax
	raw := make([]byte, 8*len(unique))
	for i, rid := range unique {
		binary.LittleEndian.PutUint64(raw[8*i:], rid)
	}
	frontier.RIDSetDigest = sha256.Sum256(raw)
	return frontier
}

type ResourceOwnerState uint8

const (
	ResourceOwnerToken ResourceOwnerState = iota + 1
	ResourceOwnerBuilder
	ResourceOwnerCandidate
	ResourceOwnerCoordinator
	ResourceOwnerRecovery
	ResourceOwnerTransferred
	ResourceOwnerView
	ResourceOwnerReleased
)

type resourceOperation func(*os.File, DurableFrontier) error

// StableResourceSpec is consumed by NewStableResourceToken. File is duplicated
// immediately; later operations never reopen DiagnosticPath.
type StableResourceSpec struct {
	Kind           ResourceKind
	LogicalLane    string
	ResourceID     string
	Generation     uint64
	DiagnosticPath string
	File           *os.File
	Frontier       DurableFrontier
	Digest         [32]byte
	Reachability   ReachabilityField
	Namespace      *StableNamespaceToken
	FlushThrough   resourceOperation
	SyncThrough    resourceOperation
	OnRelease      func()

	// StableIdentityOverride exists for deterministic platform-adapter and
	// conflict tests. Production producers leave it zero and use handle identity.
	StableIdentityOverride StableIdentity
}

type resourceTokenMetrics struct {
	flushes         atomic.Uint64
	flushNanos      atomic.Uint64
	syncs           atomic.Uint64
	syncNanos       atomic.Uint64
	registeredNanos int64
}

type StableResourceToken struct {
	kind           ResourceKind
	logicalLane    string
	resourceID     string
	generation     uint64
	diagnosticPath string
	identity       StableIdentity
	frontier       DurableFrontier
	digest         [32]byte
	reachability   ReachabilityField
	namespace      *StableNamespaceToken
	pinned         *os.File
	flush          resourceOperation
	sync           resourceOperation
	onRelease      func()
	owner          atomic.Uint32
	released       atomic.Bool
	metrics        resourceTokenMetrics
}

func NewStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	if spec.Kind == "" || spec.ResourceID == "" || spec.Generation == 0 || spec.Reachability == "" || spec.File == nil {
		return nil, fmt.Errorf("%w: incomplete resource registration", ErrUnresolvedResource)
	}
	if err := validateDiagnosticPath(spec.DiagnosticPath); err != nil {
		return nil, err
	}
	pinned, err := duplicateStableFile(spec.File)
	if err != nil {
		return nil, fmt.Errorf("duplicate stable resource handle: %w", err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = pinned.Close()
		}
	}()
	stat, err := pinned.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat pinned resource: %w", err)
	}
	if spec.Frontier.Bytes > uint64(stat.Size()) {
		return nil, fmt.Errorf("%w: required=%d length=%d", ErrFrontierBeyondResource, spec.Frontier.Bytes, stat.Size())
	}
	identity := spec.StableIdentityOverride
	if !identity.valid() {
		identity, err = stableIdentityFromFile(pinned)
		if err != nil {
			return nil, err
		}
	}
	if identity.Generation != 0 && identity.Generation != spec.Generation {
		return nil, fmt.Errorf("%w: identity generation %d differs from resource generation %d", ErrResourceConflict, identity.Generation, spec.Generation)
	}
	identity.Generation = spec.Generation
	flush := spec.FlushThrough
	if flush == nil {
		flush = func(file *os.File, _ DurableFrontier) error { return file.Sync() }
	}
	syncThrough := spec.SyncThrough
	if syncThrough == nil {
		syncThrough = func(file *os.File, _ DurableFrontier) error { return file.Sync() }
	}
	token := &StableResourceToken{
		kind: spec.Kind, logicalLane: spec.LogicalLane, resourceID: spec.ResourceID,
		generation: spec.Generation, diagnosticPath: filepath.ToSlash(spec.DiagnosticPath),
		identity: identity, frontier: spec.Frontier, digest: spec.Digest,
		reachability: spec.Reachability, namespace: spec.Namespace, pinned: pinned,
		flush: flush, sync: syncThrough, onRelease: spec.OnRelease,
	}
	token.owner.Store(uint32(ResourceOwnerToken))
	token.metrics.registeredNanos = time.Now().UnixNano()
	if token.namespace != nil {
		if err := token.namespace.retain(); err != nil {
			return nil, err
		}
	}
	closeOnError = false
	return token, nil
}

func validateDiagnosticPath(path string) error {
	if path == "" || filepath.IsAbs(path) {
		return fmt.Errorf("%w: diagnostic path must be DB-relative", ErrUnresolvedResource)
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: diagnostic path escapes DB root", ErrUnresolvedResource)
	}
	return nil
}

func (token *StableResourceToken) Kind() ResourceKind               { return token.kind }
func (token *StableResourceToken) LogicalLane() string              { return token.logicalLane }
func (token *StableResourceToken) ResourceID() string               { return token.resourceID }
func (token *StableResourceToken) Generation() uint64               { return token.generation }
func (token *StableResourceToken) DiagnosticPath() string           { return token.diagnosticPath }
func (token *StableResourceToken) Identity() StableIdentity         { return token.identity }
func (token *StableResourceToken) Frontier() DurableFrontier        { return token.frontier }
func (token *StableResourceToken) Digest() [32]byte                 { return token.digest }
func (token *StableResourceToken) Reachability() ReachabilityField  { return token.reachability }
func (token *StableResourceToken) Namespace() *StableNamespaceToken { return token.namespace }

func (token *StableResourceToken) FlushThrough() error {
	return token.flushThrough(token.frontier)
}

func (token *StableResourceToken) flushThrough(frontier DurableFrontier) error {
	if token == nil || token.released.Load() {
		return ErrResourceOwnership
	}
	started := time.Now()
	err := token.flush(token.pinned, frontier)
	token.metrics.flushes.Add(1)
	token.metrics.flushNanos.Add(uint64(time.Since(started)))
	return err
}

func (token *StableResourceToken) SyncThrough() error {
	return token.syncThrough(token.frontier)
}

func (token *StableResourceToken) syncThrough(frontier DurableFrontier) error {
	if token == nil || token.released.Load() {
		return ErrResourceOwnership
	}
	started := time.Now()
	err := token.sync(token.pinned, frontier)
	token.metrics.syncs.Add(1)
	token.metrics.syncNanos.Add(uint64(time.Since(started)))
	return err
}

func (token *StableResourceToken) ReadAt(dst []byte, offset int64) (int, error) {
	if token == nil || token.released.Load() {
		return 0, ErrResourceOwnership
	}
	return token.pinned.ReadAt(dst, offset)
}

func (token *StableResourceToken) Release() {
	if token == nil || !token.owner.CompareAndSwap(uint32(ResourceOwnerToken), uint32(ResourceOwnerReleased)) {
		return
	}
	token.releasePinned()
}

func (token *StableResourceToken) claim(owner ResourceOwnerState) error {
	if token == nil || !token.owner.CompareAndSwap(uint32(ResourceOwnerToken), uint32(owner)) {
		return ErrResourceOwnership
	}
	return nil
}

func (token *StableResourceToken) transfer(from, to ResourceOwnerState) error {
	if token == nil || !token.owner.CompareAndSwap(uint32(from), uint32(to)) {
		return ErrResourceOwnership
	}
	return nil
}

func (token *StableResourceToken) releaseFrom(owner ResourceOwnerState) {
	if token == nil || !token.owner.CompareAndSwap(uint32(owner), uint32(ResourceOwnerReleased)) {
		return
	}
	token.releasePinned()
}

func (token *StableResourceToken) releasePinned() {
	if token.released.Swap(true) {
		return
	}
	_ = token.pinned.Close()
	if token.namespace != nil {
		token.namespace.release()
	}
	if token.onRelease != nil {
		token.onRelease()
	}
}

func (token *StableResourceToken) logicalKey() string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%020d", token.kind, token.logicalLane, token.resourceID, token.generation)
}

func (token *StableResourceToken) identityKey() string {
	return fmt.Sprintf("%s\x00%s\x00%020d\x00%x\x00%020d", token.kind, token.identity.Platform,
		token.identity.VolumeID, token.identity.ObjectID, token.generation)
}

func (token *StableResourceToken) namespaceCompatible(other *StableResourceToken) bool {
	if token.namespace == nil || other.namespace == nil {
		// A later range in an already-created append-only object legitimately has
		// no new namespace operation. The coalesced entry retains whichever token
		// carries the creation obligation.
		return true
	}
	return token.namespace.compatible(other.namespace)
}

func frontierCompatible(older, newer DurableFrontier) bool {
	if older.RIDCount == 0 && newer.RIDCount == 0 {
		return true
	}
	return older.RIDCount == newer.RIDCount && older.RIDMin == newer.RIDMin && older.RIDMax == newer.RIDMax &&
		older.RIDSetDigest == newer.RIDSetDigest
}

func maxFrontier(older, newer DurableFrontier) DurableFrontier {
	out := older
	if newer.Bytes > out.Bytes {
		out.Bytes = newer.Bytes
	}
	if newer.MaxRID > out.MaxRID {
		out.MaxRID = newer.MaxRID
	}
	if newer.MaxLSN > out.MaxLSN {
		out.MaxLSN = newer.MaxLSN
	}
	if out.RIDCount == 0 && newer.RIDCount != 0 {
		out.RIDSetDigest, out.RIDCount, out.RIDMin, out.RIDMax = newer.RIDSetDigest, newer.RIDCount, newer.RIDMin, newer.RIDMax
	}
	return out
}

type namespacePersistenceAdapter interface {
	Identity(*os.File) (StableIdentity, error)
	Sync(*os.File) error
}

type nativeNamespaceAdapter struct{}

func (nativeNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (nativeNamespaceAdapter) Sync(file *os.File) error {
	return syncStableNamespace(file)
}

type StableNamespaceSpec struct {
	Parent           *os.File
	ParentGeneration uint64
	Operation        NamespaceOperation
	OldName          string
	NewName          string
	DiagnosticPath   string
}

type StableNamespaceToken struct {
	parent         *os.File
	parentIdentity StableIdentity
	operation      NamespaceOperation
	oldName        string
	newName        string
	diagnosticPath string
	adapter        namespacePersistenceAdapter
	state          atomic.Uint32
	refs           atomic.Int64
	released       atomic.Bool
	mu             sync.Mutex
	stabilizeErr   error
	syncs          atomic.Uint64
	syncNanos      atomic.Uint64
}

const (
	namespacePending uint32 = iota
	namespaceStable
	namespaceFailed
)

func NewStableNamespaceToken(spec StableNamespaceSpec) (*StableNamespaceToken, error) {
	return newStableNamespaceToken(spec, nativeNamespaceAdapter{})
}

func newStableNamespaceToken(spec StableNamespaceSpec, adapter namespacePersistenceAdapter) (*StableNamespaceToken, error) {
	if spec.Parent == nil || spec.ParentGeneration == 0 || spec.Operation == NamespaceNone || spec.NewName == "" || adapter == nil {
		return nil, fmt.Errorf("%w: incomplete namespace registration", ErrUnresolvedResource)
	}
	if spec.Operation == NamespaceRename && spec.OldName == "" {
		return nil, fmt.Errorf("%w: rename missing old name", ErrUnresolvedResource)
	}
	if err := validateDiagnosticPath(spec.DiagnosticPath); err != nil {
		return nil, err
	}
	pinned, err := duplicateStableFile(spec.Parent)
	if err != nil {
		return nil, fmt.Errorf("duplicate namespace handle: %w", err)
	}
	identity, err := adapter.Identity(pinned)
	if err != nil {
		_ = pinned.Close()
		return nil, err
	}
	identity.Generation = spec.ParentGeneration
	return &StableNamespaceToken{
		parent: pinned, parentIdentity: identity, operation: spec.Operation,
		oldName: spec.OldName, newName: spec.NewName,
		diagnosticPath: filepath.ToSlash(spec.DiagnosticPath), adapter: adapter,
	}, nil
}

func (token *StableNamespaceToken) ParentIdentity() StableIdentity { return token.parentIdentity }
func (token *StableNamespaceToken) Operation() NamespaceOperation  { return token.operation }

func (token *StableNamespaceToken) Stabilize() error {
	if token == nil || token.released.Load() {
		return ErrResourceOwnership
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return ErrResourceOwnership
	}
	switch token.state.Load() {
	case namespaceStable:
		return nil
	case namespaceFailed:
		return token.stabilizeErr
	}
	started := time.Now()
	err := token.adapter.Sync(token.parent)
	token.syncs.Add(1)
	token.syncNanos.Add(uint64(time.Since(started)))
	if err != nil {
		if errors.Is(err, ErrNamespacePersistenceUnsupported) {
			err = errors.Join(ErrNamespacePersistenceUnsupported, err)
		}
		token.stabilizeErr = err
		token.state.Store(namespaceFailed)
		return err
	}
	token.state.Store(namespaceStable)
	return nil
}

func (token *StableNamespaceToken) validateStable() error {
	if token == nil {
		return nil
	}
	switch token.state.Load() {
	case namespaceStable:
		return nil
	case namespaceFailed:
		token.mu.Lock()
		err := token.stabilizeErr
		token.mu.Unlock()
		return err
	default:
		return ErrNamespaceUnstable
	}
}

func (token *StableNamespaceToken) compatible(other *StableNamespaceToken) bool {
	return token.parentIdentity == other.parentIdentity && token.operation == other.operation &&
		token.oldName == other.oldName && token.newName == other.newName
}

func (token *StableNamespaceToken) retain() error {
	if token == nil {
		return ErrResourceOwnership
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return ErrResourceOwnership
	}
	token.refs.Add(1)
	return nil
}

func (token *StableNamespaceToken) release() {
	if token == nil {
		return
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.refs.Add(-1) > 0 {
		return
	}
	if !token.released.Swap(true) {
		_ = token.parent.Close()
	}
}

func (token *StableNamespaceToken) Release() {
	if token == nil {
		return
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.refs.Load() != 0 || token.released.Swap(true) {
		return
	}
	_ = token.parent.Close()
}

type ResourceKindStats struct {
	Kind                  ResourceKind
	PendingCount          uint64
	PendingBytes          uint64
	PendingAge            time.Duration
	Flushes               uint64
	FlushDuration         time.Duration
	Syncs                 uint64
	SyncDuration          time.Duration
	NamespaceSyncs        uint64
	NamespaceSyncDuration time.Duration
	ActivePins            uint64
	PinHighWater          uint64
}

var _ io.ReaderAt = (*StableResourceToken)(nil)
