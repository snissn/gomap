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

	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

var (
	ErrStableIdentityUnsupported       = errors.New("stable resource identity unsupported")
	ErrFilePersistenceUnsupported      = stableio.ErrFilePersistenceUnsupported
	ErrNamespacePersistenceUnsupported = errors.New("namespace persistence unsupported")
	ErrNamespaceUnstable               = errors.New("stable resource namespace unresolved")
	ErrFrontierBeyondResource          = errors.New("stable resource frontier beyond file length")
	ErrResourceConflict                = errors.New("stable resource conflict")
	ErrResourcePinned                  = errors.New("stable resource identity pinned")
	ErrUnresolvedResource              = errors.New("stable resource dependency unresolved")
	ErrResourceOwnership               = errors.New("stable resource ownership violation")
	ErrRecoveryHandoffUnavailable      = errors.New("stable resource recovery handoff unavailable")
	ErrResourceExcluded                = errors.New("stable resource field excluded from candidate ownership")
)

// ResourceKind identifies the physical durability domain of a token. The
// string values are also used by the checked inventory and metrics output.
type ResourceKind string

const (
	ResourceIndex                 ResourceKind = "index"
	ResourceValueLog              ResourceKind = "value-log"
	ResourceOuterLeafLog          ResourceKind = "outer-leaf-log"
	ResourceOuterLeafManifest     ResourceKind = "outer-leaf-manifest"
	ResourceOuterLeafPack         ResourceKind = "outer-leaf-pack"
	ResourceDictionary            ResourceKind = "dictionary"
	ResourceTemplate              ResourceKind = "template"
	ResourceColumnAsset           ResourceKind = "column-asset"
	ResourceTypedColumnAsset      ResourceKind = "typed-column-asset"
	ResourceVectorGraphPack       ResourceKind = "vector-graph-pack"
	ResourceLegacyVectorSnapshot  ResourceKind = "legacy-vector-snapshot"
	ResourceCommandWAL            ResourceKind = "command-wal"
	ResourceCommandWALExternalRID ResourceKind = "command-wal-external-rid"
	ResourceQueryReadyAsset       ResourceKind = "query-ready-asset"
	ResourceSeparateDurability    ResourceKind = "separate-durability-domain"
	ResourceLegacyTreeDBField     ResourceKind = "legacy-treedb-field"
)

// ResourceStability selects the frozen deduplication rule for a resource.
type ResourceStability uint8

const (
	ResourceMutableAppend ResourceStability = iota + 1
	ResourceImmutable
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
	exactRIDs    *exactRIDMembership
}

type exactRIDMembership struct {
	values []uint64
}

// StableLogicalObligation preserves one immutable logical reference when
// several references share and coalesce into one pinned physical resource.
// Digest binds the complete producer-canonical encoding of these fields.
type StableLogicalObligation struct {
	Class        string
	Kind         string
	Namespace    string
	Generation   uint64
	PartID       uint64
	FileID       uint64
	Offset       int64
	Length       int64
	Checksum     uint32
	Reachability ReachabilityField
	Digest       [32]byte
}

// stableLogicalObligationIndex is the immutable reference identity used for
// de-duplication. Checksum and Digest intentionally remain outside the index so
// two declarations of one reference can be rejected when their integrity
// metadata differs. Keeping this key comparable avoids formatting a string on
// every merge and descriptor sort in the publication hot path.
type stableLogicalObligationIndex struct {
	class        string
	kind         string
	namespace    string
	generation   uint64
	partID       uint64
	fileID       uint64
	offset       int64
	length       int64
	reachability ReachabilityField
}

func stableLogicalObligationKey(obligation StableLogicalObligation) stableLogicalObligationIndex {
	return stableLogicalObligationIndex{
		class: obligation.Class, kind: obligation.Kind, namespace: obligation.Namespace,
		generation: obligation.Generation, partID: obligation.PartID, fileID: obligation.FileID,
		offset: obligation.Offset, length: obligation.Length, reachability: obligation.Reachability,
	}
}

func stableLogicalObligationLess(left, right StableLogicalObligation) bool {
	leftKey, rightKey := stableLogicalObligationKey(left), stableLogicalObligationKey(right)
	if leftKey.class != rightKey.class {
		return leftKey.class < rightKey.class
	}
	if leftKey.kind != rightKey.kind {
		return leftKey.kind < rightKey.kind
	}
	if leftKey.namespace != rightKey.namespace {
		return leftKey.namespace < rightKey.namespace
	}
	if leftKey.generation != rightKey.generation {
		return leftKey.generation < rightKey.generation
	}
	if leftKey.partID != rightKey.partID {
		return leftKey.partID < rightKey.partID
	}
	if leftKey.fileID != rightKey.fileID {
		return leftKey.fileID < rightKey.fileID
	}
	if leftKey.offset != rightKey.offset {
		return leftKey.offset < rightKey.offset
	}
	if leftKey.length != rightKey.length {
		return leftKey.length < rightKey.length
	}
	return leftKey.reachability < rightKey.reachability
}

func validateStableLogicalObligation(obligation StableLogicalObligation, reachability ReachabilityField) error {
	if obligation.Class == "" || obligation.Kind == "" || obligation.Namespace == "" ||
		obligation.Generation == 0 || obligation.FileID == 0 || obligation.Offset < 0 ||
		obligation.Length <= 0 || obligation.Reachability == "" || obligation.Digest == [32]byte{} {
		return fmt.Errorf("%w: incomplete logical resource obligation", ErrUnresolvedResource)
	}
	if obligation.Reachability != reachability {
		return fmt.Errorf("%w: logical obligation field %q differs from token field %q", ErrResourceConflict, obligation.Reachability, reachability)
	}
	return nil
}

const stableLogicalObligationLinearLimit = 16

func normalizeStableLogicalObligations(obligations []StableLogicalObligation, reachability ReachabilityField) ([]StableLogicalObligation, error) {
	if len(obligations) == 1 {
		if err := validateStableLogicalObligation(obligations[0], reachability); err != nil {
			return nil, err
		}
		// StableResourceToken is immutable and must not retain a caller-owned
		// singleton backing array. Cap clamping prevents append aliasing but does
		// not prevent direct element mutation.
		return []StableLogicalObligation{obligations[0]}, nil
	}
	if len(obligations) > stableLogicalObligationLinearLimit {
		byKey := make(map[stableLogicalObligationIndex]StableLogicalObligation, len(obligations))
		for _, obligation := range obligations {
			if err := validateStableLogicalObligation(obligation, reachability); err != nil {
				return nil, err
			}
			key := stableLogicalObligationKey(obligation)
			if existing, ok := byKey[key]; ok && existing != obligation {
				return nil, fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
			}
			byKey[key] = obligation
		}
		normalized := make([]StableLogicalObligation, 0, len(byKey))
		for _, obligation := range byKey {
			normalized = append(normalized, obligation)
		}
		sort.Slice(normalized, func(i, j int) bool {
			return stableLogicalObligationLess(normalized[i], normalized[j])
		})
		return normalized[:len(normalized):len(normalized)], nil
	}
	normalized := make([]StableLogicalObligation, 0, len(obligations))
	for _, obligation := range obligations {
		if err := validateStableLogicalObligation(obligation, reachability); err != nil {
			return nil, err
		}
		key := stableLogicalObligationKey(obligation)
		duplicate := false
		for _, existing := range normalized {
			if stableLogicalObligationKey(existing) != key {
				continue
			}
			if existing != obligation {
				return nil, fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
			}
			duplicate = true
			break
		}
		if !duplicate {
			normalized = append(normalized, obligation)
		}
	}
	sort.Slice(normalized, func(i, j int) bool {
		return stableLogicalObligationLess(normalized[i], normalized[j])
	})
	return normalized[:len(normalized):len(normalized)], nil
}

func cloneStableLogicalObligations(obligations []StableLogicalObligation) []StableLogicalObligation {
	return append([]StableLogicalObligation(nil), obligations...)
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
	return newExactRIDFrontier(unique)
}

// RIDs returns a sorted, unique copy of the exact external-RID membership.
// Digest/count/min/max fields summarize this set but never substitute for it.
func (frontier DurableFrontier) RIDs() []uint64 {
	if frontier.exactRIDs == nil {
		return nil
	}
	return append([]uint64(nil), frontier.exactRIDs.values...)
}

func newExactRIDFrontier(sortedUnique []uint64) DurableFrontier {
	frontier := DurableFrontier{RIDCount: uint64(len(sortedUnique))}
	if len(sortedUnique) == 0 {
		return frontier
	}
	frontier.exactRIDs = &exactRIDMembership{values: append([]uint64(nil), sortedUnique...)}
	frontier.RIDMin = sortedUnique[0]
	frontier.RIDMax = sortedUnique[len(sortedUnique)-1]
	frontier.MaxRID = frontier.RIDMax
	raw := make([]byte, 8*len(sortedUnique))
	for i, rid := range sortedUnique {
		binary.LittleEndian.PutUint64(raw[8*i:], rid)
	}
	frontier.RIDSetDigest = sha256.Sum256(raw)
	return frontier
}

func cloneDurableFrontier(frontier DurableFrontier) DurableFrontier {
	if frontier.exactRIDs != nil {
		frontier.exactRIDs = &exactRIDMembership{values: append([]uint64(nil), frontier.exactRIDs.values...)}
	}
	return frontier
}

func validateDurableFrontier(frontier DurableFrontier) error {
	if frontier.exactRIDs == nil {
		if frontier.MaxRID != 0 || frontier.RIDCount != 0 || frontier.RIDMin != 0 || frontier.RIDMax != 0 ||
			frontier.RIDSetDigest != [32]byte{} {
			return fmt.Errorf("%w: RID summary has no exact membership", ErrUnresolvedResource)
		}
		return nil
	}
	want := newExactRIDFrontier(frontier.exactRIDs.values)
	if want.RIDCount != frontier.RIDCount || want.RIDMin != frontier.RIDMin || want.RIDMax != frontier.RIDMax ||
		want.MaxRID != frontier.MaxRID || want.RIDSetDigest != frontier.RIDSetDigest {
		return fmt.Errorf("%w: exact RID membership disagrees with summary", ErrUnresolvedResource)
	}
	for i, rid := range frontier.exactRIDs.values {
		if i > 0 && frontier.exactRIDs.values[i-1] >= rid {
			return fmt.Errorf("%w: exact RID membership is not sorted and unique", ErrUnresolvedResource)
		}
	}
	return nil
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
	// LogicalObligations retains immutable logical references that share this
	// physical resource and must survive physical pin coalescing.
	LogicalObligations []StableLogicalObligation
	FlushThrough       resourceOperation
	SyncThrough        resourceOperation
	// ContentSynced records that the exact registered frontier was already
	// persisted before capture. A later coalesced frontier is not covered and
	// must still execute SyncThrough on the pinned identity.
	ContentSynced bool
	OnRelease     func()
	// PinRegistry is the DB-scoped physical deletion gate. When set, token
	// construction acquires a pin for the exact handle identity before return.
	PinRegistry *IdentityPinRegistry

	// StableIdentityOverride exists for deterministic platform-adapter and
	// conflict tests. Production producers leave it zero and use handle identity.
	StableIdentityOverride StableIdentity
}

type resourceTokenMetrics struct {
	flushes               atomic.Uint64
	flushNanos            atomic.Uint64
	syncs                 atomic.Uint64
	syncNanos             atomic.Uint64
	physicalFileSyncs     atomic.Uint64
	physicalFileSyncNanos atomic.Uint64
	registeredNanos       int64
}

type StableResourceToken struct {
	kind               ResourceKind
	logicalLane        string
	resourceID         string
	generation         uint64
	diagnosticPath     string
	identity           StableIdentity
	frontier           DurableFrontier
	digest             [32]byte
	reachability       ReachabilityField
	logicalObligations []StableLogicalObligation
	stability          ResourceStability
	namespace          *StableNamespaceToken
	pinned             *os.File
	flush              resourceOperation
	sync               resourceOperation
	syncedFrontier     DurableFrontier
	hasSyncedFrontier  bool
	onRelease          func()
	identityPin        *IdentityPin
	owner              atomic.Uint32
	released           atomic.Bool
	metrics            resourceTokenMetrics
}

func NewStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	return newStableResourceToken(spec, nil)
}

// newStableResourceToken accepts an immutable normalized obligation view only
// for exact-handle clones produced inside this package. Public producers always
// pass nil and retain the full validation/copy boundary above.
func newStableResourceToken(spec StableResourceSpec, normalized []StableLogicalObligation) (*StableResourceToken, error) {
	if spec.Kind == "" || spec.ResourceID == "" || spec.Generation == 0 || spec.Reachability == "" || spec.File == nil {
		return nil, fmt.Errorf("%w: incomplete resource registration", ErrUnresolvedResource)
	}
	if err := validateDiagnosticPath(spec.DiagnosticPath); err != nil {
		return nil, err
	}
	if err := validateDurableFrontier(spec.Frontier); err != nil {
		return nil, err
	}
	logicalObligations := normalized
	if logicalObligations == nil {
		var err error
		logicalObligations, err = normalizeStableLogicalObligations(spec.LogicalObligations, spec.Reachability)
		if err != nil {
			return nil, err
		}
	} else {
		logicalObligations = stableLogicalObligationList(logicalObligations)
	}
	stability, ok := stableResourceStabilityForField(spec.Reachability)
	if !ok {
		return nil, fmt.Errorf("%w: no stability policy for reachability field %q", ErrUnresolvedResource, spec.Reachability)
	}
	duplicate := duplicateStableFile
	if spec.SyncThrough == nil {
		// The default Windows durability barrier needs a private write-capable
		// reopen even when the producer retains only a read handle. A custom
		// sync callback owns its handle contract, so preserve the source access
		// rights (and support non-disk handles used by custom barriers).
		duplicate = duplicateStableSyncFile
	}
	pinned, err := duplicate(spec.File)
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
	var identityPin *IdentityPin
	if spec.PinRegistry != nil {
		identityPin, err = spec.PinRegistry.Pin(identity)
		if err != nil {
			return nil, fmt.Errorf("pin stable resource identity: %w", err)
		}
		defer func() {
			if closeOnError {
				identityPin.Release()
			}
		}()
	}
	if err := spec.Namespace.validateLinkedResource(identity); err != nil {
		return nil, err
	}
	flush := spec.FlushThrough
	if flush == nil {
		// Concrete producers drain userspace buffers before registration. The
		// publication flush phase therefore has no additional file primitive;
		// SyncThrough below owns the single default content fsync.
		flush = func(*os.File, DurableFrontier) error { return nil }
	}
	syncThrough := spec.SyncThrough
	if syncThrough == nil {
		syncThrough = func(file *os.File, _ DurableFrontier) error { return stableio.SyncFile(file) }
	}
	token := &StableResourceToken{
		kind: spec.Kind, logicalLane: spec.LogicalLane, resourceID: spec.ResourceID,
		generation: spec.Generation, diagnosticPath: filepath.ToSlash(spec.DiagnosticPath),
		identity: identity, frontier: cloneDurableFrontier(spec.Frontier), digest: spec.Digest,
		reachability: spec.Reachability, logicalObligations: logicalObligations,
		stability: stability, namespace: spec.Namespace, pinned: pinned,
		flush: flush, sync: syncThrough, onRelease: spec.OnRelease, identityPin: identityPin,
	}
	if spec.ContentSynced {
		token.syncedFrontier = cloneDurableFrontier(spec.Frontier)
		token.hasSyncedFrontier = true
		// ContentSynced is producer certification that the exact registered
		// file frontier crossed a physical durability barrier before capture.
		token.metrics.physicalFileSyncs.Store(1)
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

func (token *StableResourceToken) Kind() ResourceKind       { return token.kind }
func (token *StableResourceToken) LogicalLane() string      { return token.logicalLane }
func (token *StableResourceToken) ResourceID() string       { return token.resourceID }
func (token *StableResourceToken) Generation() uint64       { return token.generation }
func (token *StableResourceToken) DiagnosticPath() string   { return token.diagnosticPath }
func (token *StableResourceToken) Identity() StableIdentity { return token.identity }
func (token *StableResourceToken) Frontier() DurableFrontier {
	return cloneDurableFrontier(token.frontier)
}
func (token *StableResourceToken) Digest() [32]byte                 { return token.digest }
func (token *StableResourceToken) Reachability() ReachabilityField  { return token.reachability }
func (token *StableResourceToken) Namespace() *StableNamespaceToken { return token.namespace }
func (token *StableResourceToken) LogicalObligations() []StableLogicalObligation {
	return cloneStableLogicalObligations(token.logicalObligations)
}

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
	var err error
	if !token.hasSyncedFrontier || !durableFrontierCovers(token.syncedFrontier, frontier) {
		physicalStarted := time.Now()
		err = token.sync(token.pinned, frontier)
		if err == nil {
			token.metrics.physicalFileSyncs.Add(1)
			token.metrics.physicalFileSyncNanos.Add(uint64(time.Since(physicalStarted)))
		}
	}
	token.metrics.syncs.Add(1)
	token.metrics.syncNanos.Add(uint64(time.Since(started)))
	return err
}

func durableFrontierCovers(stable, required DurableFrontier) bool {
	if stable.Bytes < required.Bytes || stable.MaxLSN < required.MaxLSN {
		return false
	}
	stableRIDs, requiredRIDs := stable.RIDs(), required.RIDs()
	if len(requiredRIDs) == 0 {
		return true
	}
	i := 0
	for _, requiredRID := range requiredRIDs {
		for i < len(stableRIDs) && stableRIDs[i] < requiredRID {
			i++
		}
		if i == len(stableRIDs) || stableRIDs[i] != requiredRID {
			return false
		}
		i++
	}
	return true
}

func (token *StableResourceToken) ReadAt(dst []byte, offset int64) (int, error) {
	if token == nil || token.released.Load() {
		return 0, ErrResourceOwnership
	}
	return token.pinned.ReadAt(dst, offset)
}

// WithPinnedFile scopes access to the exact retained resource handle. It is
// intended for platform adapters, such as the pager's mapped-page durability
// primitive, that must pair an already-validated identity pin with an
// operation unavailable through ordinary file Sync. Callers must not retain
// or close the handle after fn returns.
func (token *StableResourceToken) WithPinnedFile(fn func(*os.File) error) error {
	if token == nil || fn == nil || token.released.Load() || token.pinned == nil {
		return ErrResourceOwnership
	}
	return fn(token.pinned)
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
	token.identityPin.Release()
	if token.onRelease != nil {
		token.onRelease()
	}
}

type stableLogicalResourceKey struct {
	kind       ResourceKind
	lane       string
	resourceID string
	generation uint64
}

type stablePhysicalResourceKey struct {
	kind       ResourceKind
	platform   string
	volumeID   uint64
	objectID   [16]byte
	generation uint64
}

// stablePhysicalIdentityKey deliberately excludes kind and generation because
// stableResourcesCoalesce makes those distinctions after matching the pinned
// filesystem identity.
type stablePhysicalIdentityKey struct {
	platform string
	volumeID uint64
	objectID [16]byte
}

func (token *StableResourceToken) logicalKey() stableLogicalResourceKey {
	return stableLogicalResourceKey{
		kind: token.kind, lane: token.logicalLane, resourceID: token.resourceID, generation: token.generation,
	}
}

func (token *StableResourceToken) identityKey() stablePhysicalResourceKey {
	return token.mutablePhysicalKey()
}

func (token *StableResourceToken) samePhysicalIdentity(other *StableResourceToken) bool {
	return token != nil && other != nil && token.identity.Platform == other.identity.Platform &&
		token.identity.VolumeID == other.identity.VolumeID && token.identity.ObjectID == other.identity.ObjectID
}

func (token *StableResourceToken) physicalIdentityKey() stablePhysicalIdentityKey {
	return stablePhysicalIdentityKey{
		platform: token.identity.Platform, volumeID: token.identity.VolumeID, objectID: token.identity.ObjectID,
	}
}

func (token *StableResourceToken) physicalCoalescingKey() stablePhysicalResourceKey {
	key := token.mutablePhysicalKey()
	if token.stability == ResourceImmutable {
		key.kind = ""
	}
	return key
}

func (token *StableResourceToken) mutablePhysicalKey() stablePhysicalResourceKey {
	return stablePhysicalResourceKey{
		kind: token.kind, platform: token.identity.Platform, volumeID: token.identity.VolumeID,
		objectID: token.identity.ObjectID, generation: token.generation,
	}
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
	return validateDurableFrontier(older) == nil && validateDurableFrontier(newer) == nil
}

func maxFrontier(older, newer DurableFrontier) DurableFrontier {
	out := cloneDurableFrontier(older)
	if newer.Bytes > out.Bytes {
		out.Bytes = newer.Bytes
	}
	if newer.MaxLSN > out.MaxLSN {
		out.MaxLSN = newer.MaxLSN
	}
	olderRIDs, newerRIDs := older.RIDs(), newer.RIDs()
	if len(olderRIDs) != 0 || len(newerRIDs) != 0 {
		union := make([]uint64, 0, len(olderRIDs)+len(newerRIDs))
		i, j := 0, 0
		for i < len(olderRIDs) || j < len(newerRIDs) {
			var next uint64
			switch {
			case j == len(newerRIDs) || (i < len(olderRIDs) && olderRIDs[i] < newerRIDs[j]):
				next = olderRIDs[i]
				i++
			case i == len(olderRIDs) || newerRIDs[j] < olderRIDs[i]:
				next = newerRIDs[j]
				j++
			default:
				next = olderRIDs[i]
				i++
				j++
			}
			if len(union) == 0 || union[len(union)-1] != next {
				union = append(union, next)
			}
		}
		ridFrontier := newExactRIDFrontier(union)
		out.MaxRID, out.RIDSetDigest, out.RIDCount = ridFrontier.MaxRID, ridFrontier.RIDSetDigest, ridFrontier.RIDCount
		out.RIDMin, out.RIDMax, out.exactRIDs = ridFrontier.RIDMin, ridFrontier.RIDMax, ridFrontier.exactRIDs
	}
	return out
}

type namespacePersistenceAdapter interface {
	Identity(*os.File) (StableIdentity, error)
	ValidateLink(*os.File, *os.File, string) error
	ValidateIdentity(*os.File, StableIdentity, string) error
	Sync(*os.File) error
}

type nativeNamespaceAdapter struct{}

func (nativeNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (nativeNamespaceAdapter) ValidateLink(parent, resource *os.File, name string) error {
	return validateStableChildLink(parent, resource, name)
}

func (nativeNamespaceAdapter) ValidateIdentity(parent *os.File, identity StableIdentity, name string) error {
	return validateStableChildIdentity(parent, identity, name)
}

func (nativeNamespaceAdapter) Sync(file *os.File) error {
	return syncStableNamespace(file)
}

// SyncStableFile executes the platform's physical file durability barrier.
// Unsupported or weaker-only platforms fail closed with
// ErrFilePersistenceUnsupported.
func SyncStableFile(file *os.File) error { return stableio.SyncFile(file) }

// SyncStableNamespace executes the platform's exact directory/namespace
// durability barrier. Unsupported platforms fail closed.
func SyncStableNamespace(parent *os.File) error { return syncStableNamespace(parent) }

type StableNamespaceSpec struct {
	Parent           *os.File
	LinkedResource   *os.File
	ParentGeneration uint64
	Operation        NamespaceOperation
	OldName          string
	NewName          string
	DiagnosticPath   string
}

// StableNamespaceBatchSpec binds several already-linked children to stable
// namespace tokens while syncing each distinct exact parent once. Additional
// parents carry source-side obligations for cross-parent moves.
type StableNamespaceBatchSpec struct {
	Registrations     []StableNamespaceSpec
	AdditionalParents []*os.File
}

// NewStableNamespaceBatchTokens validates every exact child link, syncs each
// distinct retained parent once, and returns already-stable tokens in input
// order. No token becomes stable when any validation or parent sync fails.
func NewStableNamespaceBatchTokens(spec StableNamespaceBatchSpec) ([]*StableNamespaceToken, error) {
	return newStableNamespaceBatchTokens(spec, nativeNamespaceAdapter{})
}

func newStableNamespaceBatchTokens(spec StableNamespaceBatchSpec, adapter namespacePersistenceAdapter) ([]*StableNamespaceToken, error) {
	return newStableNamespaceBatchTokensWithDuplicate(spec, adapter, duplicateStableFile)
}

func newStableNamespaceBatchTokensWithDuplicate(spec StableNamespaceBatchSpec, adapter namespacePersistenceAdapter, duplicate func(*os.File) (*os.File, error)) ([]*StableNamespaceToken, error) {
	if len(spec.Registrations) == 0 || adapter == nil || duplicate == nil {
		return nil, fmt.Errorf("%w: empty stable namespace batch", ErrUnresolvedResource)
	}
	// A Windows creation proof is persisted through each exact child handle,
	// not through one shared parent sync. The current batch API also represents
	// rename and source-parent obligations, so keep that broader contract typed
	// unsupported instead of certifying it with the create-only primitive.
	if stableNamespaceCreationPersistsThroughChild() {
		return nil, fmt.Errorf("%w: batched parent namespace persistence is unavailable", ErrNamespacePersistenceUnsupported)
	}
	tokens := make([]*StableNamespaceToken, 0, len(spec.Registrations))
	releaseTokens := func() {
		for _, token := range tokens {
			token.Release()
		}
	}
	for _, registration := range spec.Registrations {
		token, err := newStableNamespaceToken(registration, adapter)
		if err != nil {
			releaseTokens()
			return nil, err
		}
		tokens = append(tokens, token)
	}
	type stableBatchParent struct {
		identity StableIdentity
		file     *os.File
	}
	parents := make([]stableBatchParent, 0, len(tokens)+len(spec.AdditionalParents))
	defer func() {
		for _, parent := range parents {
			_ = parent.file.Close()
		}
	}()
	seen := make(map[StableIdentity]struct{}, cap(parents))
	addParent := func(parent *os.File) error {
		if parent == nil {
			return fmt.Errorf("%w: nil stable namespace batch parent", ErrUnresolvedResource)
		}
		identity, err := adapter.Identity(parent)
		if err != nil {
			return err
		}
		identity.Generation = 0
		if _, ok := seen[identity]; ok {
			return nil
		}
		pinned, err := duplicate(parent)
		if err != nil {
			return err
		}
		seen[identity] = struct{}{}
		parents = append(parents, stableBatchParent{identity: identity, file: pinned})
		return nil
	}
	for _, registration := range spec.Registrations {
		if err := addParent(registration.Parent); err != nil {
			releaseTokens()
			return nil, err
		}
	}
	for _, parent := range spec.AdditionalParents {
		if err := addParent(parent); err != nil {
			releaseTokens()
			return nil, err
		}
	}
	var syncCounts = make(map[StableIdentity]uint64, len(parents))
	var syncNanos = make(map[StableIdentity]uint64, len(parents))
	for _, parent := range parents {
		started := time.Now()
		err := adapter.Sync(parent.file)
		elapsed := uint64(time.Since(started))
		if err != nil {
			releaseTokens()
			return nil, err
		}
		syncCounts[parent.identity] = 1
		syncNanos[parent.identity] = elapsed
	}
	for _, token := range tokens {
		identity := token.parentIdentity
		identity.Generation = 0
		token.state.Store(namespaceStable)
		if syncCounts[identity] != 0 {
			token.syncs.Store(syncCounts[identity])
			token.syncNanos.Store(syncNanos[identity])
			delete(syncCounts, identity)
		}
	}
	// Sync evidence for source-side parents has no reachable resource token of
	// its own. Attach that batch evidence once to the first real namespace token
	// so resource-set stats count physical parent syncs without inventing a
	// resource or double-counting the destination parent.
	if len(tokens) != 0 {
		for identity, count := range syncCounts {
			tokens[0].additionalSyncs.Add(count)
			tokens[0].additionalSyncNanos.Add(syncNanos[identity])
		}
	}
	return tokens, nil
}

// StableNamespaceParentGeneration derives a stable, non-zero logical
// generation from an exact parent namespace handle. The token retains and
// validates the full platform identity separately; this compact generation is
// only the logical namespace epoch used by resource registration and changes
// when the physical parent object changes.
func StableNamespaceParentGeneration(parent *os.File) (uint64, error) {
	if parent == nil {
		return 0, fmt.Errorf("%w: namespace parent generation requires an exact parent handle", ErrUnresolvedResource)
	}
	identity, err := stableIdentityFromFile(parent)
	if err != nil {
		return 0, err
	}
	if !identity.valid() {
		return 0, fmt.Errorf("%w: namespace parent has no stable identity", ErrUnresolvedResource)
	}
	// FNV-1a is sufficient here because the exact identity remains part of the
	// token and is the authoritative conflict check. Avoid process-random or
	// caller-invented constants so cloned producers agree on one parent epoch.
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	generation := offset64
	add := func(value byte) {
		generation ^= uint64(value)
		generation *= prime64
	}
	for i := range identity.Platform {
		add(identity.Platform[i])
	}
	for shift := 0; shift < 64; shift += 8 {
		add(byte(identity.VolumeID >> shift))
	}
	for _, value := range identity.ObjectID {
		add(value)
	}
	if generation == 0 {
		generation = 1
	}
	return generation, nil
}

type StableNamespaceToken struct {
	parent                 *os.File
	parentIdentity         StableIdentity
	persistence            *os.File
	persistenceIdentity    StableIdentity
	linkedResourceIdentity StableIdentity
	hasLinkedResource      bool
	operation              NamespaceOperation
	oldName                string
	newName                string
	diagnosticPath         string
	adapter                namespacePersistenceAdapter
	state                  atomic.Uint32
	refs                   atomic.Int64
	released               atomic.Bool
	mu                     sync.Mutex
	stabilizeErr           error
	syncs                  atomic.Uint64
	syncNanos              atomic.Uint64
	additionalSyncs        atomic.Uint64
	additionalSyncNanos    atomic.Uint64
}

// StableNamespaceCreationProof is an opaque, exact-handle witness that a
// newly-created child was linked from its retained parent and durably synced
// through the platform's exact creation-persistence handle. It exists solely
// to carry that one creation sync to later resource registration, where the
// logical parent generation is finally known.
type StableNamespaceCreationProof struct {
	parent      *os.File
	persistence *os.File
	parentID    StableIdentity
	childID     StableIdentity
	name        string
	adapter     namespacePersistenceAdapter
	released    atomic.Bool
	syncs       atomic.Uint64
	syncNanos   atomic.Uint64
	mu          sync.Mutex
}

// NewStableNamespaceCreationProof validates the exact parent/child link and
// performs the sole namespace sync for a just-created child.
func NewStableNamespaceCreationProof(parent, child *os.File, name string) (*StableNamespaceCreationProof, error) {
	return newStableNamespaceCreationProof(parent, child, name, nativeNamespaceAdapter{})
}

func newStableNamespaceCreationProof(parent, child *os.File, name string, adapter namespacePersistenceAdapter) (*StableNamespaceCreationProof, error) {
	if parent == nil || child == nil || !stableChildBaseName(name) {
		return nil, fmt.Errorf("%w: incomplete namespace creation proof", ErrUnresolvedResource)
	}
	if adapter == nil {
		return nil, fmt.Errorf("%w: missing namespace persistence adapter", ErrUnresolvedResource)
	}
	if err := adapter.ValidateLink(parent, child, name); err != nil {
		return nil, err
	}
	parentID, err := adapter.Identity(parent)
	if err != nil {
		return nil, err
	}
	childID, err := adapter.Identity(child)
	if err != nil {
		return nil, err
	}
	pinned, err := duplicateStableFile(parent)
	if err != nil {
		return nil, fmt.Errorf("duplicate namespace proof parent: %w", err)
	}
	persistence := pinned
	if stableNamespaceCreationPersistsThroughChild() {
		persistence, err = duplicateStableFile(child)
		if err != nil {
			_ = pinned.Close()
			return nil, fmt.Errorf("duplicate namespace proof child: %w", err)
		}
	}
	started := time.Now()
	err = adapter.Sync(persistence)
	syncNanos := uint64(time.Since(started))
	if err != nil {
		if persistence != pinned {
			_ = persistence.Close()
		}
		_ = pinned.Close()
		return nil, err
	}
	proof := &StableNamespaceCreationProof{parent: pinned, persistence: persistence, parentID: parentID, childID: childID, name: name, adapter: adapter}
	proof.syncs.Store(1)
	proof.syncNanos.Store(syncNanos)
	return proof, nil
}

// Bind returns an already-stable normal namespace token after proving the
// retained parent still links the original child. It never syncs again.
func (proof *StableNamespaceCreationProof) Bind(parent *os.File, parentGeneration uint64, name, diagnosticPath string) (*StableNamespaceToken, error) {
	if proof == nil || proof.released.Load() {
		return nil, ErrResourceOwnership
	}
	if parentGeneration == 0 {
		return nil, fmt.Errorf("%w: namespace creation proof binding requires a parent generation", ErrUnresolvedResource)
	}
	if parent == nil || name != proof.name || !stableChildBaseName(name) {
		return nil, fmt.Errorf("%w: namespace creation proof binding differs from the exact parent or child name", ErrResourceConflict)
	}
	if err := validateDiagnosticPath(diagnosticPath); err != nil {
		return nil, err
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if proof.released.Load() {
		return nil, ErrResourceOwnership
	}
	parentID, err := proof.adapter.Identity(parent)
	if err != nil {
		return nil, err
	}
	if !sameStableObject(parentID, proof.parentID) {
		return nil, fmt.Errorf("%w: namespace creation proof binding names a different parent", ErrResourceConflict)
	}
	if err := proof.adapter.ValidateIdentity(proof.parent, proof.childID, proof.name); err != nil {
		return nil, err
	}
	pinned, err := duplicateStableFile(proof.parent)
	if err != nil {
		return nil, err
	}
	parentID, err = proof.adapter.Identity(pinned)
	if err != nil {
		_ = pinned.Close()
		return nil, err
	}
	parentID.Generation = parentGeneration
	persistenceID := parentID
	persistenceID.Generation = 0
	token := &StableNamespaceToken{parent: pinned, parentIdentity: parentID, persistence: pinned, persistenceIdentity: persistenceID, linkedResourceIdentity: proof.childID, hasLinkedResource: true, operation: NamespaceCreate, newName: proof.name, diagnosticPath: filepath.ToSlash(diagnosticPath), adapter: proof.adapter}
	token.state.Store(namespaceStable)
	token.syncs.Store(proof.syncs.Load())
	token.syncNanos.Store(proof.syncNanos.Load())
	return token, nil
}

func (proof *StableNamespaceCreationProof) Release() {
	if proof == nil {
		return
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if proof.released.Swap(true) {
		return
	}
	parent, persistence := proof.parent, proof.persistence
	proof.parent, proof.persistence = nil, nil
	if persistence != nil && persistence != parent {
		_ = persistence.Close()
	}
	if parent != nil {
		_ = parent.Close()
	}
}

const (
	namespacePending uint32 = iota
	namespaceStable
	namespaceFailed
)

func NewStableNamespaceToken(spec StableNamespaceSpec) (*StableNamespaceToken, error) {
	return newStableNamespaceToken(spec, nativeNamespaceAdapter{})
}

// NewRecoveredStableNamespaceToken reconstructs an already-durable namespace
// proof during bounded root recovery. It validates the exact parent identity
// and current parent/child link but deliberately does not issue a new sync: the
// selected durable meta is itself the evidence that this namespace operation
// crossed its barrier before publication.
func NewRecoveredStableNamespaceToken(spec StableNamespaceSpec, expectedParent StableIdentity) (*StableNamespaceToken, error) {
	token, err := newStableNamespaceToken(spec, nativeNamespaceAdapter{})
	if err != nil {
		return nil, err
	}
	if token.parentIdentity.Generation != expectedParent.Generation || !SamePhysicalIdentity(token.parentIdentity, expectedParent) {
		token.Release()
		return nil, fmt.Errorf("%w: recovered namespace parent identity differs from manifest", ErrResourceConflict)
	}
	token.state.Store(namespaceStable)
	return token, nil
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
	var linkedIdentity StableIdentity
	hasLinkedResource := spec.LinkedResource != nil
	if hasLinkedResource {
		if err := adapter.ValidateLink(spec.Parent, spec.LinkedResource, spec.NewName); err != nil {
			return nil, err
		}
		var err error
		linkedIdentity, err = adapter.Identity(spec.LinkedResource)
		if err != nil {
			return nil, err
		}
		if !linkedIdentity.valid() {
			return nil, fmt.Errorf("%w: linked namespace resource has no stable identity", ErrUnresolvedResource)
		}
		linkedIdentity.Generation = 0
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
	persistence := pinned
	persistenceIdentity := identity
	persistenceIdentity.Generation = 0
	if stableNamespaceCreationPersistsThroughChild() && spec.Operation == NamespaceCreate && spec.LinkedResource != nil {
		persistence, err = duplicateStableSyncFile(spec.LinkedResource)
		if err != nil {
			_ = pinned.Close()
			return nil, fmt.Errorf("duplicate namespace creation resource: %w", err)
		}
		persistenceIdentity, err = adapter.Identity(persistence)
		if err != nil {
			_ = persistence.Close()
			_ = pinned.Close()
			return nil, err
		}
		persistenceIdentity.Generation = 0
	}
	return &StableNamespaceToken{
		parent: pinned, parentIdentity: identity, persistence: persistence, persistenceIdentity: persistenceIdentity, linkedResourceIdentity: linkedIdentity,
		hasLinkedResource: hasLinkedResource, operation: spec.Operation,
		oldName: spec.OldName, newName: spec.NewName,
		diagnosticPath: filepath.ToSlash(spec.DiagnosticPath), adapter: adapter,
	}, nil
}

// OpenStableChildFile opens or creates name relative to the exact already-open
// parent directory handle. Platforms without a real relative-directory handle
// primitive return ErrNamespacePersistenceUnsupported rather than reopening a
// diagnostic path.
func OpenStableChildFile(parent *os.File, name string, flags int, perm os.FileMode) (*os.File, error) {
	if parent == nil || name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return nil, fmt.Errorf("%w: stable child requires a base name and exact parent handle", ErrUnresolvedResource)
	}
	return openStableChildFile(parent, name, flags, perm)
}

// OpenStableAnonymousFile creates an unlinked regular file relative to the
// exact already-open parent directory handle.  It never creates a staging
// pathname: callers can publish the retained handle with
// InstallStableFileHandleNoReplace after syncing its contents.  Platforms or
// filesystems without an anonymous-file primitive fail closed before a
// namespace mutation.
func OpenStableAnonymousFile(parent *os.File, perm os.FileMode) (*os.File, error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: anonymous stable file requires an exact parent handle", ErrUnresolvedResource)
	}
	info, err := parent.Stat()
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: anonymous stable file parent is not an exact directory", ErrUnresolvedResource)
	}
	return openStableAnonymousFile(parent, perm)
}

// OpenStableParent captures a directory for later exact-handle child
// operations. On Windows the handle explicitly shares delete access so a
// rename/recreate adversary cannot force later child creation through the
// rebound diagnostic path.
func OpenStableParent(path string) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: stable parent path is empty", ErrUnresolvedResource)
	}
	return openStableParent(path)
}

// EnsureStableChildDirectory opens or creates name relative to the exact
// already-open parent and establishes the child link's namespace durability
// before returning it. The returned handle is the authority for constructing
// deeper descendants; callers must not replace it with a pathname reopen.
//
// Unix stabilizes the retained parent directory. Windows uses its narrower
// create-through-child contract and flushes the exact child directory handle.
func EnsureStableChildDirectory(parent *os.File, name string, perm os.FileMode, registry *IdentityPinRegistry) (*os.File, error) {
	if parent == nil || !stableChildBaseName(name) {
		return nil, fmt.Errorf("%w: stable child directory requires a base name and exact parent handle", ErrUnresolvedResource)
	}
	child, err := openOrCreateStableChildDirectory(parent, name, perm)
	if err != nil {
		return nil, err
	}
	info, err := child.Stat()
	if err != nil {
		_ = child.Close()
		return nil, err
	}
	if !info.IsDir() {
		_ = child.Close()
		return nil, fmt.Errorf("%w: stable child %q is not a directory", ErrResourceConflict, name)
	}
	if registry != nil {
		known, err := registry.StableDirectoryLinkKnown(parent, child, name)
		if err != nil {
			_ = child.Close()
			return nil, err
		}
		if known {
			return child, nil
		}
	}
	persistence := parent
	if stableNamespaceCreationPersistsThroughChild() {
		persistence = child
	}
	if err := syncStableNamespace(persistence); err != nil {
		_ = child.Close()
		return nil, err
	}
	if registry != nil {
		if err := registry.RememberStableDirectoryLink(parent, child, name); err != nil {
			_ = child.Close()
			return nil, err
		}
	}
	return child, nil
}

// RemoveStableChildFile unlinks name relative to the exact already-open parent
// directory. Callers remain responsible for syncing parent after the unlink.
func RemoveStableChildFile(parent *os.File, name string) error {
	if parent == nil || name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return fmt.Errorf("%w: stable child removal requires a base name and exact parent handle", ErrUnresolvedResource)
	}
	return removeStableChildFile(parent, name)
}

// RenameStableChildFile atomically replaces newName with oldName relative to
// the exact already-open parent directory. Unsupported platforms fail closed;
// this operation never reopens the parent's diagnostic pathname.
func RenameStableChildFile(parent *os.File, oldName, newName string) error {
	if parent == nil || !stableChildBaseName(oldName) || !stableChildBaseName(newName) || oldName == newName {
		return fmt.Errorf("%w: stable child rename requires distinct base names and an exact parent handle", ErrUnresolvedResource)
	}
	return renameStableChildFile(parent, oldName, newName)
}

// LinkStableChildFileNoReplace installs a hard link relative to one exact
// parent without following a rebound pathname. It reports os.ErrExist when
// newName already exists. Unsupported platforms return
// ErrNamespacePersistenceUnsupported.
func LinkStableChildFileNoReplace(parent *os.File, oldName, newName string) error {
	if parent == nil || !stableChildBaseName(oldName) || !stableChildBaseName(newName) || oldName == newName {
		return fmt.Errorf("%w: stable child link requires distinct base names and an exact parent handle", ErrUnresolvedResource)
	}
	return linkStableChildFileNoReplace(parent, oldName, newName)
}

// InstallStableFileHandleNoReplace installs the exact already-open file under
// name in destinationParent without consulting a source pathname. The boolean
// reports whether the destination link was installed; true with an error is
// namespace-ambiguous and callers must retain recovery state. Unsupported
// platforms fail closed before mutating the destination namespace.
func InstallStableFileHandleNoReplace(expected, destinationParent *os.File, name string) (bool, error) {
	if expected == nil || destinationParent == nil || !stableChildBaseName(name) {
		return false, fmt.Errorf("%w: stable handle install requires a base name and exact file and parent handles", ErrUnresolvedResource)
	}
	return installStableFileHandleNoReplace(expected, destinationParent, name)
}

// MoveStableChildFileNoReplace installs Expected under destinationParent using
// an exact retained-handle no-replace operation. oldName is validated under the
// exact source parent before and after installation, but is deliberately left
// linked for staging cleanup so this primitive never unlinks a rebound name.
// The boolean reports whether the destination link was installed; true with an
// error is namespace-ambiguous and requires recovery retention.
func MoveStableChildFileNoReplace(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
	if sourceParent == nil || expected == nil || destinationParent == nil || !stableChildBaseName(oldName) || !stableChildBaseName(newName) {
		return false, fmt.Errorf("%w: stable cross-parent move requires base names and exact parent/child handles", ErrUnresolvedResource)
	}
	return moveStableChildFileNoReplace(sourceParent, expected, oldName, destinationParent, newName)
}

// StableCrossParentMoveNoReplaceSupported reports whether the platform exposes
// an atomic cross-parent no-replace primitive used by packed promotion.
func StableCrossParentMoveNoReplaceSupported() bool {
	return stableCrossParentMoveNoReplaceSupported()
}

func stableChildBaseName(name string) bool {
	return name != "" && filepath.Base(name) == name && name != "." && name != ".."
}

// StableRelativeNamespaceSupported reports whether exact retained-parent
// create, rename, validation, removal, and namespace persistence primitives
// are available on this platform. Producers use this as a preflight before
// creating a temporary child or exposing any stable-mode visibility.
func StableRelativeNamespaceSupported() bool {
	return stableRelativeNamespaceSupported()
}

// StableNamespaceCreationSupported reports whether the platform can persist
// an exact retained-parent child creation. Windows supports this narrower
// contract by flushing the exact child even though rename, removal, and parent
// directory sync remain unsupported.
func StableNamespaceCreationSupported() bool {
	return stableRelativeNamespaceSupported() || stableNamespaceCreationPersistsThroughChild()
}

func validateStableChildLink(parent, resource *os.File, name string) error {
	resourceIdentity, err := stableIdentityFromFile(resource)
	if err != nil {
		return err
	}
	return validateStableChildIdentity(parent, resourceIdentity, name)
}

// ValidateStableChildLink proves that resource is the entry named from the
// exact retained parent handle. It never resolves the parent's diagnostic path.
func ValidateStableChildLink(parent, resource *os.File, name string) error {
	return validateStableChildLink(parent, resource, name)
}

func validateStableChildIdentity(parent *os.File, resourceIdentity StableIdentity, name string) error {
	linked, err := OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, ErrNamespacePersistenceUnsupported) {
			return err
		}
		return fmt.Errorf("%w: open %q relative to exact parent: %v", ErrResourceConflict, name, err)
	}
	defer linked.Close()
	linkedIdentity, err := stableIdentityFromFile(linked)
	if err != nil {
		return err
	}
	if linkedIdentity.Platform != resourceIdentity.Platform || linkedIdentity.VolumeID != resourceIdentity.VolumeID ||
		linkedIdentity.ObjectID != resourceIdentity.ObjectID {
		return fmt.Errorf("%w: resource %q is not linked from exact parent", ErrResourceConflict, name)
	}
	return nil
}

func (token *StableNamespaceToken) ParentIdentity() StableIdentity { return token.parentIdentity }
func (token *StableNamespaceToken) Operation() NamespaceOperation  { return token.operation }

func (token *StableNamespaceToken) physicalSyncStats() (uint64, time.Duration) {
	if token == nil {
		return 0, 0
	}
	return token.syncs.Load() + token.additionalSyncs.Load(),
		time.Duration(token.syncNanos.Load() + token.additionalSyncNanos.Load())
}

// StabilizeStableNamespaceTokens validates every exact child obligation and
// syncs each distinct platform persistence handle once. Unix tokens share a
// retained parent generation; Windows create-only tokens retain their exact
// child. Unlike construction-time namespace batches, this is intended for
// obligations accumulated by a relaxed publication protocol and therefore
// leaves pending tokens retryable when the physical sync itself fails.
func StabilizeStableNamespaceTokens(tokens ...*StableNamespaceToken) error {
	type namespaceGroup struct {
		identity StableIdentity
		tokens   []*StableNamespaceToken
	}
	groups := make([]namespaceGroup, 0, len(tokens))
	groupByIdentity := make(map[StableIdentity]int, len(tokens))
	seen := make(map[*StableNamespaceToken]struct{}, len(tokens))
	for _, token := range tokens {
		if token == nil {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		identity := token.persistenceIdentity
		groupIndex, ok := groupByIdentity[identity]
		if !ok {
			groupIndex = len(groups)
			groupByIdentity[identity] = groupIndex
			groups = append(groups, namespaceGroup{identity: identity})
		}
		groups[groupIndex].tokens = append(groups[groupIndex].tokens, token)
	}
	for _, group := range groups {
		pending := make([]*StableNamespaceToken, 0, len(group.tokens))
		for _, token := range group.tokens {
			needsSync, err := token.prepareSharedStabilize()
			if err != nil {
				return err
			}
			if needsSync {
				pending = append(pending, token)
			}
		}
		if len(pending) == 0 {
			continue
		}
		if err := pending[0].syncSharedPersistence(); err != nil {
			return err
		}
		for _, token := range pending {
			if err := token.markStableAfterSharedSync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (token *StableNamespaceToken) prepareSharedStabilize() (bool, error) {
	if token == nil {
		return false, nil
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return false, ErrResourceOwnership
	}
	if token.state.Load() == namespaceFailed {
		return false, token.stabilizeErr
	}
	if token.hasLinkedResource {
		if err := token.adapter.ValidateIdentity(token.parent, token.linkedResourceIdentity, token.newName); err != nil {
			token.stabilizeErr = err
			token.state.Store(namespaceFailed)
			return false, err
		}
	}
	return token.state.Load() != namespaceStable, nil
}

func (token *StableNamespaceToken) syncSharedPersistence() error {
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return ErrResourceOwnership
	}
	if token.state.Load() == namespaceFailed {
		return token.stabilizeErr
	}
	started := time.Now()
	err := token.adapter.Sync(token.persistence)
	token.syncs.Add(1)
	token.syncNanos.Add(uint64(time.Since(started)))
	return err
}

func (token *StableNamespaceToken) markStableAfterSharedSync() error {
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return ErrResourceOwnership
	}
	if token.state.Load() == namespaceFailed {
		return token.stabilizeErr
	}
	if token.hasLinkedResource {
		if err := token.adapter.ValidateIdentity(token.parent, token.linkedResourceIdentity, token.newName); err != nil {
			token.stabilizeErr = err
			token.state.Store(namespaceFailed)
			return err
		}
	}
	token.state.Store(namespaceStable)
	return nil
}

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
	if token.hasLinkedResource {
		if err := token.adapter.ValidateIdentity(token.parent, token.linkedResourceIdentity, token.newName); err != nil {
			token.stabilizeErr = err
			token.state.Store(namespaceFailed)
			return err
		}
	}
	err := token.adapter.Sync(token.persistence)
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
		if token.hasLinkedResource {
			token.mu.Lock()
			defer token.mu.Unlock()
			if token.state.Load() == namespaceFailed {
				return token.stabilizeErr
			}
			if err := token.adapter.ValidateIdentity(token.parent, token.linkedResourceIdentity, token.newName); err != nil {
				token.stabilizeErr = err
				token.state.Store(namespaceFailed)
				return err
			}
		}
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

// cloneStable duplicates the exact retained parent handle of an already
// stable namespace proof. It never resolves DiagnosticPath. The clone starts
// stable because the source proof's parent/child binding and namespace sync
// have already crossed their durability barrier; publication of a later root
// may therefore retain that same immutable proof without performing another
// parent-directory sync.
func (token *StableNamespaceToken) cloneStable() (*StableNamespaceToken, error) {
	if token == nil {
		return nil, nil
	}
	if err := token.validateStable(); err != nil {
		return nil, err
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() || token.state.Load() != namespaceStable || token.parent == nil {
		return nil, ErrResourceOwnership
	}
	pinned, err := duplicateStableFile(token.parent)
	if err != nil {
		return nil, fmt.Errorf("duplicate stable namespace handle: %w", err)
	}
	clone := &StableNamespaceToken{
		parent: pinned, parentIdentity: token.parentIdentity,
		linkedResourceIdentity: token.linkedResourceIdentity,
		hasLinkedResource:      token.hasLinkedResource,
		operation:              token.operation,
		oldName:                token.oldName,
		newName:                token.newName,
		diagnosticPath:         token.diagnosticPath,
		adapter:                token.adapter,
	}
	clone.state.Store(namespaceStable)
	return clone, nil
}

func (token *StableNamespaceToken) compatible(other *StableNamespaceToken) bool {
	if token.parentIdentity != other.parentIdentity || token.operation != other.operation ||
		token.oldName != other.oldName || token.newName != other.newName ||
		token.hasLinkedResource != other.hasLinkedResource {
		return false
	}
	return !token.hasLinkedResource || sameStableObject(token.linkedResourceIdentity, other.linkedResourceIdentity)
}

func (token *StableNamespaceToken) validateLinkedResource(identity StableIdentity) error {
	if token == nil {
		return nil
	}
	if !token.hasLinkedResource {
		return fmt.Errorf("%w: namespace operation %s for %q has no exact linked child", ErrUnresolvedResource, token.operation, token.newName)
	}
	if !sameStableObject(token.linkedResourceIdentity, identity) {
		return fmt.Errorf("%w: namespace child %q does not match registered resource identity", ErrResourceConflict, token.newName)
	}
	return nil
}

func sameStableObject(left, right StableIdentity) bool {
	return left.Platform == right.Platform && left.VolumeID == right.VolumeID && left.ObjectID == right.ObjectID
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
		if token.persistence != nil && token.persistence != token.parent {
			_ = token.persistence.Close()
		}
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
	if token.persistence != nil && token.persistence != token.parent {
		_ = token.persistence.Close()
	}
	_ = token.parent.Close()
}

type ResourceKindStats struct {
	Kind         ResourceKind
	PendingCount uint64
	PendingBytes uint64
	PendingAge   time.Duration
	// LogicalObligationCount may exceed PendingCount when several immutable
	// logical references share one coalesced physical pin.
	LogicalObligationCount uint64
	Flushes                uint64
	FlushDuration          time.Duration
	Syncs                  uint64
	SyncDuration           time.Duration
	// PhysicalFileSyncs counts successful producer-certified file barriers.
	// Unlike Syncs, it does not increase when SyncThrough is skipped because an
	// already-synced frontier covers the request.
	PhysicalFileSyncs        uint64
	PhysicalFileSyncDuration time.Duration
	NamespaceSyncs           uint64
	NamespaceSyncDuration    time.Duration
	ActivePins               uint64
	PinHighWater             uint64
}

var _ io.ReaderAt = (*StableResourceToken)(nil)
