package rootpublication

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
	"github.com/snissn/gomap/TreeDB/pager"
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
		// This is the same deterministically stamped mutable table used by the
		// registry and builder. Input strings remain owned by the token constructor;
		// scratch nodes own no additional keys and never escape normalization.
		byKey := newStableTable[stableLogicalObligationIndex, StableLogicalObligation](stableLogicalObligationIndexLess)
		for _, obligation := range obligations {
			if err := validateStableLogicalObligation(obligation, reachability); err != nil {
				return nil, err
			}
			key := stableLogicalObligationKey(obligation)
			if existing, ok := byKey.lookup(key); ok {
				if existing != obligation {
					return nil, fmt.Errorf("%w: logical obligation %+v has conflicting immutable checksum or digest", ErrResourceConflict, key)
				}
				continue
			}
			byKey.set(key, obligation)
		}
		// Full input capacity is intentional: the prepaid constructor plan covers
		// exactly this array even when duplicate inputs reduce logical length.
		normalized := make([]StableLogicalObligation, 0, len(obligations))
		appendNormalizedStableLogicalObligations(byKey.root, &normalized)
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
			// Ordered insertion needs no reflective sort/control allocation.
			position := len(normalized)
			normalized = append(normalized, obligation)
			for position > 0 && stableLogicalObligationLess(obligation, normalized[position-1]) {
				normalized[position] = normalized[position-1]
				position--
			}
			normalized[position] = obligation
		}
	}
	return normalized[:len(normalized):len(normalized)], nil
}

// In-order traversal writes only the already prepaid output capacity. There is
// no closure, iterator object, stack slice or second normalization engine.
func appendNormalizedStableLogicalObligations(node *stableTableNode[stableLogicalObligationIndex, StableLogicalObligation], result *[]StableLogicalObligation) {
	if node == nil {
		return
	}
	appendNormalizedStableLogicalObligations(node.left, result)
	*result = append(*result, node.value)
	appendNormalizedStableLogicalObligations(node.right, result)
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
	// ResourceOwnerShared is internal immutable-chunk ownership. Published set
	// wrappers retain/release chunk roots independently of their lifecycle phase.
	ResourceOwnerShared
	ResourceOwnerTransferred
	ResourceOwnerView
	ResourceOwnerReleased
)

type resourceOperation func(*os.File, DurableFrontier) error

// StableIndexOperationProvider is concrete named Pager backing. Its reference
// methods invoke no callbacks; no arbitrary interface implementation can run
// under a builder/child gate during shared-provider clone rollback.
// It grants no platform/backing/finite certificate.
type StableIndexOperationProvider struct {
	mu      sync.Mutex
	refs    uint64
	pager   *pager.Pager
	creator *residentcredit.Scope
}

func NewStableIndexOperationProvider(p *pager.Pager, creator *residentcredit.Scope) (*StableIndexOperationProvider, error) {
	if p == nil || creator == nil {
		return nil, ErrResourceOwnership
	}
	if err := creator.RetainOriginalLifetime(); err != nil {
		return nil, err
	}
	n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableIndexOperationProvider{})), true)
	if err == nil {
		err = creator.ReserveOriginalLifetime(n)
	}
	if err != nil {
		creator.ReleaseStableMetadata()
		return nil, err
	}
	return &StableIndexOperationProvider{refs: 1, pager: p, creator: creator}, nil
}
func (p *StableIndexOperationProvider) RetainStableResourceProvider() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refs == 0 || p.refs == ^uint64(0) || p.pager == nil {
		return ErrResourceOwnership
	}
	p.refs++
	return nil
}
func (p *StableIndexOperationProvider) ReleaseStableResourceProvider() {
	p.mu.Lock()
	if p.refs == 0 {
		p.mu.Unlock()
		panic("stable index provider reference imbalance")
	}
	p.refs--
	var creator *residentcredit.Scope
	if p.refs == 0 {
		p.pager = nil
		creator = p.creator
		p.creator = nil
	}
	p.mu.Unlock()
	if creator != nil {
		creator.ReleaseStableMetadata()
		creator = nil
	}
}
func (p *StableIndexOperationProvider) FlushStableResource(*os.File, DurableFrontier) error {
	if err := p.RetainStableResourceProvider(); err != nil {
		return err
	}
	p.ReleaseStableResourceProvider()
	return nil
}
func (p *StableIndexOperationProvider) SyncStableResource(file *os.File, _ DurableFrontier) error {
	if err := p.RetainStableResourceProvider(); err != nil {
		return err
	}
	p.mu.Lock()
	captured := p.pager
	p.mu.Unlock()
	defer func() { captured = nil; file = nil; p.ReleaseStableResourceProvider() }()
	if captured == nil {
		return ErrResourceOwnership
	}
	return captured.SyncIndexDataWithStableFile(file)
}

// StableResourceReleaseEnvironment is original-token-only release state.
// Clones have independent registry release environments and never inherit it.
type StableResourceReleaseEnvironment interface{ ReleaseStableResource() }

// StableResourceSpec is consumed by NewStableResourceToken. File is duplicated
// immediately; later operations never reopen DiagnosticPath.
type StableResourceSpec struct {
	// MetadataAccount accounts backing without replacing producer validation.
	MetadataAccount StableMetadataAccount
	// CallbackCreator retains the exact existing ordinary constructor Scope.
	// Provider and ReleaseEnvironment are named operational backing; none grant
	// generic owned/transfer/finite certification, even after terminal scrub.
	CallbackCreator    *residentcredit.Scope
	CallbackProvider   *StableIndexOperationProvider
	ReleaseEnvironment StableResourceReleaseEnvironment
	Kind               ResourceKind
	LogicalLane        string
	ResourceID         string
	Generation         uint64
	DiagnosticPath     string
	File               *os.File
	Frontier           DurableFrontier
	Digest             [32]byte
	Reachability       ReachabilityField
	Namespace          *StableNamespaceToken
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

// StableMetadataAccount is retained allocation accounting only. It grants no
// resource, namespace or registry authority. Retain/Release balance actual
// constructor owners, including a proof retained across a writer error/retry.
type StableMetadataAccount interface {
	ReserveStableMetadata(uint64) error
	RetainStableMetadata() error
	ReleaseStableMetadata()
}

var ErrStableMetadataShapeUnsupported = errors.New("rootpublication: finite metadata shape unsupported")

// These layouts are derived from Go1.26.3 linux/amd64 os.file, poll.FD,
// os.fileStat and runtime.specialfinalizer/finalizer. They account exposed
// engine-owned heap objects. Shared runtime finalizer/poll allocator tranches
// are separate process effects, not ownership of an os.File heap instance.
// The latter and registry/set backing remain separate closed admission gates.
type finiteLinuxPollFD struct {
	state             uint64
	rsema, wsema      uint32
	sysfd             int
	iovecs            unsafe.Pointer
	pollctx           uintptr
	csema, blocking   uint32
	stream, eof, file bool
}
type finiteLinuxOSFile struct {
	fd                                  finiteLinuxPollFD
	name                                string
	dir                                 unsafe.Pointer
	nonblock, stdio, appendMode, inRoot bool
}
type finiteLinuxStat struct {
	name    string
	size    int64
	mode    uint32
	modTime time.Time
	sys     [144]byte
}

func finiteStablePlatform() error {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.3" {
		return ErrStableMetadataShapeUnsupported
	}
	return nil
}
func finiteStableHandleBytes(file *os.File) (uint64, error) {
	if file == nil {
		return 0, ErrStableMetadataShapeUnsupported
	}
	if err := finiteStablePlatform(); err != nil {
		return 0, err
	}
	n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(os.File{})), true)
	if err != nil {
		return 0, err
	}
	n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(finiteLinuxOSFile{})), true)
	if err != nil {
		return 0, err
	}
	return finiteStableClassAdd(n, uint64(len(file.Name()))+uint64(len("#stable-pin")), false)
}
func finiteStableAdd(a, b uint64) (uint64, error) {
	if b > ^uint64(0)-a {
		return 0, ErrStableMetadataShapeUnsupported
	}
	return a + b, nil
}
func finiteStableCopy(value string) string {
	if value == "" {
		return ""
	}
	// Exact exposed byte backing, independent of a borrowed substring's source.
	b := make([]byte, len(value))
	copy(b, value)
	return unsafe.String(unsafe.SliceData(b), len(b))
}
func finiteStableBegin(account StableMetadataAccount, n uint64) error {
	if account == nil {
		return nil
	}
	if err := finiteStablePlatform(); err != nil {
		return err
	}
	if err := account.ReserveStableMetadata(n); err != nil {
		return err
	}
	return account.RetainStableMetadata()
}
func finiteStableObligationBytes(obligations []StableLogicalObligation) (uint64, error) {
	// Validate the whole logical input before any copied backing, table node,
	// pin or account retain. Refusal is scalar and cannot export source aliases.
	unique := uint64(0)
	for i, o := range obligations {
		if o.Class == "" || o.Kind == "" || o.Namespace == "" || o.Generation == 0 ||
			o.FileID == 0 || o.Offset < 0 || o.Length <= 0 || o.Reachability == "" ||
			o.Digest == [32]byte{} {
			return 0, ErrUnresolvedResource
		}
		// The plan is allocation-free, including conflicting duplicates. The
		// normalization table subsequently uses this same immutable key. Preserve
		// the first declaration and reject its conflicting successor before debit,
		// borrowed registry admission, copied backing, or any identity pin.
		key := stableLogicalObligationKey(o)
		duplicate := false
		for j := 0; j < i; j++ {
			if stableLogicalObligationKey(obligations[j]) != key {
				continue
			}
			if obligations[j] != o {
				return 0, ErrResourceConflict
			}
			duplicate = true
			break
		}
		if !duplicate {
			unique++
		}
	}
	// The owned copy and normalization output are two distinct allocations.
	// Each uses its actual full capacity/class; rounding their aggregate does not
	// account either retained backing. Failed normalization keeps both births.
	element := uint64(unsafe.Sizeof(StableLogicalObligation{}))
	if uint64(len(obligations)) > ^uint64(0)/element {
		return 0, ErrStableMetadataShapeUnsupported
	}
	class, err := StableBackingClassBytes(uint64(len(obligations))*element, true)
	if err != nil || class > ^uint64(0)/2 {
		return 0, ErrStableMetadataShapeUnsupported
	}
	n := class * 2
	if len(obligations) > stableLogicalObligationLinearLimit {
		// The allocation-free structural pass counted the exact distinct keys:
		// one real AVL node per first declaration. Array capacities still equal
		// the full input, including duplicates. The table control is conservatively
		// prepaid too; failed staging never refunds cumulative attempted work.
		node, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableTableNode[stableLogicalObligationIndex, StableLogicalObligation]{})), true)
		if err != nil || unique > ^uint64(0)/node {
			return 0, ErrStableMetadataShapeUnsupported
		}
		n, err = finiteStableAdd(n, unique*node)
		if err != nil {
			return 0, err
		}
		n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(stableTable[stableLogicalObligationIndex, StableLogicalObligation]{})), true)
		if err != nil {
			return 0, err
		}
	}
	for _, o := range obligations {
		for _, v := range [...]string{string(o.Class), string(o.Kind), o.Namespace, string(o.Reachability)} {
			var err error
			n, err = finiteStableClassAdd(n, uint64(len(v)), false)
			if err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}
func finiteStableOwnObligations(obligations []StableLogicalObligation) []StableLogicalObligation {
	if len(obligations) == 0 {
		return nil
	}
	out := make([]StableLogicalObligation, len(obligations))
	copy(out, obligations)
	for i := range out {
		out[i].Class = finiteStableCopy(out[i].Class)
		out[i].Kind = finiteStableCopy(out[i].Kind)
		out[i].Namespace = finiteStableCopy(out[i].Namespace)
		out[i].Reachability = ReachabilityField(finiteStableCopy(string(out[i].Reachability)))
	}
	return out
}

type StableResourceToken struct {
	finiteMetadata   bool // generic refusal remains immutable after terminal scrub
	backingCensus    BackingCensus
	backingCertified bool
	// wholeBackingOwned proves exact freshly owned physical provenance.
	// Finite transfer additionally requires backingCertified and the full census.
	// A clone census excludes shared backing and never gains this certificate
	// merely because it becomes the last reference to the pinned handle.
	wholeBackingOwned  bool
	segmentOwner       *StableSegmentOwner
	registryBorrower   *stableRegistryBorrower
	segmentRetention   StableSegmentRetention
	pinRegistry        *IdentityPinRegistry
	transferPending    bool
	metadataMu         sync.Mutex // universal short admission, never held over callbacks
	activeOperations   uint64
	releasePending     bool
	cleanupRunning     bool
	cleanupUncertain   bool
	cleanupComplete    bool
	callbackBacked     bool // immutable origin; scrubbing cannot certify a callback
	callbackCreator    *residentcredit.Scope
	callbackProvider   *StableIndexOperationProvider
	releaseEnvironment StableResourceReleaseEnvironment
	metadataAccount    StableMetadataAccount
	metadataBacking    uint64
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
	// directory pins the exact index generation/root backing this token's
	// logical view. It never retains a predecessor resource set or token.
	directory         *DependencyDirectoryV2
	stability         ResourceStability
	namespace         *StableNamespaceToken
	pinned            *os.File
	pinnedRefs        *atomic.Int64
	flush             resourceOperation
	sync              resourceOperation
	syncedFrontier    DurableFrontier
	hasSyncedFrontier bool
	onRelease         func()
	identityPin       *IdentityPin
	owner             atomic.Uint32
	released          atomic.Bool
	metrics           resourceTokenMetrics
}

func NewStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	return newStableResourceToken(spec, nil)
}

// newStableResourceToken accepts an immutable normalized obligation view only
// for exact-handle clones produced inside this package. Public producers always
// pass nil and retain the full validation/copy boundary above.
// NewStableResourceTokenWithMetadataAccount preserves the ordinary validation
// and FD identity boundary, debiting all admitted constructor backing first.
func NewStableResourceTokenWithMetadataAccount(spec StableResourceSpec, account StableMetadataAccount) (*StableResourceToken, error) {
	if account == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	return newStableResourceTokenAccount(spec, nil, account)
}
func newStableResourceToken(spec StableResourceSpec, normalized []StableLogicalObligation) (*StableResourceToken, error) {
	return newStableResourceTokenAccount(spec, normalized, spec.MetadataAccount)
}
func newStableResourceTokenAccount(spec StableResourceSpec, normalized []StableLogicalObligation, account StableMetadataAccount) (*StableResourceToken, error) {
	var err error
	account, err = inheritStableMetadataAccount(account, spec.Namespace)
	if err != nil {
		return nil, err
	}
	if account != nil && (spec.CallbackCreator != nil || spec.CallbackProvider != nil || spec.ReleaseEnvironment != nil) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	callbackCreator := spec.CallbackCreator
	callbackProvider := spec.CallbackProvider
	if callbackProvider != nil {
		callbackProvider.mu.Lock()
		sameCreator := callbackCreator != nil && callbackProvider.creator == callbackCreator && callbackProvider.refs != 0
		callbackProvider.mu.Unlock()
		if !sameCreator {
			return nil, ErrStableMetadataShapeUnsupported
		}
	}
	var flush, syncThrough resourceOperation
	var logicalObligations []StableLogicalObligation
	var pinned *os.File
	var pinnedRefs *atomic.Int64
	var identityPin *IdentityPin
	var stat os.FileInfo
	callbackHeld, providerHeld := false, false
	defer func() {
		// Drop constructor aliases before the final local retention ends.
		spec = StableResourceSpec{}
		flush, syncThrough, logicalObligations = nil, nil, nil
		pinned, pinnedRefs, identityPin, stat = nil, nil, nil, nil
		if providerHeld {
			callbackProvider.ReleaseStableResourceProvider()
		}
		callbackProvider = nil
		if callbackHeld {
			callbackCreator.ReleaseStableMetadata()
		}
		callbackCreator = nil
	}()
	if callbackCreator != nil {
		if account != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
		if err := retainStableCallbackCreator(callbackCreator, spec.File, false, spec.PinRegistry != nil); err != nil {
			return nil, err
		}
		callbackHeld = true
	}
	if callbackProvider != nil {
		if err := callbackProvider.RetainStableResourceProvider(); err != nil {
			return nil, err
		}
		providerHeld = true
	}
	var metadataBacking uint64
	retainedAccount := false
	var registryBorrower *stableRegistryBorrower
	if account != nil {
		if spec.Frontier.exactRIDs != nil || spec.StableIdentityOverride.valid() || spec.FlushThrough != nil || spec.SyncThrough != nil || spec.OnRelease != nil || spec.CallbackCreator != nil || spec.CallbackProvider != nil || spec.ReleaseEnvironment != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
		n, err := finiteStableHandleBytes(spec.File)
		if err != nil {
			return nil, err
		}
		for _, size := range [...]uint64{uint64(unsafe.Sizeof(StableResourceToken{})), uint64(unsafe.Sizeof(atomic.Int64{})), uint64(unsafe.Sizeof(IdentityPin{})), uint64(unsafe.Sizeof(finiteLinuxStat{})), uint64(unsafe.Sizeof(finiteLinuxStat{}))} {
			n, err = finiteStableClassAdd(n, size, true)
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		for _, v := range [...]string{string(spec.Kind), spec.LogicalLane, spec.ResourceID, spec.DiagnosticPath, string(spec.Reachability)} {
			n, err = finiteStableClassAdd(n, uint64(len(v)), false)
			if err != nil {
				return nil, err
			}
		}
		obligations := spec.LogicalObligations
		if normalized != nil {
			obligations = normalized
		}
		b, err := finiteStableObligationBytes(obligations)
		if err != nil {
			return nil, err
		}
		n, err = finiteStableAdd(n, b)
		if err != nil {
			return nil, err
		}
		// filepath.Clean may create a full-length transient diagnostic path.
		n, err = finiteStableClassAdd(n, uint64(len(spec.DiagnosticPath)), false)
		if err != nil {
			return nil, err
		}
		if spec.PinRegistry != nil {
			registryBorrower, err = spec.PinRegistry.acquireBorrowerWithBacking(account, n, true)
			if err != nil {
				return nil, err
			}
		} else if err := finiteStableBegin(account, n); err != nil {
			return nil, err
		}
		retainedAccount = true
		metadataBacking = n
		defer func() {
			if retainedAccount {
				registryBorrower.release()
				account.ReleaseStableMetadata()
			}
		}()
		spec.Kind = ResourceKind(finiteStableCopy(string(spec.Kind)))
		spec.LogicalLane = finiteStableCopy(spec.LogicalLane)
		spec.ResourceID = finiteStableCopy(spec.ResourceID)
		spec.DiagnosticPath = finiteStableCopy(spec.DiagnosticPath)
		spec.Reachability = ReachabilityField(finiteStableCopy(string(spec.Reachability)))
		owned := finiteStableOwnObligations(obligations)
		if normalized != nil {
			normalized = owned
		} else {
			spec.LogicalObligations = owned
		}
	}

	if account == nil && spec.Frontier.exactRIDs == nil && len(spec.LogicalObligations) == 0 && len(normalized) == 0 && spec.FlushThrough == nil && spec.SyncThrough == nil && spec.OnRelease == nil && spec.CallbackCreator == nil && spec.CallbackProvider == nil && spec.ReleaseEnvironment == nil && !spec.StableIdentityOverride.valid() {
		spec.Kind = ResourceKind(finiteStableCopy(string(spec.Kind)))
		spec.LogicalLane = finiteStableCopy(spec.LogicalLane)
		spec.ResourceID = finiteStableCopy(spec.ResourceID)
		spec.DiagnosticPath = finiteStableCopy(spec.DiagnosticPath)
		spec.Reachability = ReachabilityField(finiteStableCopy(string(spec.Reachability)))
	}

	if spec.Kind == "" || spec.ResourceID == "" || spec.Generation == 0 || spec.Reachability == "" || spec.File == nil {
		return nil, fmt.Errorf("%w: incomplete resource registration", ErrUnresolvedResource)
	}
	if err := validateDiagnosticPath(spec.DiagnosticPath); err != nil {
		return nil, err
	}
	if err := validateDurableFrontier(spec.Frontier); err != nil {
		return nil, err
	}
	logicalObligations = normalized
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
	if spec.SyncThrough == nil && spec.CallbackProvider == nil {
		// The default Windows durability barrier needs a private write-capable
		// reopen even when the producer retains only a read handle. A custom
		// sync callback owns its handle contract, so preserve the source access
		// rights (and support non-disk handles used by custom barriers).
		duplicate = duplicateStableSyncFile
	}
	pinned, err = duplicate(spec.File)
	if err != nil {
		return nil, fmt.Errorf("duplicate stable resource handle: %w", err)
	}
	pinnedRefs = &atomic.Int64{}
	pinnedRefs.Store(1)
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = pinned.Close()
		}
	}()
	stat, err = pinned.Stat()
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
	flush = spec.FlushThrough
	if flush == nil {
		// Concrete producers drain userspace buffers before registration. The
		// publication flush phase therefore has no additional file primitive;
		// SyncThrough below owns the single default content fsync.
		flush = func(*os.File, DurableFrontier) error { return nil }
	}
	syncThrough = spec.SyncThrough
	if syncThrough == nil {
		syncThrough = func(file *os.File, _ DurableFrontier) error { return stableio.SyncFile(file) }
	}
	token := &StableResourceToken{
		finiteMetadata: account != nil, metadataAccount: account, metadataBacking: metadataBacking, pinRegistry: spec.PinRegistry, registryBorrower: registryBorrower,
		kind: spec.Kind, logicalLane: spec.LogicalLane, resourceID: spec.ResourceID,
		generation: spec.Generation, diagnosticPath: filepath.ToSlash(spec.DiagnosticPath),
		identity: identity, frontier: cloneDurableFrontier(spec.Frontier), digest: spec.Digest,
		reachability: spec.Reachability, logicalObligations: logicalObligations,
		stability: stability, namespace: spec.Namespace, pinned: pinned, pinnedRefs: pinnedRefs,
		flush: flush, sync: syncThrough, onRelease: spec.OnRelease, identityPin: identityPin,
		callbackCreator: callbackCreator, callbackProvider: callbackProvider, releaseEnvironment: spec.ReleaseEnvironment,
		callbackBacked: spec.FlushThrough != nil || spec.SyncThrough != nil || spec.OnRelease != nil || callbackCreator != nil || callbackProvider != nil || spec.ReleaseEnvironment != nil,
	}
	if spec.ContentSynced {
		token.syncedFrontier = cloneDurableFrontier(spec.Frontier)
		token.hasSyncedFrontier = true
		// ContentSynced is producer certification that the exact registered
		// file frontier crossed a physical durability barrier before capture.
		token.metrics.physicalFileSyncs.Store(1)
	}
	// Exact newly owned physical provenance is independent of the finite
	// platform/class certificate. Ordinary producers use the same installed
	// handle on every platform supported by stable I/O; finite borrowing and
	// transfer still require the separate complete backing census.
	token.wholeBackingOwned = token.frontier.exactRIDs == nil && len(token.logicalObligations) == 0 && spec.FlushThrough == nil && spec.SyncThrough == nil && spec.OnRelease == nil && spec.CallbackCreator == nil && spec.CallbackProvider == nil && spec.ReleaseEnvironment == nil && !spec.StableIdentityOverride.valid()
	token.backingCertified = finiteStablePlatform() == nil && token.wholeBackingOwned
	if token.backingCertified {
		token.backingCensus = stableTokenRetainedCensus(token)
	}
	token.owner.Store(uint32(ResourceOwnerToken))
	token.metrics.registeredNanos = time.Now().UnixNano()
	if token.namespace != nil {
		if err := token.namespace.retain(); err != nil {
			return nil, err
		}
	}
	closeOnError = false
	retainedAccount = false
	callbackHeld, providerHeld = false, false
	return token, nil
}

// cloneSharedPinned retains immutable state and its actual content certificate
// without re-opening, re-statting, or certifying a larger requested frontier.
func (token *StableResourceToken) cloneSharedPinned(logicalLane, resourceID, diagnosticPath string, frontier DurableFrontier, reachability ReachabilityField, logicalObligations []StableLogicalObligation, onRelease func()) (*StableResourceToken, error) {
	if token == nil {
		return nil, ErrResourceOwnership
	}
	if err := token.beginOperation(); err != nil {
		return nil, err
	}
	defer func() { token.endOperationOutcome(recover()) }()
	token.metadataMu.Lock()
	var directory *DependencyDirectoryV2
	if token.metadataAccount == nil {
		directory = token.directory
	}
	token.metadataMu.Unlock()
	return token.cloneSharedPinnedDirectory(logicalLane, resourceID, diagnosticPath, frontier, reachability, logicalObligations, directory, onRelease)
}

func (token *StableResourceToken) cloneSharedPinnedDirectory(logicalLane, resourceID, diagnosticPath string, frontier DurableFrontier, reachability ReachabilityField, logicalObligations []StableLogicalObligation, directory *DependencyDirectoryV2, onRelease func()) (*StableResourceToken, error) {
	return token.cloneSharedPinnedAccount(logicalLane, resourceID, diagnosticPath, frontier, reachability, logicalObligations, directory, onRelease, nil, 0)
}
func (token *StableResourceToken) cloneSharedPinnedAccount(logicalLane, resourceID, diagnosticPath string, frontier DurableFrontier, reachability ReachabilityField, logicalObligations []StableLogicalObligation, directory *DependencyDirectoryV2, onRelease func(), borrowAccount StableMetadataAccount, loanBytes uint64) (*StableResourceToken, error) {
	return token.cloneSharedPinnedAccountWithCredit(logicalLane, resourceID, diagnosticPath, frontier, reachability, logicalObligations, directory, onRelease, borrowAccount, loanBytes, false, nil)
}
func (token *StableResourceToken) cloneSharedPinnedAccountWithCredit(logicalLane, resourceID, diagnosticPath string, frontier DurableFrontier, reachability ReachabilityField, logicalObligations []StableLogicalObligation, directory *DependencyDirectoryV2, onRelease func(), borrowAccount StableMetadataAccount, loanBytes uint64, prepaid bool, sourceLoanOwner *StableSegmentOwner) (*StableResourceToken, error) {
	return token.cloneSharedPinnedEnvironment(logicalLane, resourceID, diagnosticPath, frontier, reachability, logicalObligations, directory, onRelease, nil, borrowAccount, loanBytes, prepaid, sourceLoanOwner)
}
func (token *StableResourceToken) cloneSharedPinnedEnvironment(logicalLane, resourceID, diagnosticPath string, frontier DurableFrontier, reachability ReachabilityField, logicalObligations []StableLogicalObligation, directory *DependencyDirectoryV2, onRelease func(), environment StableResourceReleaseEnvironment, borrowAccount StableMetadataAccount, loanBytes uint64, prepaid bool, sourceLoanOwner *StableSegmentOwner) (*StableResourceToken, error) {
	prepaidHeld := prepaid
	defer func() {
		if prepaidHeld {
			borrowAccount.ReleaseStableMetadata()
		}
	}()
	if token == nil {
		return nil, ErrResourceOwnership
	}
	if err := token.beginOperation(); err != nil {
		return nil, err
	}
	defer func() { token.endOperationOutcome(recover()) }()
	token.metadataMu.Lock()
	forbidden := token.transferPending || token.segmentRetention != nil
	token.metadataMu.Unlock()
	if forbidden {
		return nil, ErrResourceOwnership
	}
	account, accountErr := inheritStableMetadataAccount(token.metadataAccount, token.namespace)
	if accountErr != nil {
		return nil, accountErr
	}
	if borrowAccount != nil {
		if token.callbackBacked || !token.backingCertified || account != nil && (sourceLoanOwner == nil || sourceLoanOwner.token != token) || token.namespace != nil && (!token.namespace.backingCertified || token.namespace.metadataAccount != nil && sourceLoanOwner == nil) {
			return nil, ErrStableMetadataShapeUnsupported
		}
		account = borrowAccount
	}
	callbackCreator := token.callbackCreator
	callbackProvider := token.callbackProvider
	callbackHeld, providerHeld := false, false
	defer func() {
		if providerHeld {
			callbackProvider.ReleaseStableResourceProvider()
		}
		callbackProvider = nil
		if callbackHeld {
			callbackCreator.ReleaseStableMetadata()
		}
		callbackCreator = nil
	}()
	if callbackCreator != nil {
		if account != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
		if err := retainStableCallbackCreator(callbackCreator, token.pinned, true, token.pinRegistry != nil || token.identityPin != nil); err != nil {
			return nil, err
		}
		callbackHeld = true
	}
	if callbackProvider != nil {
		if err := callbackProvider.RetainStableResourceProvider(); err != nil {
			return nil, err
		}
		providerHeld = true
	}
	var metadataBacking uint64
	retainedAccount := false
	if account != nil {
		if frontier.exactRIDs != nil || token.syncedFrontier.exactRIDs != nil || directory != nil || onRelease != nil || environment != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
		n, err := finiteStableObligationBytes(logicalObligations)
		if err != nil {
			return nil, err
		}
		n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(StableResourceToken{})), true)
		if err == nil {
			n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(IdentityPin{})), true)
		}
		if err == nil {
			n, err = finiteStableAdd(n, loanBytes)
		}
		if err != nil {
			return nil, err
		}
		for _, v := range [...]string{logicalLane, resourceID, diagnosticPath, string(reachability)} {
			n, err = finiteStableClassAdd(n, uint64(len(v)), false)
			if err != nil {
				return nil, err
			}
		}
		if !prepaid {
			if err := finiteStableBegin(account, n); err != nil {
				return nil, err
			}
		}
		retainedAccount = !prepaid
		metadataBacking = n
		defer func() {
			if retainedAccount {
				account.ReleaseStableMetadata()
			}
		}()
		logicalLane = finiteStableCopy(logicalLane)
		resourceID = finiteStableCopy(resourceID)
		diagnosticPath = finiteStableCopy(diagnosticPath)
		reachability = ReachabilityField(finiteStableCopy(string(reachability)))
		logicalObligations = finiteStableOwnObligations(logicalObligations)
	}
	if account == nil {
		logicalLane = finiteStableCopy(logicalLane)
		resourceID = finiteStableCopy(resourceID)
		diagnosticPath = finiteStableCopy(diagnosticPath)
		reachability = ReachabilityField(finiteStableCopy(string(reachability)))
	}
	if err := token.retainPinned(); err != nil {
		return nil, err
	}
	retainedPinned := true
	defer func() {
		if retainedPinned {
			token.releasePinnedReference()
		}
	}()
	if directory != nil {
		if err := directory.Retain(); err != nil {
			return nil, err
		}
		defer func() {
			if retainedPinned {
				directory.Release()
			}
		}()
	}
	var identityPin *IdentityPin
	if token.pinRegistry != nil || token.identityPin != nil {
		registry := token.pinRegistry
		if registry == nil {
			registry = token.identityPin.registry
		}
		var err error
		identityPin, err = registry.Pin(token.identity)
		if err != nil {
			return nil, fmt.Errorf("pin stable resource identity: %w", err)
		}
		defer func() {
			if retainedPinned {
				identityPin.Release()
			}
		}()
	}
	if token.namespace != nil {
		if err := token.namespace.retain(); err != nil {
			return nil, err
		}
	}
	inheritedBorrower := token.registryBorrower
	if borrowAccount != nil {
		inheritedBorrower = nil
	}
	if inheritedBorrower != nil {
		inheritedBorrower.retain()
	}
	cloned := &StableResourceToken{
		finiteMetadata: account != nil, metadataAccount: account, metadataBacking: metadataBacking, pinRegistry: token.pinRegistry, registryBorrower: inheritedBorrower,
		kind: token.kind, logicalLane: logicalLane, resourceID: resourceID,
		generation: token.generation, diagnosticPath: diagnosticPath,
		identity: token.identity, frontier: cloneDurableFrontier(frontier), digest: token.digest,
		reachability: reachability, logicalObligations: stableLogicalObligationList(logicalObligations),
		directory: directory,
		stability: token.stability, namespace: token.namespace, pinned: token.pinned, pinnedRefs: token.pinnedRefs,
		flush: token.flush, sync: token.sync,
		syncedFrontier: cloneDurableFrontier(token.syncedFrontier), hasSyncedFrontier: token.hasSyncedFrontier,
		onRelease: onRelease, releaseEnvironment: environment, identityPin: identityPin,
		callbackCreator: callbackCreator, callbackProvider: callbackProvider,
		callbackBacked: token.callbackBacked || onRelease != nil || environment != nil,
	}
	cloned.backingCertified = !cloned.callbackBacked && token.backingCertified && frontier.exactRIDs == nil && len(logicalObligations) == 0 && directory == nil && onRelease == nil
	if cloned.backingCertified {
		cloned.backingCensus = stableClonedTokenRetainedCensus(cloned)
	}
	// Clone-owned census is partial even if all earlier aliases later release.
	cloned.wholeBackingOwned = false
	cloned.owner.Store(uint32(ResourceOwnerToken))
	cloned.metrics.registeredNanos = time.Now().UnixNano()
	if cloned.hasSyncedFrontier {
		cloned.metrics.physicalFileSyncs.Store(1)
	}
	retainedPinned = false
	retainedAccount = false
	prepaidHeld = false
	callbackHeld, providerHeld = false, false
	return cloned, nil
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

// MetadataBacking reports only constructor-stamped accounting provenance.
func (token *StableResourceToken) MetadataBacking() (uint64, bool) {
	if token == nil {
		return 0, false
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	return token.metadataBacking, token.metadataAccount != nil
}

// Mutable backing admission always uses metadataMu, including ordinary tokens.
// Immutable ordinary diagnostics survive terminal release; namespace authority
// and operational provider aliases are revoked separately.
func (token *StableResourceToken) Kind() ResourceKind {
	if token == nil {
		return ""
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return ""
	}
	return token.kind
}
func (token *StableResourceToken) LogicalLane() string {
	if token == nil {
		return ""
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return ""
	}
	return token.logicalLane
}
func (token *StableResourceToken) ResourceID() string {
	if token == nil {
		return ""
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return ""
	}
	return token.resourceID
}
func (token *StableResourceToken) Generation() uint64 { return token.generation }
func (token *StableResourceToken) DiagnosticPath() string {
	if token == nil {
		return ""
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return ""
	}
	return token.diagnosticPath
}
func (token *StableResourceToken) Identity() StableIdentity { return token.identity }
func (token *StableResourceToken) Frontier() DurableFrontier {
	if token == nil {
		return DurableFrontier{}
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil && token.frontier.exactRIDs != nil {
		return DurableFrontier{}
	}
	return cloneDurableFrontier(token.frontier)
}
func (token *StableResourceToken) Digest() [32]byte { return token.digest }
func (token *StableResourceToken) Reachability() ReachabilityField {
	if token == nil {
		return ""
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return ""
	}
	return token.reachability
}
func (token *StableResourceToken) Namespace() *StableNamespaceToken {
	if token == nil {
		return nil
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.released.Load() || token.requireMetadataExportLocked() != nil {
		return nil
	}
	return token.namespace
}
func (token *StableResourceToken) LogicalObligations() []StableLogicalObligation {
	if token == nil {
		return nil
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.requireMetadataExportLocked() != nil {
		return nil
	}
	return cloneStableLogicalObligations(token.logicalObligations)
}

var ErrStableResourceOperationBusy = errors.New("stable resource has an admitted operation or uncertain cleanup")

// beginOperation admits only a short actual call. No callbacks, I/O or waiting
// run under metadataMu; Release can close admission reentrantly without joining
// the operation that invoked it.
func (token *StableResourceToken) beginOperation() error {
	if token == nil {
		return ErrResourceOwnership
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.released.Load() || token.transferPending || token.cleanupUncertain || token.activeOperations == ^uint64(0) {
		return ErrResourceOwnership
	}
	token.activeOperations++
	return nil
}

// endOperationOutcome receives recover at the admitted caller's defer. A panic
// preserves the actual token/provider/creator debt and never replays cleanup.
func (token *StableResourceToken) endOperationOutcome(value any) {
	if value != nil {
		token.metadataMu.Lock()
		token.cleanupUncertain = true
		token.metadataMu.Unlock()
	}
	token.endOperation()
	if value != nil {
		panic(value)
	}
}
func (token *StableResourceToken) endOperation() {
	token.metadataMu.Lock()
	if token.activeOperations == 0 {
		token.metadataMu.Unlock()
		panic("stable resource operation imbalance")
	}
	token.activeOperations--
	finish := token.activeOperations == 0 && token.releasePending && !token.cleanupRunning && !token.cleanupUncertain && !token.cleanupComplete
	token.metadataMu.Unlock()
	if finish {
		_ = token.finishRelease(nil)
	}
}

func (token *StableResourceToken) FlushThrough() error {
	return token.runContentOperation(DurableFrontier{}, true, true)
}
func (token *StableResourceToken) flushThrough(frontier DurableFrontier) error {
	return token.runContentOperation(frontier, false, true)
}
func (token *StableResourceToken) SyncThrough() error {
	return token.runContentOperation(DurableFrontier{}, true, false)
}
func (token *StableResourceToken) syncThrough(frontier DurableFrontier) error {
	return token.runContentOperation(frontier, false, false)
}
func (token *StableResourceToken) runContentOperation(frontier DurableFrontier, registered, flushOnly bool) error {
	if err := token.beginOperation(); err != nil {
		return err
	}
	file, provider, operation := token.pinned, token.callbackProvider, token.sync
	if registered {
		frontier = token.frontier
	}
	if flushOnly {
		operation = token.flush
	}
	defer func() {
		// The operation's final edge follows ALL local provider/backing aliases.
		file, provider, operation = nil, nil, nil
		frontier = DurableFrontier{}
		token.endOperationOutcome(recover())
	}()
	if file == nil || provider == nil && operation == nil {
		return ErrResourceOwnership
	}
	started := time.Now()
	var err error
	if flushOnly {
		if provider != nil {
			err = provider.FlushStableResource(file, frontier)
		} else {
			err = operation(file, frontier)
		}
		token.metrics.flushes.Add(1)
		token.metrics.flushNanos.Add(uint64(time.Since(started)))
		return err
	}
	if !token.hasSyncedFrontier || !durableFrontierCovers(token.syncedFrontier, frontier) {
		physicalStarted := time.Now()
		if provider != nil {
			err = provider.SyncStableResource(file, frontier)
		} else {
			err = operation(file, frontier)
		}
		if err == nil {
			token.metrics.physicalFileSyncs.Add(1)
			token.metrics.physicalFileSyncNanos.Add(uint64(time.Since(physicalStarted)))
		}
	}
	token.metrics.syncs.Add(1)
	token.metrics.syncNanos.Add(uint64(time.Since(started)))
	return err
}

// durableFrontierCovers reads already-validated, privately owned frontiers.
// Keep RIDs() as the owned-copy boundary for callers outside this package.
func durableFrontierCovers(stable, required DurableFrontier) bool {
	if stable.Bytes < required.Bytes || stable.MaxLSN < required.MaxLSN {
		return false
	}
	if required.exactRIDs == nil || len(required.exactRIDs.values) == 0 {
		return true
	}
	if stable.exactRIDs == nil {
		return false
	}
	stableRIDs, requiredRIDs := stable.exactRIDs.values, required.exactRIDs.values
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
	if err := token.beginOperation(); err != nil {
		return 0, err
	}
	file := token.pinned
	defer func() { file = nil; token.endOperationOutcome(recover()) }()
	if file == nil {
		return 0, ErrResourceOwnership
	}
	return file.ReadAt(dst, offset)
}

// WithPinnedFile preserves the ordinary scoped-handle API. The actual admitted
// call pins its local alias through a concurrent or reentrant ordinary Release.
func (token *StableResourceToken) WithPinnedFile(fn func(*os.File) error) error {
	if fn == nil {
		return ErrResourceOwnership
	}
	if err := token.beginOperation(); err != nil {
		return err
	}
	file := token.pinned
	defer func() { file, fn = nil, nil; token.endOperationOutcome(recover()) }()
	if err := token.RequireMetadataExport(); err != nil {
		return err
	}
	if file == nil {
		return ErrResourceOwnership
	}
	return fn(file)
}

func (token *StableResourceToken) Release() error {
	return token.releaseFromWithTerminal(ResourceOwnerToken, nil)
}

// ReleaseWithTerminal uses a transient trusted consumer. Complete reservation
// precedes the first owner/pin/FD/account mutation.
func (token *StableResourceToken) ReleaseWithTerminal(consumer StableSegmentTerminalConsumer) error {
	if token == nil {
		return nil
	}
	token.metadataMu.Lock()
	if token.cleanupUncertain || token.activeOperations != 0 || token.cleanupRunning {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	token.metadataMu.Unlock()
	if ResourceOwnerState(token.owner.Load()) == ResourceOwnerReleased {
		return nil
	}
	if ResourceOwnerState(token.owner.Load()) != ResourceOwnerToken {
		return ErrResourceOwnership
	}
	joined := false
	if consumer != nil {
		var err error
		joined, err = consumer.BeginTerminalRelease()
		if err != nil {
			return err
		}
		defer consumer.EndTerminalRelease(joined)
	}
	if err := prepareStableSegmentTerminalTokens([]*StableResourceToken{token}, consumer); err != nil {
		return err
	}
	return token.releaseFromWithTerminal(ResourceOwnerToken, consumer)
}

func (token *StableResourceToken) claim(owner ResourceOwnerState) error {
	if token == nil {
		return ErrResourceOwnership
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.released.Load() || !token.owner.CompareAndSwap(uint32(ResourceOwnerToken), uint32(owner)) {
		return ErrResourceOwnership
	}
	return nil
}
func (token *StableResourceToken) transferLocked(from, to ResourceOwnerState) error {
	if token.released.Load() || !token.owner.CompareAndSwap(uint32(from), uint32(to)) {
		return ErrResourceOwnership
	}
	return nil
}
func (token *StableResourceToken) transfer(from, to ResourceOwnerState) error {
	if token == nil {
		return ErrResourceOwnership
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	return token.transferLocked(from, to)
}
func (token *StableResourceToken) releaseFrom(owner ResourceOwnerState) error {
	return token.releaseFromWithTerminal(owner, nil)
}
func (token *StableResourceToken) releaseFromWithTerminal(owner ResourceOwnerState, consumer StableSegmentTerminalConsumer) error {
	if token == nil {
		return nil
	}
	token.metadataMu.Lock()
	if token.cleanupUncertain {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	if token.owner.Load() == uint32(ResourceOwnerReleased) && (token.cleanupComplete || token.releasePending && !token.cleanupUncertain) {
		token.metadataMu.Unlock()
		return nil
	}
	if token.owner.Load() != uint32(owner) {
		token.metadataMu.Unlock()
		return ErrResourceOwnership
	}
	_, checkedCleanup := token.releaseEnvironment.(StableResourceCleanupEnvironmentV1)
	if token.transferPending || (checkedCleanup || consumer != nil || token.metadataAccount != nil || token.segmentRetention != nil) && token.activeOperations != 0 {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	retention := token.segmentRetention
	token.metadataMu.Unlock()
	if err := validateStableSegmentTerminalRetention(retention, consumer); err != nil {
		return err
	}
	token.metadataMu.Lock()
	if token.owner.Load() == uint32(ResourceOwnerReleased) && (token.cleanupComplete || token.releasePending && !token.cleanupUncertain) {
		token.metadataMu.Unlock()
		return nil
	}
	if token.segmentRetention != retention || token.owner.Load() != uint32(owner) {
		token.metadataMu.Unlock()
		return ErrResourceOwnership
	}
	if (checkedCleanup || consumer != nil || token.metadataAccount != nil || retention != nil) && token.activeOperations != 0 {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	token.released.Store(true)
	token.releasePending = true
	if retention == nil && !checkedCleanup {
		token.owner.Store(uint32(ResourceOwnerReleased))
	}
	pending := token.activeOperations != 0 || token.cleanupRunning
	token.metadataMu.Unlock()
	if pending {
		if checkedCleanup {
			return ErrStableResourceOperationBusy
		}
		return nil
	}
	result := token.finishRelease(consumer)
	if (retention != nil || checkedCleanup) && token.CleanupCompleteV1() {
		token.metadataMu.Lock()
		token.owner.Store(uint32(ResourceOwnerReleased))
		token.metadataMu.Unlock()
	}
	return result
}
func (token *StableResourceToken) releasePinned() { _ = token.releasePinnedWithTerminal(nil) }
func (token *StableResourceToken) releasePinnedWithTerminal(consumer StableSegmentTerminalConsumer) error {
	token.metadataMu.Lock()
	_, checkedCleanup := token.releaseEnvironment.(StableResourceCleanupEnvironmentV1)
	if token.cleanupUncertain || token.activeOperations != 0 && (checkedCleanup || consumer != nil || token.metadataAccount != nil || token.segmentRetention != nil) {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	token.released.Store(true)
	token.releasePending = true
	pending := token.activeOperations != 0 || token.cleanupRunning
	token.metadataMu.Unlock()
	if pending {
		if checkedCleanup {
			return ErrStableResourceOperationBusy
		}
		return nil
	}
	return token.finishRelease(consumer)
}

// finishRelease performs each existing ordinary cleanup phase once. A panic
// retains the SAME token's remaining aliases/creator as uncertain custody;
// neither reentrant Release nor later Release silently replays the callback.
func (token *StableResourceToken) finishRelease(consumer StableSegmentTerminalConsumer) (result error) {
	token.metadataMu.Lock()
	if token.cleanupComplete {
		token.metadataMu.Unlock()
		return nil
	}
	if token.cleanupUncertain || token.activeOperations != 0 {
		token.metadataMu.Unlock()
		return ErrStableResourceOperationBusy
	}
	if token.cleanupRunning {
		_, checked := token.releaseEnvironment.(StableResourceCleanupEnvironmentV1)
		token.metadataMu.Unlock()
		if checked {
			return ErrStableResourceOperationBusy
		}
		return nil
	}
	token.cleanupRunning = true
	file, refs, namespace, pin, directory := token.pinned, token.pinnedRefs, token.namespace, token.identityPin, token.directory
	onRelease, environment, provider, creator := token.onRelease, token.releaseEnvironment, token.callbackProvider, token.callbackCreator
	token.metadataMu.Unlock()
	defer func() {
		file, refs, namespace, pin, directory = nil, nil, nil, nil, nil
		onRelease, environment, provider, creator = nil, nil, nil, nil
		token.metadataMu.Lock()
		token.cleanupRunning = false
		if value := recover(); value != nil {
			token.cleanupUncertain = true
			token.metadataMu.Unlock()
			panic(value)
		}
		token.metadataMu.Unlock()
	}()
	if file != nil {
		if refs == nil || refs.Add(-1) == 0 {
			_ = file.Close()
		}
		file, refs = nil, nil
		token.metadataMu.Lock()
		token.pinned, token.pinnedRefs = nil, nil
		token.metadataMu.Unlock()
	}
	if namespace != nil {
		namespace.release()
		namespace = nil
		token.metadataMu.Lock()
		token.namespace = nil
		token.metadataMu.Unlock()
	}
	if pin != nil {
		pin.Release()
		pin = nil
		token.metadataMu.Lock()
		token.identityPin = nil
		token.metadataMu.Unlock()
	}
	if directory != nil {
		directory.Release()
		directory = nil
		token.metadataMu.Lock()
		token.directory = nil
		token.metadataMu.Unlock()
	}
	if onRelease != nil {
		onRelease()
		onRelease = nil
		token.metadataMu.Lock()
		token.onRelease = nil
		token.metadataMu.Unlock()
	}
	if checked, ok := environment.(StableResourceCleanupEnvironmentV1); ok {
		outcome, err := checked.AdvanceStableResourceCleanupV1()
		result = err
		if !outcome.Complete() {
			if result == nil {
				result = ErrStableResourceOperationBusy
			}
			return result
		}
	} else if environment != nil {
		environment.ReleaseStableResource()
	}
	environment = nil
	token.metadataMu.Lock()
	token.onRelease, token.releaseEnvironment = nil, nil
	token.metadataMu.Unlock()
	token.metadataMu.Lock()
	token.flush, token.sync = nil, nil
	token.metadataMu.Unlock()
	if provider != nil {
		provider.ReleaseStableResourceProvider()
		provider = nil
		token.metadataMu.Lock()
		token.callbackProvider = nil
		token.metadataMu.Unlock()
	}
	token.metadataMu.Lock()
	segmentOwner, retention, borrower := token.segmentOwner, token.segmentRetention, token.registryBorrower
	token.metadataMu.Unlock()
	defer func() { segmentOwner, retention, borrower = nil, nil, nil }()
	if segmentOwner != nil {
		segmentOwner.release()
		segmentOwner = nil
		token.metadataMu.Lock()
		token.segmentOwner = nil
		token.metadataMu.Unlock()
	}
	if retention != nil {
		if err := releaseStableSegmentTerminalRetention(retention, consumer); err != nil {
			return err
		}
		retention = nil
		token.metadataMu.Lock()
		token.segmentRetention = nil
		token.metadataMu.Unlock()
	}
	if borrower != nil {
		borrower.release()
		borrower = nil
		token.metadataMu.Lock()
		token.registryBorrower = nil
		token.metadataMu.Unlock()
	}
	token.metadataMu.Lock()
	account := token.metadataAccount
	if account != nil {
		token.kind, token.logicalLane, token.resourceID, token.diagnosticPath = "", "", "", ""
		token.reachability = ""
		token.logicalObligations = nil
		token.metadataAccount = nil
	}
	token.callbackCreator = nil
	token.cleanupComplete = true
	token.releasePending = false
	token.metadataMu.Unlock()
	if account != nil {
		account.ReleaseStableMetadata()
		account = nil
	}
	// Provider and callback locals are scrubbed BEFORE this final token edge.
	if creator != nil {
		creator.ReleaseStableMetadata()
		creator = nil
	}
	return result
}

// CleanupCompleteV1 reports actual effects/local-scrub completion, never admission closure.
func (token *StableResourceToken) CleanupCompleteV1() bool {
	if token == nil {
		return true
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	return token.cleanupComplete
}

// retainStableCallbackCreator prepays known token/pin/control/handle classes
// before their births. Hidden func backing and non-Linux private FD layout stay
// ordinary-only, permanently callback-backed and uncertified.
func retainStableCallbackCreator(creator *residentcredit.Scope, file *os.File, shared, pin bool) error {
	if err := creator.RetainOriginalLifetime(); err != nil {
		return err
	}
	var plan stableBackingSizePlan
	plan.add(uint64(unsafe.Sizeof(StableResourceToken{})), true)
	if !shared {
		plan.add(uint64(unsafe.Sizeof(atomic.Int64{})), true)
		if finiteStablePlatform() == nil {
			n, err := finiteStableHandleBytes(file)
			if err != nil {
				creator.ReleaseStableMetadata()
				return err
			}
			plan.bytes, plan.err = finiteStableAdd(plan.bytes, n)
			plan.add(uint64(unsafe.Sizeof(finiteLinuxStat{})), true)
			plan.add(uint64(unsafe.Sizeof(finiteLinuxStat{})), true)
		} else {
			plan.add(uint64(unsafe.Sizeof(os.File{})), true)
		}
	}
	if pin {
		plan.add(uint64(unsafe.Sizeof(IdentityPin{})), true)
	}
	if plan.err != nil {
		creator.ReleaseStableMetadata()
		return plan.err
	}
	if err := creator.ReserveOriginalLifetime(plan.bytes); err != nil {
		creator.ReleaseStableMetadata()
		return err
	}
	return nil
}

func (token *StableResourceToken) releasePinnedReference() {
	if token.pinnedRefs == nil || token.pinnedRefs.Add(-1) == 0 {
		_ = token.pinned.Close()
	}
}

func (token *StableResourceToken) retainPinned() error {
	if token == nil || token.pinned == nil || token.pinnedRefs == nil {
		return ErrResourceOwnership
	}
	for refs := token.pinnedRefs.Load(); refs > 0; refs = token.pinnedRefs.Load() {
		if refs == int64(^uint64(0)>>1) {
			return ErrResourceOwnership
		}
		if token.pinnedRefs.CompareAndSwap(refs, refs+1) {
			return nil
		}
	}
	return ErrResourceOwnership
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
	backingCensus          BackingCensus
	backingCertified       bool
	metadataAccount        StableMetadataAccount
	metadataBacking        uint64
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
	backingCensus    BackingCensus
	backingCertified bool
	metadataAccount  StableMetadataAccount
	metadataBacking  uint64
	parent           *os.File
	persistence      *os.File
	parentID         StableIdentity
	childID          StableIdentity
	name             string
	adapter          namespacePersistenceAdapter
	released         atomic.Bool
	syncs            atomic.Uint64
	syncNanos        atomic.Uint64
	mu               sync.Mutex
}

// NewStableNamespaceCreationProof validates the exact parent/child link and
// performs the sole namespace sync for a just-created child.
func NewStableNamespaceCreationProof(parent, child *os.File, name string) (*StableNamespaceCreationProof, error) {
	return newStableNamespaceCreationProof(parent, child, name, nativeNamespaceAdapter{})
}

func NewStableNamespaceCreationProofWithMetadataAccount(parent, child *os.File, name string, account StableMetadataAccount) (*StableNamespaceCreationProof, error) {
	if account == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	return newStableNamespaceCreationProofAccount(parent, child, name, nativeNamespaceAdapter{}, account)
}
func newStableNamespaceCreationProof(parent, child *os.File, name string, adapter namespacePersistenceAdapter) (*StableNamespaceCreationProof, error) {
	return newStableNamespaceCreationProofAccount(parent, child, name, adapter, nil)
}
func newStableNamespaceCreationProofAccount(parent, child *os.File, name string, adapter namespacePersistenceAdapter, account StableMetadataAccount) (*StableNamespaceCreationProof, error) {
	var backing uint64
	retainedAccount := false
	if account != nil {
		if _, ok := adapter.(nativeNamespaceAdapter); !ok {
			return nil, ErrStableMetadataShapeUnsupported
		}
		n, err := finiteStableNamespaceBytes(parent, name, "", uint64(unsafe.Sizeof(StableNamespaceCreationProof{})))
		if err != nil {
			return nil, err
		}
		if err := finiteStableBegin(account, n); err != nil {
			return nil, err
		}
		backing = n
		retainedAccount = true
		defer func() {
			if retainedAccount {
				account.ReleaseStableMetadata()
			}
		}()
		name = finiteStableCopy(name)
	}

	if account == nil {
		name = finiteStableCopy(name)
	}
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
	proof := &StableNamespaceCreationProof{metadataAccount: account, metadataBacking: backing, parent: pinned, persistence: persistence, parentID: parentID, childID: childID, name: name, adapter: adapter}
	if _, ok := adapter.(nativeNamespaceAdapter); ok {
		proof.backingCertified = finiteStablePlatform() == nil
		var p backingLayout
		p.add(uint64(unsafe.Sizeof(StableNamespaceCreationProof{})), true)
		p.file(pinned)
		if persistence != pinned {
			p.file(persistence)
		}
		p.string(name)
		proof.backingCensus = p.census
	}
	proof.syncs.Store(1)
	proof.syncNanos.Store(syncNanos)
	retainedAccount = false
	return proof, nil
}

func finiteStableNamespaceBytes(parent *os.File, name, path string, wrapper uint64) (uint64, error) {
	n, err := finiteStableHandleBytes(parent)
	if err != nil {
		return 0, err
	}
	// ValidateLink/ValidateIdentity opens one transient exact child. Its name is
	// the complete parent/name allocation, not merely the basename substring.
	for _, layout := range [...]struct {
		bytes uint64
		scan  bool
	}{
		{uint64(unsafe.Sizeof(os.File{})), true}, {uint64(unsafe.Sizeof(finiteLinuxOSFile{})), true},
		{uint64(len(parent.Name())) + 1 + uint64(len(name)), false}, {wrapper, true},
		{uint64(len(name)), false}, {uint64(len(path)), false}, {uint64(len(path)), false},
		// A failing file primitive can construct its owned PathError wrapper.
		{uint64(unsafe.Sizeof(os.PathError{})), true},
	} {
		n, err = finiteStableClassAdd(n, layout.bytes, layout.scan)
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

// Bind returns an already-stable normal namespace token after proving the
// retained parent still links the original child. It never syncs again.
func (proof *StableNamespaceCreationProof) Bind(parent *os.File, parentGeneration uint64, name, diagnosticPath string) (*StableNamespaceToken, error) {
	return proof.bindAccount(parent, parentGeneration, name, diagnosticPath, nil)
}
func (proof *StableNamespaceCreationProof) BindWithMetadataAccount(parent *os.File, parentGeneration uint64, name, diagnosticPath string, account StableMetadataAccount) (*StableNamespaceToken, error) {
	if account == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	return proof.bindAccount(parent, parentGeneration, name, diagnosticPath, account)
}
func (proof *StableNamespaceCreationProof) bindAccount(parent *os.File, parentGeneration uint64, name, diagnosticPath string, account StableMetadataAccount) (*StableNamespaceToken, error) {
	if proof == nil {
		return nil, ErrResourceOwnership
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if proof.released.Load() {
		return nil, ErrResourceOwnership
	}
	if parentGeneration == 0 {
		return nil, fmt.Errorf("%w: namespace creation proof binding requires a parent generation", ErrUnresolvedResource)
	}
	if parent == nil || name != proof.name || !stableChildBaseName(name) {
		return nil, fmt.Errorf("%w: namespace creation proof binding differs from the exact parent or child name", ErrResourceConflict)
	}
	if account == nil {
		account = proof.metadataAccount
	}
	// An already-accounted proof may only derive objects in the same owner.
	if proof.metadataAccount != nil && (!reflect.ValueOf(account).Comparable() || !reflect.ValueOf(proof.metadataAccount).Comparable() || account != proof.metadataAccount) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	var backing uint64
	retainedAccount := false
	if account != nil {
		n, err := finiteStableNamespaceBytes(proof.parent, proof.name, diagnosticPath, uint64(unsafe.Sizeof(StableNamespaceToken{})))
		if err != nil {
			return nil, err
		}
		if err := finiteStableBegin(account, n); err != nil {
			return nil, err
		}
		backing = n
		retainedAccount = true
		defer func() {
			if retainedAccount {
				account.ReleaseStableMetadata()
			}
		}()
		diagnosticPath = finiteStableCopy(diagnosticPath)
		name = finiteStableCopy(name)
	}

	if account == nil {
		name = finiteStableCopy(name)
		diagnosticPath = finiteStableCopy(diagnosticPath)
	}

	if proof == nil || proof.released.Load() {
		return nil, ErrResourceOwnership
	}
	if err := validateDiagnosticPath(diagnosticPath); err != nil {
		return nil, err
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
	token := &StableNamespaceToken{metadataAccount: account, metadataBacking: backing, parent: pinned, parentIdentity: parentID, persistence: pinned, persistenceIdentity: persistenceID, linkedResourceIdentity: proof.childID, hasLinkedResource: true, operation: NamespaceCreate, newName: name, diagnosticPath: filepath.ToSlash(diagnosticPath), adapter: proof.adapter}
	if _, ok := proof.adapter.(nativeNamespaceAdapter); ok {
		token.backingCertified = finiteStablePlatform() == nil
		token.backingCensus = stableNamespaceRetainedCensus(token)
	}
	token.state.Store(namespaceStable)
	token.syncs.Store(proof.syncs.Load())
	token.syncNanos.Store(proof.syncNanos.Load())
	retainedAccount = false
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
	proof.name = ""
	proof.adapter = nil
	if proof.metadataAccount != nil {
		proof.metadataAccount.ReleaseStableMetadata()
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

func NewStableNamespaceTokenWithMetadataAccount(spec StableNamespaceSpec, account StableMetadataAccount) (*StableNamespaceToken, error) {
	if account == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	return newStableNamespaceTokenAccount(spec, nativeNamespaceAdapter{}, account)
}
func newStableNamespaceToken(spec StableNamespaceSpec, adapter namespacePersistenceAdapter) (*StableNamespaceToken, error) {
	return newStableNamespaceTokenAccount(spec, adapter, nil)
}
func newStableNamespaceTokenAccount(spec StableNamespaceSpec, adapter namespacePersistenceAdapter, account StableMetadataAccount) (*StableNamespaceToken, error) {
	var backing uint64
	retainedAccount := false
	if account != nil {
		if _, ok := adapter.(nativeNamespaceAdapter); !ok {
			return nil, ErrStableMetadataShapeUnsupported
		}
		n, err := finiteStableNamespaceBytes(spec.Parent, spec.NewName, spec.DiagnosticPath, uint64(unsafe.Sizeof(StableNamespaceToken{})))
		if err != nil {
			return nil, err
		}
		n, err = finiteStableClassAdd(n, uint64(len(spec.OldName)), false)
		if err != nil {
			return nil, err
		}
		if err := finiteStableBegin(account, n); err != nil {
			return nil, err
		}
		backing = n
		retainedAccount = true
		defer func() {
			if retainedAccount {
				account.ReleaseStableMetadata()
			}
		}()
		spec.OldName = finiteStableCopy(spec.OldName)
		spec.NewName = finiteStableCopy(spec.NewName)
		spec.DiagnosticPath = finiteStableCopy(spec.DiagnosticPath)
	}

	if account == nil {
		spec.OldName = finiteStableCopy(spec.OldName)
		spec.NewName = finiteStableCopy(spec.NewName)
		spec.DiagnosticPath = finiteStableCopy(spec.DiagnosticPath)
	}

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
	token := &StableNamespaceToken{
		metadataAccount: account, metadataBacking: backing,
		parent: pinned, parentIdentity: identity, persistence: persistence, persistenceIdentity: persistenceIdentity, linkedResourceIdentity: linkedIdentity,
		hasLinkedResource: hasLinkedResource, operation: spec.Operation,
		oldName: spec.OldName, newName: spec.NewName,
		diagnosticPath: filepath.ToSlash(spec.DiagnosticPath), adapter: adapter,
	}
	if _, ok := adapter.(nativeNamespaceAdapter); ok {
		token.backingCertified = finiteStablePlatform() == nil
		token.backingCensus = stableNamespaceRetainedCensus(token)
	}
	retainedAccount = false
	return token, nil
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

// OpenStableParentForRetention preserves the same exact physical opening as
// OpenStableParent, with a fixed diagnostic Name so retained metadata has a
// path-independent byte bound. The Name is never namespace authority.
func OpenStableParentForRetention(path string) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: stable parent path is empty", ErrUnresolvedResource)
	}
	return openStableParentNamed(path, "stable-retirement-parent")
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
	// Group scratch has no finite owner. Scan every operand before staging it.
	for _, token := range tokens {
		if token != nil && token.metadataAccount != nil {
			return ErrStableMetadataShapeUnsupported
		}
	}
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
			if token.released.Load() {
				return ErrResourceOwnership
			}
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
	// Generic namespace cloning has no finite set/registry lifetime contract.
	// Refuse before validation, descriptor duplication or wrapper allocation.
	if token.metadataAccount != nil {
		return nil, ErrStableMetadataShapeUnsupported
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
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return ErrResourceOwnership
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
		token.releaseMetadataAccount()
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
	token.releaseMetadataAccount()
}
func (token *StableNamespaceToken) releaseMetadataAccount() {
	if token.metadataAccount != nil {
		token.oldName = ""
		token.newName = ""
		token.diagnosticPath = ""
		token.adapter = nil
		token.parent, token.persistence = nil, nil
		token.metadataAccount.ReleaseStableMetadata()
	}
}

type ResourceKindStats struct {
	Kind         ResourceKind
	PendingCount uint64
	PendingBytes uint64
	PendingAge   time.Duration
	// LogicalObligationCount may exceed PendingCount when several immutable
	// logical references share one coalesced physical pin.
	LogicalObligationCount          uint64
	LogicalObligationCountAvailable bool
	Flushes                         uint64
	FlushDuration                   time.Duration
	Syncs                           uint64
	SyncDuration                    time.Duration
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

// StableOwnedOldName and StableOwnedNewName follow the metadata export boundary;
// ordinary producer caches may retain their immutable constructor-owned names.
func (t *StableNamespaceToken) StableOwnedOldName() string {
	if t == nil || t.metadataAccount != nil {
		return ""
	}
	return t.oldName
}
func (t *StableNamespaceToken) StableOwnedNewName() string {
	if t == nil || t.metadataAccount != nil {
		return ""
	}
	return t.newName
}
