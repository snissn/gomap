package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafGenerationPackAuthoritySegment struct {
	created        rewriteCreatedSegment
	handle         *os.File
	identity       rootpublication.StableIdentity
	frontierBytes  uint64
	digest         [32]byte
	pin            *rootpublication.IdentityPin
	observed       bool
	installed      bool
	referenced     bool
	descriptorSeen bool
	handleCharge   uint64
}

// leafGenerationPackPromotionAuthority owns exact handles and deletion pins
// from staging capture through the alternate-meta publication boundary. It is
// deliberately local to packed promotion and does not consume the future
// publication-seal authority tracked by #3679.
type leafGenerationPackPromotionAuthority struct {
	db                      *DB
	metadata                *retainedalloc.Owner
	metadataCharge          uint64
	failure                 originalDBCleanupFailure
	targetProbe             rootpublication.StableChildFileInstallProbe
	captureCleanupOnce      sync.Once
	captureCleanupErr       error
	recoveryHook            func() error
	deferMetadataRefund     bool
	completionCharge        uint64
	releaseOnce             sync.Once
	releaseErr              error
	rollbackOnce            sync.Once
	rollbackErr             error
	stagingParentCharge     uint64
	destinationParentCharge uint64
	segmentIndex            valueLogRefCounts
	promoted                []rewriteCreatedSegment
	installed               []rewriteCreatedSegment
	registrations           []rootpublication.StableNamespaceSpec
	namespaces              []*rootpublication.StableNamespaceToken
	stagingRoot             string
	stagingDir              string
	destinationDir          string
	stagingParent           *os.File
	destinationParent       *os.File
	destinationGeneration   uint64
	segments                []leafGenerationPackAuthoritySegment
	dictionaryResources     *rootpublication.StableResourceSet
	resources               *rootpublication.StableResourceSet
	namespaceSyncNanos      int64
	retainedForRecovery     bool
	cleanupFailed           bool
	recoveryCleanup         []rewriteCreatedSegment
	recoveryStagingRoot     string
	released                bool
	moveNoReplace           func(*os.File, *os.File, string, *os.File, string) (bool, error)
}

func (authority *leafGenerationPackPromotionAuthority) captureDictionary(ctx context.Context, dictID uint64, dictionary []byte) error {
	if authority == nil || authority.released || authority.dictionaryResources != nil || len(authority.segments) != 0 {
		return rootpublication.ErrResourceOwnership
	}
	resources, err := captureStableDictionaryResourcesForDB(authority.db, ctx, authority.db.stableDictionaryResourceProvider(), dictID, dictionary, authority.metadata)
	if err != nil {
		return err
	}
	authority.dictionaryResources = resources
	return nil
}

func leafGenerationPackPromotionPreflight() error {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		return fmt.Errorf("%w: leaf generation pack promotion", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	return nil
}

var leafGenerationPackPromotionTargetProbe = func(authority *leafGenerationPackPromotionAuthority) error {
	return authority.targetProbe.Run(authority.destinationParent, authority.metadata)
}

func (db *DB) leafGenerationPackPromotionTargetPreflight(destinationDir string) error {
	if err := leafGenerationPackPromotionPreflight(); err != nil {
		return err
	}
	authority, err := newLeafGenerationPackAuthorityDestination(db, destinationDir)
	if err != nil {
		return err
	}
	probeErr := leafGenerationPackPromotionTargetProbe(authority)
	cleanupErr := authority.release()
	if probeErr != nil {
		probeErr = fmt.Errorf("leaf generation pack promotion target %q: %w", destinationDir, probeErr)
	}
	return errors.Join(probeErr, cleanupErr)
}

// Destination preflight and real promotion share the same original containing
// allocation and checked parent lifetime; successful preflight fully disposes it.
func newLeafGenerationPackAuthorityDestination(db *DB, destinationDir string) (*leafGenerationPackPromotionAuthority, error) {
	if db == nil || db.valueLogIdentityPins == nil {
		return nil, fmt.Errorf("%w: leaf pack requires DB-scoped identity pins", rootpublication.ErrUnresolvedResource)
	}
	metadata := db.valueLogIdentityPins.MetadataOwner()
	destinationDir = filepath.Clean(destinationDir)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(leafGenerationPackPromotionAuthority{})))
	if err := checkedRetainedPayloadChargeAdd(&charge, uint64(len(destinationDir)), 1); err != nil {
		return nil, err
	}
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	authority := &leafGenerationPackPromotionAuthority{db: db, metadata: metadata, metadataCharge: charge,
		destinationDir: strings.Clone(destinationDir), moveNoReplace: rootpublication.MoveStableChildFileNoReplace}
	authority.failure.payload = authority
	authority.segmentIndex.metadata = metadata
	if err := authority.openParent(destinationDir, &authority.destinationParent); err != nil {
		return nil, errors.Join(err, authority.release())
	}
	generation, err := rootpublication.StableNamespaceParentGeneration(authority.destinationParent)
	if err != nil {
		return nil, errors.Join(err, authority.release())
	}
	authority.destinationGeneration = generation
	return authority, nil
}

func (authority *leafGenerationPackPromotionAuthority) openParent(path string, target **os.File) error {
	fileCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(path)))
	if err != nil {
		return err
	}
	if err := authority.metadata.AddPending(fileCharge); err != nil {
		return err
	}
	authority.metadataCharge += fileCharge
	file, err := os.Open(path)
	if err != nil {
		authority.metadata.RemovePending(fileCharge)
		authority.metadataCharge -= fileCharge
		return err
	}
	*target = file
	if target == &authority.stagingParent {
		authority.stagingParentCharge = fileCharge
	} else {
		authority.destinationParentCharge = fileCharge
	}
	return nil
}

func newLeafGenerationPackPromotionAuthority(db *DB, stagingRoot, stagingDir, destinationDir string) (*leafGenerationPackPromotionAuthority, error) {
	stagingRoot = filepath.Clean(stagingRoot)
	stagingDir = filepath.Clean(stagingDir)
	if stagingRoot == "." || stagingRoot == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: leaf pack requires an explicit outer staging root", rootpublication.ErrUnresolvedResource)
	}
	relativeStagingDir, err := filepath.Rel(stagingRoot, stagingDir)
	if err != nil || relativeStagingDir == "." || relativeStagingDir == ".." || filepath.IsAbs(relativeStagingDir) || len(relativeStagingDir) >= 3 && relativeStagingDir[:3] == ".."+string(filepath.Separator) {
		return nil, fmt.Errorf("%w: leaf pack staging directory %q is not contained by staging root %q", rootpublication.ErrUnresolvedResource, stagingDir, stagingRoot)
	}
	if err := leafGenerationPackPromotionPreflight(); err != nil {
		return nil, err
	}
	authority, err := newLeafGenerationPackAuthorityDestination(db, destinationDir)
	if err != nil {
		return nil, err
	}
	var charge uint64
	for _, path := range []string{stagingRoot, stagingDir} {
		if err := checkedRetainedPayloadChargeAdd(&charge, uint64(len(path)), 1); err != nil {
			return nil, errors.Join(err, authority.release())
		}
	}
	if err := authority.metadata.AddPending(charge); err != nil {
		return nil, errors.Join(err, authority.release())
	}
	authority.metadataCharge += charge
	authority.stagingRoot, authority.stagingDir = strings.Clone(stagingRoot), strings.Clone(stagingDir)
	if err := authority.openParent(stagingDir, &authority.stagingParent); err != nil {
		return nil, errors.Join(err, authority.release())
	}
	return authority, nil
}

func leafGenerationPackPointers(ctx *leafRefRewriteCtx) []page.LeafLogPtr {
	if ctx == nil || len(ctx.leafPtrMap) == 0 {
		return nil
	}
	out := make([]page.LeafLogPtr, 0, len(ctx.leafPtrMap))
	for _, ref := range ctx.leafPtrMap {
		if ref.IsLeafLog() {
			out = append(out, ref.Log)
		}
	}
	return out
}

func stablePackedSegmentDigest(file *os.File, fileID uint32, size uint64) ([32]byte, error) {
	if file == nil || size == 0 || size > uint64(^uint64(0)>>1) {
		return [32]byte{}, fmt.Errorf("%w: packed resource has invalid immutable frontier", rootpublication.ErrUnresolvedResource)
	}
	// The first record header carries its payload checksum. Bind that canonical
	// creation prefix together with the exact file generation and immutable byte
	// frontier, avoiding another full-file read after the copy was already synced.
	prefixSize := uint64(valuelog.HeaderSize + valuelog.FrameHeaderSize)
	if size < prefixSize {
		prefixSize = size
	}
	const domain = "treedb-leaf-generation-pack-v1"
	var storage [len(domain) + 4 + 8 + valuelog.HeaderSize + valuelog.FrameHeaderSize]byte
	canonical := storage[:len(domain)+4+8+int(prefixSize)]
	copy(canonical, domain)
	binary.LittleEndian.PutUint32(canonical[len(domain):], fileID)
	binary.LittleEndian.PutUint64(canonical[len(domain)+4:], size)
	if _, err := io.ReadFull(io.NewSectionReader(file, 0, int64(prefixSize)), canonical[len(domain)+12:]); err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func (authority *leafGenerationPackPromotionAuthority) capture(created []rewriteCreatedSegment, pointers []page.LeafLogPtr) (err error) {
	if authority == nil || authority.released || len(authority.segments) != 0 {
		return rootpublication.ErrResourceOwnership
	}
	if len(created) == 0 || len(pointers) == 0 {
		return fmt.Errorf("%w: packed promotion has no created segments or pointers", rootpublication.ErrUnresolvedResource)
	}
	captured := false
	defer func() {
		if !captured {
			err = errors.Join(err, authority.releaseCaptured())
		}
	}()
	var charge uint64
	if err := checkedRetainedPayloadChargeAdd(&charge, uint64(len(created)), uint64(unsafe.Sizeof(leafGenerationPackAuthoritySegment{}))); err != nil {
		return err
	}
	for _, segment := range created {
		if err := checkedRetainedPayloadChargeAdd(&charge, uint64(len(segment.path)), 1); err != nil {
			return err
		}
	}
	if err := authority.metadata.AddPending(charge); err != nil {
		return err
	}
	authority.metadataCharge += charge
	authority.segments = make([]leafGenerationPackAuthoritySegment, 0, len(created))
	if err := authority.segmentIndex.reserve(len(created)); err != nil {
		return err
	}
	for _, segment := range created {
		if segment.path == "" || segment.fileID == 0 || segment.identity == (rootpublication.StableIdentity{}) {
			return fmt.Errorf("%w: incomplete packed segment creation identity", rootpublication.ErrUnresolvedResource)
		}
		if filepath.Clean(filepath.Dir(segment.path)) != authority.stagingDir {
			return fmt.Errorf("%w: packed segment escaped exact staging parent", rootpublication.ErrResourceConflict)
		}
		if authority.segmentIndex.get(segment.fileID) != 0 {
			return fmt.Errorf("%w: duplicate packed segment generation %d", rootpublication.ErrResourceConflict, segment.fileID)
		}
		name := filepath.Base(segment.path)
		fileCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(segment.path)))
		if err != nil {
			return err
		}
		if err = authority.metadata.AddPending(fileCharge); err != nil {
			return err
		}
		authority.metadataCharge += fileCharge
		handle, err := rootpublication.OpenStableChildFile(authority.stagingParent, name, os.O_RDONLY, 0)
		if err != nil {
			authority.metadata.RemovePending(fileCharge)
			authority.metadataCharge -= fileCharge
			return err
		}
		segment.path = strings.Clone(segment.path)
		authority.segments = append(authority.segments, leafGenerationPackAuthoritySegment{created: segment, handle: handle, handleCharge: fileCharge})
		owned := &authority.segments[len(authority.segments)-1]
		if err := authority.segmentIndex.set(segment.fileID, uint64(len(authority.segments))); err != nil {
			return err
		}
		if err := rootpublication.ValidateStableChildLink(authority.stagingParent, handle, name); err != nil {
			return err
		}
		identity, err := rootpublication.StableIdentityFromFile(handle)
		if err != nil || !rootpublication.SamePhysicalIdentity(identity, segment.identity) {
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: packed segment creation identity changed before capture", rootpublication.ErrResourceConflict)
		}
		info, err := handle.Stat()
		if err != nil || info.Size() <= 0 {
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: packed segment has empty frontier", rootpublication.ErrUnresolvedResource)
		}
		frontier := uint64(info.Size())
		digest, err := stablePackedSegmentDigest(handle, segment.fileID, frontier)
		if err != nil {
			return err
		}
		owned.identity, owned.frontierBytes, owned.digest = identity, frontier, digest
	}
	for _, pointer := range pointers {
		if pointer.FileID == 0 || pointer.RecordLength() == 0 {
			return fmt.Errorf("%w: packed pointer has zero file or record length", rootpublication.ErrUnresolvedResource)
		}
		position := authority.segmentIndex.get(page.ValueLogFileID(pointer.FileID))
		if position == 0 {
			return fmt.Errorf("%w: packed pointer references foreign file %d", rootpublication.ErrResourceConflict, pointer.FileID)
		}
		segment := &authority.segments[position-1]
		length := uint64(pointer.RecordLength())
		if pointer.Offset > ^uint64(0)-length || pointer.Offset+length > segment.frontierBytes {
			return fmt.Errorf("%w: packed pointer end exceeds immutable frontier", rootpublication.ErrFrontierBeyondResource)
		}
		segment.referenced = true
	}
	for _, segment := range authority.segments {
		if !segment.referenced {
			return fmt.Errorf("%w: packed segment %d has no reachable pointer", rootpublication.ErrUnresolvedResource, segment.created.fileID)
		}
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		if err := authority.db.valueLogIdentityPins.Observe(segment.identity); err != nil {
			return err
		}
		segment.observed = true
		pin, err := authority.db.valueLogIdentityPins.Pin(segment.identity)
		if err != nil {
			return err
		}
		segment.pin = pin
	}
	captured = true
	return nil
}

func (authority *leafGenerationPackPromotionAuthority) promote() ([]rewriteCreatedSegment, bool, error) {
	if authority == nil || authority.released || len(authority.segments) == 0 || authority.resources != nil {
		return nil, false, rootpublication.ErrResourceOwnership
	}
	var charge uint64
	for _, width := range []uint64{uint64(unsafe.Sizeof(rewriteCreatedSegment{})), uint64(unsafe.Sizeof(rewriteCreatedSegment{})), uint64(unsafe.Sizeof(rootpublication.StableNamespaceSpec{}))} {
		if err := checkedRetainedPayloadChargeAdd(&charge, uint64(len(authority.segments)), width); err != nil {
			return nil, false, err
		}
	}
	for _, segment := range authority.segments {
		length, err := checkedRetainedCapacity(1, len(authority.destinationDir), len(filepath.Base(segment.created.path)))
		if err != nil {
			return nil, false, err
		}
		if err := checkedRetainedPayloadChargeAdd(&charge, uint64(length), 1); err != nil {
			return nil, false, err
		}
	}
	if err := authority.metadata.AddPending(charge); err != nil {
		return nil, false, err
	}
	authority.metadataCharge += charge
	promoted := make([]rewriteCreatedSegment, len(authority.segments))
	authority.promoted = promoted
	for i := range authority.segments {
		promoted[i] = authority.segments[i].created
	}
	installedSegments := make([]rewriteCreatedSegment, 0, len(authority.segments))
	authority.installed = installedSegments
	mutated := false
	fail := func(err error) ([]rewriteCreatedSegment, bool, error) {
		if mutated {
			// No root or alternate meta page references these children yet. Try
			// exact identity-verified rollback immediately. If deletion durability
			// is ambiguous, retain the same parent, children, pins, and staging
			// root in terminal custody after the first hard failure.
			cleanupErr := authority.rollbackPromoted()
			if cleanupErr != nil {
				authority.retainPromotedCleanupForRecovery(
					installedSegments,
					authority.stagingRoot,
				)
				err = errors.Join(err, cleanupErr, ErrRecoveryRequired)
			} else {
				// A namespace mutation happened, but exact rollback proved that no
				// live destination child remains. Report no outstanding mutation so
				// callers take their ordinary staging cleanup path.
				mutated = false
			}
		}
		return promoted, mutated, err
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		name := filepath.Base(segment.created.path)
		installed, err := authority.moveNoReplace(authority.stagingParent, segment.handle, name, authority.destinationParent, name)
		if installed {
			mutated = true
			segment.installed = true
			destination := filepath.Join(authority.destinationDir, name)
			segment.created.path = destination
			promoted[i] = segment.created
			installedSegments = append(installedSegments, segment.created)
			authority.installed = installedSegments
		}
		if err != nil {
			return fail(err)
		}
		if !installed {
			return fail(fmt.Errorf("%w: packed promotion reported no installed child %q", rootpublication.ErrUnresolvedResource, name))
		}
		destination := segment.created.path
		if err := rootpublication.ValidateStableChildLink(authority.destinationParent, segment.handle, name); err != nil {
			return fail(err)
		}
		// This authority can still remove the exact installed child before any
		// root becomes visible. Preserve the observer error itself here; fail()
		// adds recovery debt only when exact rollback is ambiguous.
		if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceOuterLeaf, authority.destinationDir, "", destination); err != nil {
			return fail(err)
		}
	}
	for _, parent := range []string{authority.stagingDir, authority.destinationDir} {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, parent, parent); err != nil {
			return fail(err)
		}
	}
	registrations := make([]rootpublication.StableNamespaceSpec, len(authority.segments))
	authority.registrations = registrations
	for i := range authority.segments {
		segment := &authority.segments[i]
		name := filepath.Base(segment.created.path)
		relativeDir, err := filepath.Rel(authority.db.dir, authority.destinationDir)
		if err != nil {
			return fail(err)
		}
		registrations[i] = rootpublication.StableNamespaceSpec{
			Parent: authority.destinationParent, LinkedResource: segment.handle,
			ParentGeneration: authority.destinationGeneration, Operation: rootpublication.NamespaceCreate,
			NewName: name, DiagnosticPath: relativeDir,
		}
	}
	namespaceSyncStarted := time.Now()
	namespaces, err := rootpublication.NewStableNamespaceBatchTokens(rootpublication.StableNamespaceBatchSpec{
		Registrations: registrations, AdditionalParents: []*os.File{authority.stagingParent},
		MetadataOwner: authority.metadata, Registry: authority.db.valueLogIdentityPins,
	})
	authority.namespaceSyncNanos = time.Since(namespaceSyncStarted).Nanoseconds()
	if err != nil {
		return fail(err)
	}
	authority.namespaces = namespaces
	for i := range authority.segments {
		segment := &authority.segments[i]
		name := filepath.Base(segment.created.path)
		if err := authority.db.valueLogIdentityPins.RememberStableNamespaceLink(
			authority.destinationParent, segment.handle, name,
		); err != nil {
			return fail(err)
		}
	}
	for _, parent := range []string{authority.stagingDir, authority.destinationDir} {
		if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, parent, parent); err != nil {
			return fail(err)
		}
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(authority.metadata, rootpublication.ReachabilityOuterLeafPackedPointer)
	if err != nil {
		return fail(err)
	}
	if authority.dictionaryResources != nil {
		if err := builder.Merge(authority.dictionaryResources); err != nil {
			return fail(errors.Join(err, builder.AbandonWithError()))
		}
		authority.dictionaryResources = nil
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		relativePath, err := filepath.Rel(authority.db.dir, segment.created.path)
		if err != nil {
			return fail(errors.Join(err, builder.AbandonWithError()))
		}
		token, err := rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerOuterLeaf, rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceOuterLeafPack, LogicalLane: "leaf-generation-pack",
			ResourceID: strconv.FormatUint(uint64(segment.created.fileID), 10), Generation: uint64(segment.created.fileID),
			DiagnosticPath: relativePath, File: segment.handle,
			Frontier: rootpublication.DurableFrontier{Bytes: segment.frontierBytes}, Digest: segment.digest,
			Reachability: rootpublication.ReachabilityOuterLeafPackedPointer, Namespace: namespaces[i],
			ContentSynced: true, PinRegistry: authority.db.valueLogIdentityPins, MetadataOwner: authority.metadata,
		}, "authoritative")
		if err != nil {
			return fail(errors.Join(err, builder.AbandonWithError()))
		}
		if err := builder.Add(token); err != nil {
			return fail(errors.Join(err, token.ReleaseWithError(), builder.AbandonWithError()))
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return fail(errors.Join(err, builder.AbandonWithError()))
	}
	authority.resources = resources
	return promoted, true, nil
}

func (authority *leafGenerationPackPromotionAuthority) verifyManagerRegistration() error {
	if authority == nil || authority.db == nil || authority.db.valueLogManager == nil {
		return rootpublication.ErrUnresolvedResource
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		identity, ok := authority.db.valueLogManager.StableSegmentIdentity(segment.created.fileID)
		if !ok || !rootpublication.SamePhysicalIdentity(identity, segment.identity) {
			return fmt.Errorf("%w: manager registered a different packed segment identity %d", rootpublication.ErrResourceConflict, segment.created.fileID)
		}
	}
	return nil
}

// takeStableResources transfers the promoted packed/dictionary closure into
// the durable-root candidate. Construction handles and deletion pins remain
// owned by authority until publication succeeds or exact abandonment runs.
func (authority *leafGenerationPackPromotionAuthority) takeStableResources() (*rootpublication.StableResourceSet, error) {
	if authority == nil || authority.released || authority.resources == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	resources := authority.resources
	authority.resources = nil
	return resources, nil
}

// rollbackPromoted uses only the exact handles captured before promotion.
// Progress and first refusal belong to this same original authority; no reopen,
// replacement child, copied cleanup slice, or teardown retry is introduced.
func (authority *leafGenerationPackPromotionAuthority) rollbackPromoted() error {
	authority.rollbackOnce.Do(func() {
		for i := range authority.segments {
			segment := &authority.segments[i]
			if !segment.installed {
				continue
			}
			name := filepath.Base(segment.created.path)
			identity, err := rootpublication.StableIdentityFromFile(segment.handle)
			if err == nil && !rootpublication.SamePhysicalIdentity(identity, segment.identity) {
				err = rootpublication.ErrResourceConflict
			}
			if err == nil {
				err = rootpublication.ValidateStableChildLink(authority.destinationParent, segment.handle, name)
			}
			if err == nil {
				err = rootpublication.RemoveStableChildFile(authority.destinationParent, name)
			}
			if err == nil {
				err = observeNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceOuterLeaf, authority.destinationDir, segment.created.path, "")
			}
			if err != nil {
				authority.rollbackErr = err
				return
			}
		}
		if len(authority.installed) != 0 {
			err := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, authority.destinationDir, authority.destinationDir)
			if err == nil {
				err = leafGenerationPackAuthoritySyncDirectory(authority.destinationParent)
			}
			if err == nil {
				err = durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, authority.destinationDir, authority.destinationDir)
			}
			if err != nil {
				authority.rollbackErr = err
				return
			}
			parentIdentity, err := rootpublication.StableIdentityFromFile(authority.destinationParent)
			if err != nil {
				authority.rollbackErr = err
				return
			}
			for i := range authority.segments {
				segment := &authority.segments[i]
				if !segment.installed {
					continue
				}
				if err := authority.db.valueLogIdentityPins.ForgetStableNamespaceLinkIdentity(parentIdentity, segment.identity, filepath.Base(segment.created.path)); err != nil {
					authority.rollbackErr = err
					return
				}
				segment.installed = false
			}
		}
	})
	return authority.rollbackErr
}

// Publication ambiguity has not refused a cleanup action. The existing
// producer teardown edge owns the same authority until its physical drain.
// The DB refunds containing/method storage only after the callback returns.
func (authority *leafGenerationPackPromotionAuthority) retainForRecovery() error {
	if authority == nil || authority.released {
		return nil
	}
	if authority.retainedForRecovery {
		if authority.cleanupFailed {
			return &authority.failure
		}
		return nil
	}
	charge := retainedalloc.AllocationCharge(2 * uint64(unsafe.Sizeof(uintptr(0))))
	if err := authority.metadata.AddPending(charge); err != nil {
		return authority.retainCleanupFailure(err)
	}
	authority.metadataCharge += charge
	physicalCharge := authority.stagingParentCharge + authority.destinationParentCharge
	for i := range authority.segments {
		physicalCharge += authority.segments[i].handleCharge
	}
	if physicalCharge > authority.metadataCharge {
		return authority.retainCleanupFailure(rootpublication.ErrResourceOwnership)
	}
	authority.completionCharge = authority.metadataCharge - physicalCharge
	authority.recoveryHook = authority.release
	accepted, err := authority.db.tryRegisterCaptureTeardownOwnedHook(
		authority.recoveryHook, authority.db.valueLogIdentityPins.MetadataOwner(),
		authority.metadata, authority.completionCharge,
	)
	if err != nil || !accepted {
		return authority.retainCleanupFailure(errors.Join(err, rootpublication.ErrResourceOwnership))
	}
	authority.retainedForRecovery, authority.deferMetadataRefund = true, true
	return nil
}
func (authority *leafGenerationPackPromotionAuthority) retainPromotedCleanupForRecovery(promoted []rewriteCreatedSegment, stagingRoot string) {
	if authority == nil || authority.released || len(promoted) == 0 {
		return
	}
	// These are already admitted aliases of the original installed backing.
	authority.recoveryCleanup, authority.recoveryStagingRoot = promoted, stagingRoot
	authority.failure.retainCause(1, ErrRecoveryRequired)
	if authority.rollbackErr != nil {
		authority.retainCleanupFailure(authority.rollbackErr)
	}
	// A refused rollback already has terminal original DB custody. It must
	// never become an automatic teardown retry.
}

// Every constituent is attempted once. Successful siblings clear their exact
// aliases; a refusal retains the SAME admitted authority and first receipt.
func (authority *leafGenerationPackPromotionAuthority) retainCleanupFailure(cause error) error {
	if cause == nil {
		return nil
	}
	authority.failure.retainCause(0, cause)
	authority.retainedForRecovery, authority.cleanupFailed = true, true
	authority.metadata.CleanupFailed()
	authority.db.retainOriginalCleanupError(&authority.failure)
	return &authority.failure
}
func (authority *leafGenerationPackPromotionAuthority) releaseCaptured() error {
	if authority == nil {
		return nil
	}
	authority.captureCleanupOnce.Do(func() {
		var err error
		if authority.resources != nil {
			e := authority.resources.ReleaseWithError()
			err = errors.Join(err, e)
			if e == nil {
				authority.resources = nil
			}
		}
		if authority.dictionaryResources != nil {
			e := authority.dictionaryResources.ReleaseWithError()
			err = errors.Join(err, e)
			if e == nil {
				authority.dictionaryResources = nil
			}
		}
		for i, namespace := range authority.namespaces {
			e := namespace.ReleaseWithError()
			err = errors.Join(err, e)
			if e == nil {
				authority.namespaces[i] = nil
			}
		}
		for i := range authority.segments {
			segment := &authority.segments[i]
			if segment.handle != nil {
				e := leafGenerationPackAuthorityCloseFile(segment.handle)
				err = errors.Join(err, e)
				if e == nil {
					segment.handle = nil
					authority.metadata.RemovePending(segment.handleCharge)
					authority.metadataCharge -= segment.handleCharge
					segment.handleCharge = 0
				}
			}
			// A failed physical file retains its original deletion pin/observation.
			if segment.handle == nil {
				segment.pin.Release()
				segment.pin = nil
				if segment.observed {
					e := authority.db.valueLogIdentityPins.Unobserve(segment.identity)
					err = errors.Join(err, e)
					if e == nil {
						segment.observed = false
					}
				}
			}
		}
		authority.captureCleanupErr = err
	})
	return authority.captureCleanupErr
}

var leafGenerationPackAuthorityCloseFile = func(file *os.File) error { return file.Close() }
var leafGenerationPackAuthoritySyncDirectory = func(file *os.File) error { return file.Sync() }

func (authority *leafGenerationPackPromotionAuthority) release() error {
	if authority == nil {
		return nil
	}
	// A hard destructive refusal retains the exact original graph. It is
	// distinct from a publication ambiguity whose teardown can still succeed.
	if authority.cleanupFailed {
		return &authority.failure
	}
	authority.releaseOnce.Do(func() {
		authority.released = true
		if err := authority.targetProbe.CleanupError(); err != nil {
			authority.releaseErr = authority.retainCleanupFailure(err)
			return
		}
		err := authority.releaseCaptured()
		if authority.stagingParent != nil {
			e := leafGenerationPackAuthorityCloseFile(authority.stagingParent)
			err = errors.Join(err, e)
			if e == nil {
				authority.stagingParent = nil
				authority.metadata.RemovePending(authority.stagingParentCharge)
				authority.metadataCharge -= authority.stagingParentCharge
				authority.stagingParentCharge = 0
			}
		}
		if authority.destinationParent != nil {
			e := leafGenerationPackAuthorityCloseFile(authority.destinationParent)
			err = errors.Join(err, e)
			if e == nil {
				authority.destinationParent = nil
				authority.metadata.RemovePending(authority.destinationParentCharge)
				authority.metadataCharge -= authority.destinationParentCharge
				authority.destinationParentCharge = 0
			}
		}
		if err != nil {
			authority.releaseErr = authority.retainCleanupFailure(err)
			return
		}
		if authority.deferMetadataRefund && authority.metadataCharge != authority.completionCharge {
			authority.releaseErr = authority.retainCleanupFailure(rootpublication.ErrResourceOwnership)
			return
		}
		clear(authority.segments)
		clear(authority.promoted)
		clear(authority.installed)
		clear(authority.registrations)
		clear(authority.namespaces)
		authority.segments, authority.promoted, authority.installed = nil, nil, nil
		authority.registrations, authority.namespaces = nil, nil
		authority.recoveryCleanup, authority.recoveryStagingRoot = nil, ""
		authority.stagingRoot, authority.stagingDir, authority.destinationDir = "", "", ""
		authority.segmentIndex.close()
		owner, charge := authority.metadata, authority.metadataCharge
		authority.recoveryHook = nil
		authority.metadata, authority.metadataCharge, authority.db = nil, 0, nil
		if !authority.deferMetadataRefund {
			owner.RemovePending(charge)
		}
	})
	return authority.releaseErr
}
