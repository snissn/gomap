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
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafGenerationPackAuthoritySegment struct {
	created       rewriteCreatedSegment
	handle        *os.File
	identity      rootpublication.StableIdentity
	frontierBytes uint64
	digest        [32]byte
	pin           *rootpublication.IdentityPin
	observed      bool
}

// leafGenerationPackPromotionAuthority owns exact handles and deletion pins
// from staging capture through the alternate-meta publication boundary. It is
// deliberately local to packed promotion and does not consume the future
// publication-seal authority tracked by #3679.
type leafGenerationPackPromotionAuthority struct {
	db                    *DB
	stagingRoot           string
	stagingDir            string
	destinationDir        string
	stagingParent         *os.File
	destinationParent     *os.File
	destinationGeneration uint64
	segments              []leafGenerationPackAuthoritySegment
	dictionaryResources   *rootpublication.StableResourceSet
	resources             *rootpublication.StableResourceSet
	namespaceSyncNanos    int64
	retainedForRecovery   bool
	recoveryCleanup       []rewriteCreatedSegment
	recoveryStagingRoot   string
	released              bool
	moveNoReplace         func(*os.File, *os.File, string, *os.File, string) (bool, error)
}

func (authority *leafGenerationPackPromotionAuthority) captureDictionary(ctx context.Context, dictID uint64, dictionary []byte) error {
	if authority == nil || authority.released || authority.dictionaryResources != nil || len(authority.segments) != 0 {
		return rootpublication.ErrResourceOwnership
	}
	resources, err := captureStableDictionaryResources(ctx, authority.db.stableDictionaryResourceProvider(), dictID, dictionary)
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

var leafGenerationPackPromotionTargetProbe = rootpublication.ProbeStableChildFileNoReplaceInstall

func leafGenerationPackPromotionTargetPreflight(destinationDir string) error {
	if err := leafGenerationPackPromotionPreflight(); err != nil {
		return err
	}
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		return err
	}
	defer destinationParent.Close()
	if err := leafGenerationPackPromotionTargetProbe(destinationParent); err != nil {
		return fmt.Errorf("leaf generation pack promotion target %q: %w", destinationDir, err)
	}
	return nil
}

func newLeafGenerationPackPromotionAuthority(db *DB, stagingRoot, stagingDir, destinationDir string) (*leafGenerationPackPromotionAuthority, error) {
	if db == nil || db.valueLogIdentityPins == nil {
		return nil, fmt.Errorf("%w: leaf pack requires DB-scoped identity pins", rootpublication.ErrUnresolvedResource)
	}
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
	stagingParent, err := os.Open(stagingDir)
	if err != nil {
		return nil, err
	}
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		_ = stagingParent.Close()
		return nil, err
	}
	destinationGeneration, err := rootpublication.StableNamespaceParentGeneration(destinationParent)
	if err != nil {
		_ = stagingParent.Close()
		_ = destinationParent.Close()
		return nil, err
	}
	return &leafGenerationPackPromotionAuthority{
		db: db, stagingRoot: stagingRoot, stagingDir: stagingDir, destinationDir: filepath.Clean(destinationDir),
		stagingParent: stagingParent, destinationParent: destinationParent,
		destinationGeneration: destinationGeneration,
		moveNoReplace:         rootpublication.MoveStableChildFileNoReplace,
	}, nil
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
	canonical := make([]byte, len(domain)+4+8+int(prefixSize))
	copy(canonical, domain)
	binary.LittleEndian.PutUint32(canonical[len(domain):], fileID)
	binary.LittleEndian.PutUint64(canonical[len(domain)+4:], size)
	if _, err := io.ReadFull(io.NewSectionReader(file, 0, int64(prefixSize)), canonical[len(domain)+12:]); err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func (authority *leafGenerationPackPromotionAuthority) capture(created []rewriteCreatedSegment, pointers []page.LeafLogPtr) error {
	if authority == nil || authority.released || len(authority.segments) != 0 {
		return rootpublication.ErrResourceOwnership
	}
	if len(created) == 0 || len(pointers) == 0 {
		return fmt.Errorf("%w: packed promotion has no created segments or pointers", rootpublication.ErrUnresolvedResource)
	}
	captured := false
	defer func() {
		if !captured {
			authority.releaseCaptured()
		}
	}()
	authority.segments = make([]leafGenerationPackAuthoritySegment, 0, len(created))
	byRawFileID := make(map[uint32]*leafGenerationPackAuthoritySegment, len(created))
	seenGeneration := make(map[uint32]struct{}, len(created))
	for _, segment := range created {
		if segment.path == "" || segment.fileID == 0 || segment.identity == (rootpublication.StableIdentity{}) {
			return fmt.Errorf("%w: incomplete packed segment creation identity", rootpublication.ErrUnresolvedResource)
		}
		if filepath.Clean(filepath.Dir(segment.path)) != authority.stagingDir {
			return fmt.Errorf("%w: packed segment escaped exact staging parent", rootpublication.ErrResourceConflict)
		}
		if _, ok := seenGeneration[segment.fileID]; ok {
			return fmt.Errorf("%w: duplicate packed segment generation %d", rootpublication.ErrResourceConflict, segment.fileID)
		}
		seenGeneration[segment.fileID] = struct{}{}
		name := filepath.Base(segment.path)
		handle, err := rootpublication.OpenStableChildFile(authority.stagingParent, name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if err := rootpublication.ValidateStableChildLink(authority.stagingParent, handle, name); err != nil {
			_ = handle.Close()
			return err
		}
		identity, err := rootpublication.StableIdentityFromFile(handle)
		if err != nil || !rootpublication.SamePhysicalIdentity(identity, segment.identity) {
			_ = handle.Close()
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: packed segment creation identity changed before capture", rootpublication.ErrResourceConflict)
		}
		info, err := handle.Stat()
		if err != nil || info.Size() <= 0 {
			_ = handle.Close()
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: packed segment has empty frontier", rootpublication.ErrUnresolvedResource)
		}
		frontier := uint64(info.Size())
		digest, err := stablePackedSegmentDigest(handle, segment.fileID, frontier)
		if err != nil {
			_ = handle.Close()
			return err
		}
		authority.segments = append(authority.segments, leafGenerationPackAuthoritySegment{
			created: segment, handle: handle, identity: identity, frontierBytes: frontier, digest: digest,
		})
		byRawFileID[page.ValueLogSegmentID(segment.fileID)] = &authority.segments[len(authority.segments)-1]
	}
	referenced := make(map[uint32]struct{}, len(created))
	for _, pointer := range pointers {
		if pointer.FileID == 0 || pointer.RecordLength() == 0 {
			authority.releaseCaptured()
			return fmt.Errorf("%w: packed pointer has zero file or record length", rootpublication.ErrUnresolvedResource)
		}
		segment := byRawFileID[pointer.FileID]
		if segment == nil {
			authority.releaseCaptured()
			return fmt.Errorf("%w: packed pointer references foreign file %d", rootpublication.ErrResourceConflict, pointer.FileID)
		}
		length := uint64(pointer.RecordLength())
		if pointer.Offset > ^uint64(0)-length || pointer.Offset+length > segment.frontierBytes {
			authority.releaseCaptured()
			return fmt.Errorf("%w: packed pointer end exceeds immutable frontier", rootpublication.ErrFrontierBeyondResource)
		}
		referenced[page.ValueLogFileID(pointer.FileID)] = struct{}{}
	}
	for _, segment := range authority.segments {
		if _, ok := referenced[segment.created.fileID]; !ok {
			authority.releaseCaptured()
			return fmt.Errorf("%w: packed segment %d has no reachable pointer", rootpublication.ErrUnresolvedResource, segment.created.fileID)
		}
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		if err := authority.db.valueLogIdentityPins.Observe(segment.identity); err != nil {
			authority.releaseCaptured()
			return err
		}
		segment.observed = true
		pin, err := authority.db.valueLogIdentityPins.Pin(segment.identity)
		if err != nil {
			authority.releaseCaptured()
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
	promoted := make([]rewriteCreatedSegment, len(authority.segments))
	for i := range authority.segments {
		promoted[i] = authority.segments[i].created
	}
	installedSegments := make([]rewriteCreatedSegment, 0, len(authority.segments))
	mutated := false
	fail := func(err error) ([]rewriteCreatedSegment, bool, error) {
		if mutated {
			// No root or alternate meta page references these children yet. Try
			// exact identity-verified rollback immediately. If deletion durability
			// is ambiguous, retain the same parent, children, pins, and staging
			// root so DB teardown can retry idempotently.
			cleanupErr := cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(
				authority.destinationParent,
				installedSegments,
				authority.db.valueLogIdentityPins,
			)
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
			destination := filepath.Join(authority.destinationDir, name)
			segment.created.path = destination
			promoted[i] = segment.created
			installedSegments = append(installedSegments, segment.created)
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
	})
	authority.namespaceSyncNanos = time.Since(namespaceSyncStarted).Nanoseconds()
	if err != nil {
		return fail(err)
	}
	defer func() {
		for _, namespace := range namespaces {
			namespace.Release()
		}
	}()
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
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafPackedPointer)
	if authority.dictionaryResources != nil {
		if err := builder.Merge(authority.dictionaryResources); err != nil {
			builder.Abandon()
			return fail(err)
		}
		authority.dictionaryResources = nil
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		relativePath, err := filepath.Rel(authority.db.dir, segment.created.path)
		if err != nil {
			builder.Abandon()
			return fail(err)
		}
		token, err := rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerOuterLeaf, rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceOuterLeafPack, LogicalLane: "leaf-generation-pack",
			ResourceID: strconv.FormatUint(uint64(segment.created.fileID), 10), Generation: uint64(segment.created.fileID),
			DiagnosticPath: relativePath, File: segment.handle,
			Frontier: rootpublication.DurableFrontier{Bytes: segment.frontierBytes}, Digest: segment.digest,
			Reachability: rootpublication.ReachabilityOuterLeafPackedPointer, Namespace: namespaces[i],
			ContentSynced: true, PinRegistry: authority.db.valueLogIdentityPins,
		}, "authoritative")
		if err != nil {
			builder.Abandon()
			return fail(err)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return fail(err)
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return fail(err)
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

func (authority *leafGenerationPackPromotionAuthority) retainForRecovery() {
	if authority == nil || authority.released || authority.retainedForRecovery {
		return
	}
	authority.retainedForRecovery = true
	authority.db.registerCaptureTeardownHook(func() error {
		var cleanupErr error
		stagingRoot := authority.recoveryStagingRoot
		if len(authority.recoveryCleanup) != 0 {
			cleanupErr = cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(
				authority.destinationParent,
				authority.recoveryCleanup,
				authority.db.valueLogIdentityPins,
			)
			if cleanupErr == nil {
				authority.recoveryCleanup = nil
			}
		}
		releaseErr := authority.release()
		if cleanupErr != nil {
			return errors.Join(cleanupErr, releaseErr, ErrRecoveryRequired)
		}
		if releaseErr != nil {
			return errors.Join(releaseErr, ErrRecoveryRequired)
		}
		if stagingRoot != "" {
			if err := removeLeafGenerationPackStagingDirFn(stagingRoot); err != nil {
				return errors.Join(err, ErrRecoveryRequired)
			}
		}
		return nil
	})
}

func (authority *leafGenerationPackPromotionAuthority) retainPromotedCleanupForRecovery(promoted []rewriteCreatedSegment, stagingRoot string) {
	if authority == nil || authority.released || len(promoted) == 0 {
		return
	}
	authority.recoveryCleanup = append(authority.recoveryCleanup[:0], promoted...)
	authority.recoveryStagingRoot = stagingRoot
	authority.retainForRecovery()
}

func (authority *leafGenerationPackPromotionAuthority) releaseCaptured() {
	if authority == nil {
		return
	}
	if authority.resources != nil {
		authority.resources.Release()
		authority.resources = nil
	}
	if authority.dictionaryResources != nil {
		authority.dictionaryResources.Release()
		authority.dictionaryResources = nil
	}
	for i := range authority.segments {
		segment := &authority.segments[i]
		segment.pin.Release()
		segment.pin = nil
		if segment.observed && authority.db != nil && authority.db.valueLogIdentityPins != nil {
			_ = authority.db.valueLogIdentityPins.Unobserve(segment.identity)
			segment.observed = false
		}
		if segment.handle != nil {
			_ = segment.handle.Close()
			segment.handle = nil
		}
	}
}

func (authority *leafGenerationPackPromotionAuthority) release() error {
	if authority == nil || authority.released {
		return nil
	}
	authority.released = true
	authority.recoveryCleanup = nil
	authority.recoveryStagingRoot = ""
	authority.releaseCaptured()
	var err error
	if authority.stagingParent != nil {
		err = errors.Join(err, authority.stagingParent.Close())
		authority.stagingParent = nil
	}
	if authority.destinationParent != nil {
		err = errors.Join(err, authority.destinationParent.Close())
		authority.destinationParent = nil
	}
	return err
}
