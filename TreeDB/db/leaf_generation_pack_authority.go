package db

import (
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
	stagingDir            string
	destinationDir        string
	stagingParent         *os.File
	destinationParent     *os.File
	destinationGeneration uint64
	segments              []leafGenerationPackAuthoritySegment
	resources             *rootpublication.StableResourceSet
	namespaceSyncNanos    int64
	retainedForRecovery   bool
	released              bool
}

func leafGenerationPackPromotionPreflight() error {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		return fmt.Errorf("%w: leaf generation pack promotion", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	return nil
}

func newLeafGenerationPackPromotionAuthority(db *DB, stagingDir, destinationDir string) (*leafGenerationPackPromotionAuthority, error) {
	if db == nil || db.valueLogIdentityPins == nil {
		return nil, fmt.Errorf("%w: leaf pack requires DB-scoped identity pins", rootpublication.ErrUnresolvedResource)
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
		db: db, stagingDir: filepath.Clean(stagingDir), destinationDir: filepath.Clean(destinationDir),
		stagingParent: stagingParent, destinationParent: destinationParent,
		destinationGeneration: destinationGeneration,
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
	mutated := false
	for i := range authority.segments {
		segment := &authority.segments[i]
		name := filepath.Base(segment.created.path)
		installed, err := rootpublication.MoveStableChildFileNoReplace(authority.stagingParent, segment.handle, name, authority.destinationParent, name)
		mutated = mutated || installed
		if err != nil {
			return promoted, mutated, err
		}
		destination := filepath.Join(authority.destinationDir, name)
		segment.created.path = destination
		promoted[i] = segment.created
		if err := rootpublication.ValidateStableChildLink(authority.destinationParent, segment.handle, name); err != nil {
			return promoted, true, errors.Join(err, ErrRecoveryRequired)
		}
		if err := observeNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceOuterLeaf, authority.destinationDir, filepath.Join(authority.stagingDir, name), destination); err != nil {
			return promoted, true, err
		}
	}
	for _, parent := range []string{authority.stagingDir, authority.destinationDir} {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, parent, parent); err != nil {
			return promoted, true, errors.Join(err, ErrRecoveryRequired)
		}
	}
	registrations := make([]rootpublication.StableNamespaceSpec, len(authority.segments))
	for i := range authority.segments {
		segment := &authority.segments[i]
		name := filepath.Base(segment.created.path)
		relativeDir, err := filepath.Rel(authority.db.dir, authority.destinationDir)
		if err != nil {
			return promoted, true, err
		}
		registrations[i] = rootpublication.StableNamespaceSpec{
			Parent: authority.destinationParent, LinkedResource: segment.handle,
			ParentGeneration: authority.destinationGeneration, Operation: rootpublication.NamespaceRename,
			OldName: name, NewName: name, DiagnosticPath: relativeDir,
		}
	}
	namespaceSyncStarted := time.Now()
	namespaces, err := rootpublication.NewStableNamespaceBatchTokens(rootpublication.StableNamespaceBatchSpec{
		Registrations: registrations, AdditionalParents: []*os.File{authority.stagingParent},
	})
	authority.namespaceSyncNanos = time.Since(namespaceSyncStarted).Nanoseconds()
	if err != nil {
		return promoted, true, errors.Join(err, ErrRecoveryRequired)
	}
	defer func() {
		for _, namespace := range namespaces {
			namespace.Release()
		}
	}()
	for _, parent := range []string{authority.stagingDir, authority.destinationDir} {
		if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, parent, parent); err != nil {
			return promoted, true, errors.Join(err, ErrRecoveryRequired)
		}
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafPackedPointer)
	for i := range authority.segments {
		segment := &authority.segments[i]
		relativePath, err := filepath.Rel(authority.db.dir, segment.created.path)
		if err != nil {
			builder.Abandon()
			return promoted, true, err
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
			return promoted, true, err
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return promoted, true, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return promoted, true, err
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

func (authority *leafGenerationPackPromotionAuthority) retainForRecovery() {
	if authority == nil || authority.released || authority.retainedForRecovery {
		return
	}
	authority.retainedForRecovery = true
	authority.db.registerInternalTeardownHook(func() error {
		return authority.release()
	})
}

func (authority *leafGenerationPackPromotionAuthority) releaseCaptured() {
	if authority == nil {
		return
	}
	if authority.resources != nil {
		authority.resources.Release()
		authority.resources = nil
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
