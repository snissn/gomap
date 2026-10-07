package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// This format inherits TreeDB's exclusive index-writer contract, not kernel
// protection against arbitrary external pwrite/mmap. The owning system root,
// index-generation identity, and existing COW retirement/pin capability are
// authority. No namespace inventory or second revision/pin registry is used.
const ownedLeafManifestMaxBytes = 64 << 10
const ownedLeafManifestChunkBytes = 2048
const ownedLeafManifestMaxDepth = 32
const ownedLeafManifestMaxGenerations = 128
const ownedLeafManifestMaxFileIDs = 4096

var ownedLeafManifestHeaderKey = []byte("\x00treedb/owned-leaf-manifest/0")
var ownedLeafManifestMagic = [8]byte{'T', 'D', 'L', 'M', 'O', 'W', 'N', 1}

func ownedLeafManifestChunkKey(i int) []byte {
	return []byte(fmt.Sprintf("\x00treedb/owned-leaf-manifest/1/%08x", i))
}

func admitOwnedLeafManifest(m *leafGenerationManifest) error {
	if m == nil || len(m.Generations) > ownedLeafManifestMaxGenerations {
		return errors.New("owned manifest generation ceiling exceeded")
	}
	ids := 0
	for _, g := range m.Generations {
		if len(g.FileIDs) > ownedLeafManifestMaxFileIDs-ids {
			return errors.New("owned manifest file-ID ceiling exceeded")
		}
		ids += len(g.FileIDs)
	}
	return nil
}

func encodeOwnedLeafManifest(m *leafGenerationManifest) ([]byte, [][]byte, error) {
	if err := admitOwnedLeafManifest(m); err != nil {
		return nil, nil, err
	}
	if m == nil || m.ManifestRevision == 0 {
		return nil, nil, errors.New("owned manifest has no revision")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, nil, err
	}
	if len(data) == 0 || len(data) > ownedLeafManifestMaxBytes {
		return nil, nil, fmt.Errorf("owned manifest exceeds %d-byte format ceiling", ownedLeafManifestMaxBytes)
	}
	// Reuse the legacy semantic checks without importing its filename authority.
	if _, err := decodeLeafGenerationManifest(data, "pager-owned manifest"); err != nil {
		return nil, nil, err
	}
	chunks := make([][]byte, 0, (len(data)+ownedLeafManifestChunkBytes-1)/ownedLeafManifestChunkBytes)
	for off := 0; off < len(data); off += ownedLeafManifestChunkBytes {
		end := off + ownedLeafManifestChunkBytes
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[off:end])
	}
	header := make([]byte, 64)
	copy(header, ownedLeafManifestMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(chunks)))
	binary.LittleEndian.PutUint64(header[16:24], m.ManifestRevision)
	digest := sha256.Sum256(data)
	copy(header[24:56], digest[:])
	return header, chunks, nil
}

// readOwnedManifestEntry charges a finite path before reading any page. Every
// visited page is freshly checksummed, even when pager verified-page caching is
// enabled. Manifest objects may never resolve through external leaf/value refs.
func readOwnedManifestEntry(source freelist.PageSource, root, extent uint64, key []byte) ([]byte, error) {
	for depth := 0; depth < ownedLeafManifestMaxDepth; depth++ {
		if root < 2 || root >= extent {
			return nil, errors.New("owned manifest page outside root extent")
		}
		image, err := source.ReadPage(root)
		if err != nil {
			return nil, err
		}
		if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) || page.DecodeHeader(image).PageID != root {
			return nil, errors.New("owned manifest page checksum/identity mismatch")
		}
		n := node.NewNode(image)
		switch n.Type() {
		case page.PageTypeLeaf:
			i, found, err := n.SearchLeaf(key)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, errors.New("owned manifest entry missing")
			}
			value, ptr, flags, err := n.GetLeafValueView(i)
			if err != nil {
				return nil, err
			}
			if flags != 0 || ptr != (page.ValuePtr{}) {
				return nil, errors.New("owned manifest entry must be inline")
			}
			return append([]byte(nil), value...), nil
		case page.PageTypeInternal:
			next, found, err := n.SearchInternalChildID(key)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, errors.New("owned manifest path missing")
			}
			root = next
		default:
			return nil, errors.New("owned manifest path has invalid node type")
		}
	}
	return nil, errors.New("owned manifest path exceeds format depth ceiling")
}

// validateOwnedManifestKeys checks the entire reserved key interval. It is
// bounded by the admitted number of chunks and the format depth before reads;
// arbitrary user/system keys outside that interval are not an inventory.
func validateOwnedManifestKeys(source freelist.PageSource, root, extent uint64, chunks int) error {
	low := []byte("\x00treedb/owned-leaf-manifest/")
	high := []byte("\x00treedb/owned-leaf-manifest0")
	remaining := (chunks + 2) * ownedLeafManifestMaxDepth
	ordinal := -1 // header first, then exactly the advertised chunk ordinals
	var visit func(uint64, int) error
	visit = func(id uint64, depth int) error {
		if depth >= ownedLeafManifestMaxDepth || remaining == 0 || id < 2 || id >= extent {
			return errors.New("owned manifest key range exceeds page/depth/extent bound")
		}
		remaining--
		image, err := source.ReadPage(id)
		if err != nil {
			return err
		}
		if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) || page.DecodeHeader(image).PageID != id {
			return errors.New("owned manifest range page checksum/identity mismatch")
		}
		n := node.NewNode(image)
		if int(n.Count()) > page.PageSize/2 {
			return errors.New("owned manifest range count exceeds page bound")
		}
		switch n.Type() {
		case page.PageTypeLeaf:
			for i := uint16(0); i < n.Count(); i++ {
				key, flags, err := n.GetLeafKeyFlagsView(i)
				if err != nil {
					return err
				}
				if bytes.Compare(key, low) < 0 {
					continue
				}
				if bytes.Compare(key, high) >= 0 {
					break
				}
				if flags != 0 {
					return errors.New("owned manifest range entry is not inline")
				}
				var want []byte
				if ordinal == -1 {
					want = ownedLeafManifestHeaderKey
				} else if ordinal < chunks {
					want = ownedLeafManifestChunkKey(ordinal)
				} else {
					return errors.New("owned manifest has unexpected trailing key")
				}
				if !bytes.Equal(key, want) {
					return errors.New("owned manifest has unexpected or missing ordinal key")
				}
				ordinal++
			}
		case page.PageTypeInternal:
			for i := uint16(0); i < n.Count(); i++ {
				key, child, err := n.GetInternalEntryView(i)
				if err != nil {
					return err
				}
				if bytes.Compare(key, high) >= 0 {
					break
				}
				if i+1 < n.Count() {
					next, _, err := n.GetInternalEntryView(i + 1)
					if err != nil {
						return err
					}
					if bytes.Compare(next, low) <= 0 {
						continue
					}
				}
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		default:
			return errors.New("owned manifest range is not a pager tree")
		}
		return nil
	}
	if err := visit(root, 0); err != nil {
		return err
	}
	if ordinal != chunks {
		return errors.New("owned manifest key count mismatch")
	}
	return nil
}

func loadOwnedLeafManifest(source freelist.PageSource, root, extent uint64) (*leafGenerationManifest, error) {
	header, err := readOwnedManifestEntry(source, root, extent, ownedLeafManifestHeaderKey)
	if err != nil {
		return nil, err
	}
	if len(header) != 64 || !bytes.Equal(header[:8], ownedLeafManifestMagic[:]) || !bytes.Equal(header[56:], make([]byte, 8)) {
		return nil, errors.New("invalid owned manifest header")
	}
	size, count := int(binary.LittleEndian.Uint32(header[8:12])), int(binary.LittleEndian.Uint32(header[12:16]))
	rev := binary.LittleEndian.Uint64(header[16:24])
	if size < 1 || size > ownedLeafManifestMaxBytes || count != (size+ownedLeafManifestChunkBytes-1)/ownedLeafManifestChunkBytes || rev == 0 {
		return nil, errors.New("invalid owned manifest length/chunk/revision")
	}
	if err := validateOwnedManifestKeys(source, root, extent, count); err != nil {
		return nil, err
	}
	// Length admission precedes allocation or any chunk load.
	data := make([]byte, 0, size)
	for i := 0; i < count; i++ {
		chunk, err := readOwnedManifestEntry(source, root, extent, ownedLeafManifestChunkKey(i))
		if err != nil {
			return nil, err
		}
		want := ownedLeafManifestChunkBytes
		if i == count-1 {
			want = size - i*ownedLeafManifestChunkBytes
		}
		if len(chunk) != want {
			return nil, errors.New("invalid owned manifest chunk length")
		}
		data = append(data, chunk...)
	}
	digest := sha256.Sum256(data)
	if !bytes.Equal(digest[:], header[24:56]) {
		return nil, errors.New("owned manifest content digest mismatch")
	}
	m, err := decodeLeafGenerationManifest(data, "pager-owned manifest")
	if err != nil {
		return nil, err
	}
	if err := admitOwnedLeafManifest(m); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if m.ManifestRevision != rev || !bytes.Equal(canonical, data) {
		return nil, errors.New("noncanonical owned manifest or revision mismatch")
	}
	return m, nil
}

func ownedLeafManifestDelta(m *leafGenerationManifest, oldChunks int) (*batch.Batch, error) {
	header, chunks, err := encodeOwnedLeafManifest(m)
	if err != nil {
		return nil, err
	}
	delta := batch.New(nil, math.MaxInt)
	if err := delta.Set(ownedLeafManifestHeaderKey, header); err != nil {
		return nil, err
	}
	for i, c := range chunks {
		if err := delta.Set(ownedLeafManifestChunkKey(i), c); err != nil {
			return nil, err
		}
	}
	for i := len(chunks); i < oldChunks; i++ {
		if err := delta.Delete(ownedLeafManifestChunkKey(i)); err != nil {
			return nil, err
		}
	}
	return delta, nil
}

func (db *DB) stageOwnedLeafManifestForCommit(idx *indexGen, root uint64, candidate *leafGenerationManifest, raw []uint32, seq uint64, limits *PreparedRootPublicationLimits) (uint64, []uint64, *leafGenerationManifest, []uint32, error) {
	db.mu.RLock()
	basis := db.leafGenerationManifest
	db.mu.RUnlock()
	if candidate == nil {
		candidate = basis
	}
	if err := admitOwnedLeafManifest(candidate); err != nil {
		return 0, nil, nil, nil, err
	}
	if err := admitOwnedLeafManifest(basis); err != nil {
		return 0, nil, nil, nil, err
	}
	ownedLimits := PreparedRootPublicationLimits{MaxLeafGenerations: ownedLeafManifestMaxGenerations, MaxLeafGenerationFileIDs: ownedLeafManifestMaxFileIDs, MaxPendingLeafFileIDs: ownedLeafManifestMaxFileIDs}
	if limits != nil {
		ownedLimits = *limits
		if ownedLimits.MaxLeafGenerations > ownedLeafManifestMaxGenerations {
			ownedLimits.MaxLeafGenerations = ownedLeafManifestMaxGenerations
		}
		if ownedLimits.MaxLeafGenerationFileIDs > ownedLeafManifestMaxFileIDs {
			ownedLimits.MaxLeafGenerationFileIDs = ownedLeafManifestMaxFileIDs
		}
		if ownedLimits.MaxPendingLeafFileIDs > ownedLeafManifestMaxFileIDs {
			ownedLimits.MaxPendingLeafFileIDs = ownedLeafManifestMaxFileIDs
		}
	}
	staged, err := db.stagedLeafGenerationManifestWithPendingResultAndLimit(candidate, 0, seq, &ownedLimits)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	candidate = staged.manifest.clone()
	if candidate.ManifestRevision < basis.ManifestRevision {
		candidate.ManifestRevision = basis.ManifestRevision
	}
	if candidate.ManifestRevision == math.MaxUint64 {
		return 0, nil, nil, nil, errors.New("owned manifest revision exhausted")
	}
	candidate.ManifestRevision++
	_, old, err := encodeOwnedLeafManifest(basis)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	delta, err := ownedLeafManifestDelta(candidate, len(old))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	opts := systemRootOrderedPublishOptions(db)
	// A format-local worst-case bound is charged before zipper starts. The
	// prepared publisher must include this allowance in its pre-WAL profile.
	opts.maxOutputPages = uint64(2 * (delta.Len() + 1) * ownedLeafManifestMaxDepth)
	next, retired, _, err := db.publishOrderedRootDeltaBatch(root, delta, opts)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	return next, retired, candidate, append(raw, staged.rawFileIDs...), nil
}

// publishOwnedLeafManifestLocked is used by existing serialized manifest
// maintenance. It publishes the same user root and a new intrinsic system root.
func (db *DB) publishOwnedLeafManifestLocked(m *leafGenerationManifest) error {
	db.mu.RLock()
	meta := db.meta
	db.mu.RUnlock()
	post, err := db.finalizeCommitLockedWithOptions(meta.UserRootPageID, meta.SystemRootPageID, nil, true, adaptive.Metrics{}, nil, false, nil, m, nil, finalizeCommitOptions{expectedBaseCommitSeq: meta.CommitSeq, hasExpectedBaseCommitSeq: true})
	if err != nil {
		return err
	}
	db.mu.RLock()
	published := db.leafGenerationManifest.clone()
	db.mu.RUnlock()
	*m = *published
	db.finalizeCommitPostWork(post)
	return nil
}

// CheckpointOwnedLeafManifest creates an independently durable revision of the
// pager-owned manifest. It publishes a root; no standalone FD closure is minted.
func (db *DB) CheckpointOwnedLeafManifest() (uint64, error) {
	if db == nil || db.closing.Load() {
		return 0, ErrClosed
	}
	if !db.ownedLeafManifests {
		return 0, errors.New("owned manifest format is not enabled")
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return 0, ErrClosed
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return 0, err
	}
	db.mu.RLock()
	m := db.leafGenerationManifest.clone()
	db.mu.RUnlock()
	if err := db.publishOwnedLeafManifestLocked(m); err != nil {
		return 0, err
	}
	db.mu.RLock()
	rev := db.leafGenerationManifest.ManifestRevision
	db.mu.RUnlock()
	return rev, nil
}

// stageRebuiltOwnedLeafManifest only changes the requested replacement root;
// an older slot retains the object copied with its own system root.
func stageRebuiltOwnedLeafManifest(p *pager.Pager, root uint64, m *leafGenerationManifest) (uint64, error) {
	old, err := loadOwnedLeafManifest(p, root, p.PageCount())
	if err != nil {
		return 0, err
	}
	_, chunks, err := encodeOwnedLeafManifest(old)
	if err != nil {
		return 0, err
	}
	m = m.clone()
	if m.ManifestRevision < old.ManifestRevision {
		m.ManifestRevision = old.ManifestRevision
	}
	if m.ManifestRevision == math.MaxUint64 {
		return 0, errors.New("owned manifest revision exhausted")
	}
	m.ManifestRevision++
	delta, err := ownedLeafManifestDelta(m, len(chunks))
	if err != nil {
		return 0, err
	}
	z := zipper.New(p, freelist.New(p, 0))
	z.SetOutputPageLimit(uint64(2 * (delta.Len() + 1) * ownedLeafManifestMaxDepth))
	next, _, _, err := z.Apply(root, delta)
	return next, err
}

// OwnedLeafManifestPruneStep reports allocator work, never unlinked-file work.
type OwnedManifestIntrinsicWork struct {
	CanonicalObjects, PagerReads, PagerBytes, PageCredits, ByteCredits, PhysicalIdentityChecks uint64
	RootCaptures, RootRevalidations, PinFenceChecks, SlotModeChecks                            uint64
}

func (w *OwnedManifestIntrinsicWork) Add(other OwnedManifestIntrinsicWork) {
	w.CanonicalObjects += other.CanonicalObjects
	w.PagerReads += other.PagerReads
	w.PagerBytes += other.PagerBytes
	w.PageCredits += other.PageCredits
	w.ByteCredits += other.ByteCredits
	w.PhysicalIdentityChecks += other.PhysicalIdentityChecks
	w.RootCaptures += other.RootCaptures
	w.RootRevalidations += other.RootRevalidations
	w.PinFenceChecks += other.PinFenceChecks
	w.SlotModeChecks += other.SlotModeChecks
}

type ownedManifestBoundedSource struct {
	source    freelist.PageSource
	remaining uint64
	work      *OwnedManifestIntrinsicWork
}

func (s *ownedManifestBoundedSource) ReadPage(id uint64) ([]byte, error) {
	if s.remaining == 0 {
		return nil, errors.New("owned manifest validation exhausted page credit")
	}
	s.remaining--
	s.work.PagerReads++
	image, err := s.source.ReadPage(id)
	s.work.PagerBytes += uint64(len(image))
	return image, err
}

type OwnedLeafManifestPruneStep struct {
	Intrinsic                                    OwnedManifestIntrinsicWork
	Work                                         freelist.BoundedPruneStats
	RetiredPages, FreePages, IndexHighWaterPages uint64
}

// PruneOwnedLeafManifestStep acquires fresh current/dual-slot/reader authority
// each call. The cursor belongs to the existing allocator; losing it on reopen
// only repeats scheduling. Interrupted or stale calls cannot free pages.
func (db *DB) PruneOwnedLeafManifestStep(ctx context.Context) (OwnedLeafManifestPruneStep, error) {
	if db == nil || db.closing.Load() {
		return OwnedLeafManifestPruneStep{}, ErrClosed
	}
	if !db.ownedLeafManifests {
		return OwnedLeafManifestPruneStep{}, errors.New("owned manifest format is not enabled")
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return OwnedLeafManifestPruneStep{}, ErrClosed
	}
	return db.pruneOwnedLeafManifestStepWithMaintenanceLockHeld(ctx)
}
func (db *DB) pruneOwnedLeafManifestStepWithMaintenanceLockHeld(ctx context.Context) (OwnedLeafManifestPruneStep, error) {
	var out OwnedLeafManifestPruneStep
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	roots, err := db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
	if err != nil {
		return out, err
	}
	defer roots.Release()
	out, err = db.pruneOwnedLeafManifestStepWithCapturedRoots(ctx, &ownedManifestPruneCertificate{roots: roots})
	out.Intrinsic.RootCaptures++
	return out, err
}

// A drain may retain the existing exact root pins across scheduling steps.
// Each step still validates that basis under publication serialization and
// obtains a fresh allocator capability after closing snapshot admission.
// A certificate is local to one pinned capture, never persisted or indexed.
type ownedManifestPruneCertificate struct {
	roots     *RecoverableRootSet
	certified bool
}

func (db *DB) pruneOwnedLeafManifestStepWithCapturedRoots(ctx context.Context, certificate *ownedManifestPruneCertificate) (OwnedLeafManifestPruneStep, error) {
	var out OwnedLeafManifestPruneStep
	db.writeMu.Lock()
	runLeafGenerationGCExclusivePhaseHook(true)
	defer func() { runLeafGenerationGCExclusivePhaseHook(false); db.writeMu.Unlock() }()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return out, err
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	out.Intrinsic.RootRevalidations++
	if err := certificate.roots.revalidateWithDurablePublishLockHeld(); err != nil {
		return out, err
	}
	db.rootReuseMu.Lock()
	defer db.rootReuseMu.Unlock()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	idx := db.idx.Load()
	// Every step binds the live held pager FD, captured root publication epoch,
	// index generation and fresh reader horizon. A supported publication or
	// relocation invalidates the certificate through root-set revalidation.
	out.Intrinsic.PhysicalIdentityChecks++
	if err := idx.pager.WithStableResourceFile(func(file *os.File) error {
		identity, err := rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, certificate.roots.rootResourceIndex) {
			return ErrRecoverableRootSetStale
		}
		return nil
	}); err != nil {
		return out, err
	}
	if !certificate.certified {
		for _, record := range db.durableRoot.slotRecord {
			out.Intrinsic.SlotModeChecks++
			if record.CommitSeq != 0 && !record.OwnedLeafManifest {
				return out, ErrLegacyFormatRebuildRequired
			}
		}
		// Validate all exact current/older/queued roots once per pinned capture.
		// Supported engine writes cannot mutate their immutable object pages;
		// checksums/digests alone do not certify unsupported external aliases.
		for i, root := range certificate.roots.roots {
			duplicate := false
			for _, earlier := range certificate.roots.roots[:i] {
				if earlier.SystemRootPageID == root.SystemRootPageID {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			const pages = 2 * (ownedLeafManifestMaxBytes/ownedLeafManifestChunkBytes + 2) * ownedLeafManifestMaxDepth
			out.Intrinsic.CanonicalObjects++
			out.Intrinsic.PageCredits += pages
			out.Intrinsic.ByteCredits += pages*page.PageSize + 3*ownedLeafManifestMaxBytes
			bounded := &ownedManifestBoundedSource{source: idx.pager, remaining: pages, work: &out.Intrinsic}
			if _, err := loadOwnedLeafManifest(bounded, root.SystemRootPageID, idx.pager.PageCount()); err != nil {
				return out, err
			}
		}
		certificate.certified = true
	}
	out.Intrinsic.PinFenceChecks++
	cap, err := db.durableRootReuseCapabilityV1(db.durableRoot)
	if err != nil {
		return out, err
	}
	out.Work, err = idx.allocator.PruneCOWBoundedStepV1(cap)
	if err != nil {
		return out, err
	}
	profile := idx.allocator.COWPrepareProfileV1()
	out.RetiredPages = profile.RetiredPages
	out.IndexHighWaterPages = profile.HighWater
	out.FreePages = profile.FreePages
	return out, nil
}
