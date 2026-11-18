package treedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type ordinaryLeafIndexFile struct{ *os.File }

func (f ordinaryLeafIndexFile) ReadPage(id uint64) ([]byte, error) {
	buf := make([]byte, page.PageSize)
	_, err := f.ReadAt(buf, int64(id)*page.PageSize)
	return buf, err
}

type ordinaryLeafSlot struct {
	record      rootpublication.DurableRootRecordV1
	manifest    rootpublication.DependencyManifestEntryV1
	generation  map[uint32]uint64
	packedFiles []uint32
	pointer     page.LeafLogPtr
	target      bool
	frontier    uint64
	path        string
}

// The root capture owns the original dependency and its namespace. This
// reads that immutable revision as an observation and acquires no publication
// authority. The operational manifest.json is never an input.
func readOrdinaryLeafManifest(dir string, entry rootpublication.DependencyManifestEntryV1) (_ map[uint32]uint64, retErr error) {
	name := fmt.Sprintf("manifest.durable.%016x.json", entry.Generation)
	relative := filepath.Join("leaf_vlog", name)
	if entry.Generation == 0 || entry.LogicalLane != "manifest" || entry.ResourceID != name ||
		entry.DiagnosticPath != relative || entry.Namespace == nil || entry.Namespace.NewName != name ||
		entry.Frontier.Bytes == 0 || entry.Frontier.Bytes >= math.MaxInt64 || entry.Digest == ([32]byte{}) {
		return nil, fmt.Errorf("invalid immutable dependency for revision %d: %+v", entry.Generation, entry)
	}

	parentPath := filepath.Join(dir, "maindb", "leaf_vlog")
	parent, err := os.Open(parentPath)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, parent.Close()) }()
	parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return nil, err
	}
	if !rootpublication.SamePhysicalIdentity(parentIdentity, entry.Namespace.ParentIdentity) {
		return nil, fmt.Errorf("leaf manifest parent identity differs for revision %d", entry.Generation)
	}

	file, err := os.Open(filepath.Join(parentPath, name))
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return nil, err
	}
	if !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
		return nil, fmt.Errorf("leaf manifest physical identity differs for revision %d", entry.Generation)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(entry.Frontier.Bytes) {
		return nil, fmt.Errorf("leaf manifest frontier differs for revision %d: size=%d frontier=%d", entry.Generation, info.Size(), entry.Frontier.Bytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != entry.Frontier.Bytes || sha256.Sum256(data) != entry.Digest {
		return nil, fmt.Errorf("immutable manifest bytes differ from durable revision %d", entry.Generation)
	}
	var view struct {
		Revision    uint64 `json:"manifest_revision"`
		Generations []struct {
			ID    uint64   `json:"generation_id"`
			State string   `json:"state"`
			Files []uint32 `json:"file_ids"`
		} `json:"generations"`
	}
	if err := json.Unmarshal(data, &view); err != nil {
		return nil, err
	}
	if view.Revision != entry.Generation {
		return nil, fmt.Errorf("decoded manifest revision=%d differs from dependency=%d", view.Revision, entry.Generation)
	}
	active := make(map[uint32]uint64)
	for _, generation := range view.Generations {
		if generation.State == "deleted" || generation.State == "retiring" {
			continue
		}
		for _, fileID := range generation.Files {
			active[fileID] = generation.ID
		}
	}
	return active, nil
}

// Decode each on-disk meta slot with the production codecs. The original
// recoverable-root capture owns both roots throughout this inspection.
func inspectOrdinaryLeafSlots(t *testing.T, backend *backenddb.DB, dir, name string, id, seedID []byte) [2]ordinaryLeafSlot {
	t.Helper()
	roots, err := backend.CaptureRecoverableRootSetForInspection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := roots.ReleaseWithError(); err != nil {
			t.Error(err)
		}
	}()
	index, err := os.Open(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := index.Close(); err != nil {
			t.Error(err)
		}
	}()
	source := ordinaryLeafIndexFile{index}
	captured := roots.Roots()
	var slots [2]ordinaryLeafSlot
	for i := range slots {
		slot := &slots[i]
		image, err := source.ReadPage(uint64(i))
		if err != nil || !page.VerifyChecksumNonMutating(image) {
			t.Fatalf("meta slot %d: %v", i, err)
		}
		meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
		if err != nil {
			t.Fatalf("meta slot %d: %v", i, err)
		}
		image, err = source.ReadPage(meta.RootRecordPageID)
		if err != nil {
			t.Fatal(err)
		}
		record, err := rootpublication.DecodeDurableRootRecordV1(image, meta.RootRecordPageID, meta.RootRecordDigest)
		if err != nil || record.CommitSeq != meta.CommitSeq || record.DurableSeq != meta.DurableSeq {
			t.Fatalf("root slot %d meta=%+v error=%v", i, meta, err)
		}
		slot.record = record
		rootIndex := slices.IndexFunc(captured, func(root backenddb.RecoverableRoot) bool {
			return root.Durable && root.CommitSeq == record.CommitSeq && root.AppliedCommandLSN == record.AppliedCommandLSN &&
				root.UserRootPageID == record.UserRootPageID && root.SystemRootPageID == record.SystemRootPageID &&
				root.MaxEntryRevision == record.MaxEntryRevision
		})
		if rootIndex < 0 {
			t.Fatalf("slot %d missing from exact captured roots: %+v", i, record)
		}
		root := captured[rootIndex]
		dependencies, err := rootpublication.LoadDependencyManifestV1(source, record.Manifest)
		if err != nil {
			t.Fatalf("slot %d dependency manifest: %v", i, err)
		}
		entries := dependencies.Entries()
		for _, entry := range entries {
			if entry.Kind == rootpublication.ResourceOuterLeafPack {
				slot.packedFiles = append(slot.packedFiles, page.ValueLogSegmentID(uint32(entry.Generation)))
			}
			if entry.Kind != rootpublication.ResourceOuterLeafManifest {
				continue
			}
			if slot.manifest.Generation != 0 {
				t.Fatalf("slot %d has multiple leaf manifests", i)
			}
			slot.manifest = entry
			slot.generation, err = readOrdinaryLeafManifest(dir, entry)
			if err != nil {
				t.Fatalf("slot %d immutable manifest: %v", i, err)
			}
		}
		if id == nil {
			continue
		}
		func() {
			snap := roots.AcquireSnapshotForRoot(root)
			if snap == nil {
				t.Fatalf("slot %d snapshot unavailable", i)
			}
			defer func() {
				if err := snap.Close(); err != nil {
					t.Error(err)
				}
			}()
			descriptor, err := snap.GetAtRoot(root.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
			if err != nil || len(descriptor) != 8 {
				t.Fatalf("slot %d primary descriptor=%x err=%v", i, descriptor, err)
			}
			collectionRoot := binary.BigEndian.Uint64(descriptor)
			probeID := id
			row, err := snap.GetAtRoot(collectionRoot, probeID)
			if errors.Is(err, ErrKeyNotFound) || err == nil && row == nil {
				// An older root before the insert still owns its own existing
				// reachable leaf. Never project the current pointer into it.
				probeID = seedID
				row, err = snap.GetAtRoot(collectionRoot, probeID)
			} else {
				slot.target = true
			}
			if err != nil || len(row) == 0 {
				t.Fatalf("slot %d retained row %q: %v", i, probeID, err)
			}
			if err := bson.Raw(row).Validate(); err != nil {
				t.Fatalf("slot %d retained row %q is not BSON: %v", i, probeID, err)
			}
			reader, err := snap.NativeReaderAtRoot(collectionRoot)
			if err != nil {
				t.Fatal(err)
			}
			// Borrow the established fixed-scratch observer from the standalone
			// dictionary test. The sentinel only terminates the pointer probe.
			observed := errors.New("observed exact outer-leaf pointer")
			var keyScratch, leafScratch [page.PageSize]byte
			_, err = reader.GetEntryExactWithFixedScratch(probeID, keyScratch[:], leafScratch[:], func(ptr page.LeafLogPtr, _ []byte) ([]byte, error) {
				slot.pointer = ptr
				return nil, observed
			})
			if !errors.Is(err, observed) || slot.pointer.FileID == 0 {
				t.Fatalf("slot %d target needs a real outer-leaf pointer: ptr=%+v err=%v", i, slot.pointer, err)
			}
			file, identity, err := snap.PinnedValueLogFile(slot.pointer.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			limits := valuelog.COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
			shape, err := valuelog.InspectCOWLeafRecord(file, slot.pointer.ValuePtr(), limits)
			if err != nil {
				t.Fatalf("slot %d inspect reachable leaf record: %v", i, err)
			}
			recordEnd := slot.pointer.Offset - 4 + shape.RecordBytes
			for _, entry := range entries {
				leafDependency := entry.Kind == rootpublication.ResourceOuterLeafLog ||
					entry.Kind == rootpublication.ResourceOuterLeafPack
				if leafDependency && entry.Generation == uint64(slot.pointer.ValueLogFileID()) {
					if slot.frontier != 0 || !rootpublication.SamePhysicalIdentity(identity, entry.Identity) || entry.Frontier.Bytes < recordEnd {
						t.Fatalf("slot %d pointer=%+v outside its unique durable identity/frontier: %+v", i, slot.pointer, entry)
					}
					slot.frontier, slot.path = entry.Frontier.Bytes, file.Name()
				}
			}
			if slot.frontier == 0 {
				t.Fatalf("slot %d root-reachable pointer=%+v lacks a physical dependency", i, slot.pointer)
			}
			rawID := page.ValueLogSegmentID(slot.pointer.FileID)
			t.Logf("slot=%d commit=%d applied_lsn=%d roots=%d/%d collection_root=%d row=%q raw=%d tagged=%d offset=%d frontier=%d manifest=%d digest=%x mapped_generation=%d",
				i, record.CommitSeq, record.AppliedCommandLSN, record.UserRootPageID, record.SystemRootPageID, collectionRoot, probeID, rawID,
				slot.pointer.ValueLogFileID(), slot.pointer.Offset, slot.frontier, slot.manifest.Generation, slot.manifest.Digest, slot.generation[rawID])
		}()
	}
	if err := roots.Revalidate(); err != nil {
		t.Fatal(err)
	}
	return slots
}

func newerOrdinaryLeafSlot(slots [2]ordinaryLeafSlot) ordinaryLeafSlot {
	if slots[1].record.DurableSeq > slots[0].record.DurableSeq {
		return slots[1]
	}
	return slots[0]
}

func TestOrdinaryLeafRotationRetainsExactManifestThroughReopen(t *testing.T) {
	ctx := context.Background()
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("exact leaf-generation pack requires retained-parent namespace support")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.DisableBackgroundPrune = true
	opts.FlushApplyConcurrency = 1
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 8 << 10
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	closeOwners := func() error {
		var managerErr, databaseErr error
		if manager != nil {
			owner := manager
			manager = nil
			managerErr = owner.Close()
		}
		if database != nil {
			owner := database
			database = nil
			databaseErr = owner.Close()
		}
		return errors.Join(managerErr, databaseErr)
	}
	defer func() {
		if err := closeOwners(); err != nil {
			t.Error(err)
		}
	}()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	const name = "db.leaf_rollover"
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	insert := func(first, count int) ([]byte, []byte) {
		t.Helper()
		ids, docs := make([][]byte, count), make([][]byte, count)
		for i := range ids {
			id := fmt.Sprintf("row-%06d", first+i)
			ids[i] = []byte(id)
			payload := make([]byte, 0, 192)
			for block := 0; block < 6; block++ {
				sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", id, block)))
				payload = append(payload, sum[:]...)
			}
			var err error
			docs[i], err = bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(first + i)}, {Key: "payload", Value: payload}})
			if err != nil {
				t.Fatal(err)
			}
		}
		if inserted, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil || len(inserted) != count {
			t.Fatalf("ordinary InsertBatchValidatedBSON ack count=%d want=%d err=%v", len(inserted), count, err)
		}
		if err := backend.SyncNativeSemanticPrefix(); err != nil {
			t.Fatalf("native durable acknowledgement: %v", err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("public Checkpoint: %v", err)
		}
		return ids[count-1], docs[count-1]
	}

	seed, _ := insert(0, 128)
	for i := 128; i < 512; i += 128 {
		insert(i, 128)
	}
	if err := closeOwners(); err != nil {
		t.Fatalf("checked seed manager and DB Close before pack: %v", err)
	}
	// Reuse the maintained closed-owner tooling route from the standalone
	// dictionary fixture. The generic pack never borrows an active native owner.
	packPredecessor := func() (proof ordinaryLeafSlot, retErr error) {
		maintenance, cleanup, err := OpenBackend(OptionsFor(ProfileCommandWALRelaxed, opts.Dir))
		if err != nil {
			return proof, err
		}
		defer func() { retErr = errors.Join(retErr, cleanup()) }()
		plan, err := maintenance.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
		if err != nil || len(plan.CandidateGenerationIDs) == 0 {
			return proof, fmt.Errorf("fixture requires sealed live generation from the ordinary producer: %+v %v", plan, err)
		}
		packed, err := maintenance.LeafGenerationPack(ctx, LeafGenerationPackOptions{GenerationIDs: plan.CandidateGenerationIDs[:1], Force: true, Sync: true})
		if err != nil || len(packed.CreatedFileIDs) == 0 {
			return proof, fmt.Errorf("fixture requires actual committed pack: %+v %v", packed, err)
		}
		visible, ok := maintenance.StateToken()
		if !ok || visible.CommitSeq == 0 {
			return proof, errors.New("packed predecessor has no visible commit")
		}
		t.Logf("pack returned before maintenance checkpoint: visible_commit=%d lsn=%d packed_files=%v",
			visible.CommitSeq, visible.AppliedCommandLSN, packed.CreatedFileIDs)
		// The queued command-WAL path can return after admitting the visible
		// root. Seal that prefix before inspecting the physical slots.
		if err := maintenance.Checkpoint(); err != nil {
			return proof, fmt.Errorf("checkpoint packed predecessor: %w", err)
		}
		proof = newerOrdinaryLeafSlot(inspectOrdinaryLeafSlots(t, maintenance, opts.Dir, name, nil, nil))
		t.Logf("packed exact predecessor after checkpoint: durable_commit=%d lsn=%d revision=%d digest=%x",
			proof.record.CommitSeq, proof.record.AppliedCommandLSN, proof.manifest.Generation, proof.manifest.Digest)
		if proof.record.CommitSeq < visible.CommitSeq {
			return proof, fmt.Errorf("checkpoint selected durable commit %d before packed visible commit %d", proof.record.CommitSeq, visible.CommitSeq)
		}
		for _, rawID := range packed.CreatedFileIDs {
			if proof.generation[rawID] == 0 {
				return proof, fmt.Errorf("exact predecessor revision %d omits actual packed file %d", proof.manifest.Generation, rawID)
			}
		}
		return proof, nil
	}
	predecessor, err := packPredecessor()
	if err != nil {
		t.Fatalf("maintained pack and checked maintenance Close: %v", err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatalf("resume public cached owner after pack: %v", err)
	}
	resumed := newerOrdinaryLeafSlot(inspectOrdinaryLeafSlots(t, database.backend, opts.Dir, name, nil, nil))
	if resumed.manifest.Generation != predecessor.manifest.Generation || resumed.manifest.Digest != predecessor.manifest.Digest ||
		resumed.record.SystemRootPageID != predecessor.record.SystemRootPageID ||
		resumed.record.AppliedCommandLSN != predecessor.record.AppliedCommandLSN {
		t.Fatalf("resumed owner lost the packed exact predecessor: packed=%+v resumed=%+v", predecessor, resumed)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	collection, err = manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}

	// First create a real ordinary writer and prove its reachable leaf under
	// this resumed owner. Initial segment creation also increments rotations,
	// so it cannot be part of the successor rotation count.
	prime, _ := insert(512, 1)
	primed := newerOrdinaryLeafSlot(inspectOrdinaryLeafSlots(t, backend, opts.Dir, name, prime, seed))
	if !primed.target || primed.pointer.FileID == 0 || slices.Contains(primed.packedFiles, primed.pointer.FileID) || primed.record.AppliedCommandLSN <= resumed.record.AppliedCommandLSN {
		t.Fatalf("resumed owner did not publish its initial ordinary leaf: %+v", primed)
	}
	primeLane, primeSeq := valuelog.DecodeFileID(primed.pointer.ValueLogFileID())
	leafDir := filepath.Join(opts.Dir, "maindb", "leaf_vlog")
	priorFiles, err := os.ReadDir(leafDir)
	if err != nil {
		t.Fatal(err)
	}
	before := make(map[string]bool, len(priorFiles))
	for _, f := range priorFiles {
		before[f.Name()] = true
	}

	rotationCounts := func() (total, idle uint64) {
		t.Helper()
		stats := database.Stats()
		if stats["treedb.cache.leaf_log_lanes.configured"] != "1" ||
			stats["treedb.cache.leaf_log_lanes.lane.00.segments_active"] != "1" {
			t.Fatalf("fixture requires one already-active ordinary leaf writer")
		}
		const prefix = "treedb.cache.leaf_log_lanes.segment_rotations"
		var err error
		total, err = strconv.ParseUint(stats[prefix+"_total"], 10, 64)
		if err != nil {
			t.Fatalf("ordinary leaf rotation counter: %v", err)
		}
		idle, err = strconv.ParseUint(stats[prefix+"_idle_total"], 10, 64)
		if err != nil || idle > total {
			t.Fatalf("idle leaf rotations=%d, total=%d: %v", idle, total, err)
		}
		return total, idle
	}
	beforeTotal, beforeIdle := rotationCounts()
	target, want := insert(513, 128)
	afterTotal, afterIdle := rotationCounts()
	if afterTotal-afterIdle <= beforeTotal-beforeIdle {
		t.Fatalf("fixture needs a real non-idle leaf rotation during the ordinary write: total=%d->%d idle=%d->%d", beforeTotal, afterTotal, beforeIdle, afterIdle)
	}
	t.Logf("ordinary leaf rotations total=%d->%d idle=%d->%d", beforeTotal, afterTotal, beforeIdle, afterIdle)
	slots := inspectOrdinaryLeafSlots(t, backend, opts.Dir, name, target, seed)
	current := newerOrdinaryLeafSlot(slots)
	lane, seq := valuelog.DecodeFileID(current.pointer.ValueLogFileID())
	if current.record.CommitSeq <= primed.record.CommitSeq || current.record.AppliedCommandLSN <= primed.record.AppliedCommandLSN ||
		!current.target || current.pointer.FileID == 0 || slices.Contains(current.packedFiles, current.pointer.FileID) || lane != primeLane || seq <= primeSeq ||
		before[filepath.Base(current.path)] || predecessor.generation[page.ValueLogSegmentID(current.pointer.FileID)] != 0 {
		t.Fatalf("fixture did not prove a newly emitted, reachable leaf on the already-active ordinary lane: primed=%+v current=%+v", primed, current)
	}
	for i, slot := range slots {
		if slot.manifest.Generation == 0 {
			t.Fatalf("slot %d commit=%d has no exact immutable leaf manifest", i, slot.record.CommitSeq)
		}
		rawID := page.ValueLogSegmentID(slot.pointer.FileID)
		if slot.generation[rawID] == 0 {
			t.Errorf("exact generation coverage: slot=%d commit=%d raw_file=%d offset=%d frontier=%d token_revision=%d token_digest=%x",
				i, slot.record.CommitSeq, rawID, slot.pointer.Offset, slot.frontier, slot.manifest.Generation, slot.manifest.Digest)
		}
	}
	if err := closeOwners(); err != nil {
		t.Fatalf("checked pre-reopen manager and DB Close: %v", err)
	}
	t.Log("checked original manager and DB Close before public reopen")
	database, err = Open(opts)
	if err != nil {
		t.Fatalf("public reopen: %v", err)
	}
	// Match the A1 order. No manager, read or explicit checkpoint precedes
	// the first public plan on this newly opened owner.
	if _, err := database.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{}); err != nil {
		t.Errorf("first public LeafGenerationPlan after reopen: %v", err)
	}
	reopened := newerOrdinaryLeafSlot(inspectOrdinaryLeafSlots(t, database.backend, opts.Dir, name, target, seed))
	if !reopened.target || reopened.pointer != current.pointer || slices.Contains(reopened.packedFiles, reopened.pointer.FileID) || reopened.record.AppliedCommandLSN != current.record.AppliedCommandLSN {
		t.Errorf("reopened target authority changed: acknowledged pointer=%+v lsn=%d, reopened pointer=%+v lsn=%d",
			current.pointer, current.record.AppliedCommandLSN, reopened.pointer, reopened.record.AppliedCommandLSN)
	}
	if reopened.generation[page.ValueLogSegmentID(reopened.pointer.FileID)] == 0 {
		t.Errorf("reopened exact leaf manifest revision=%d digest=%x omits target raw file=%d",
			reopened.manifest.Generation, reopened.manifest.Digest, page.ValueLogSegmentID(reopened.pointer.FileID))
	}
	sampleGCState := func(label string) backenddb.StateToken {
		t.Helper()
		backendToken, backendOK := database.backend.StateToken()
		capture, err := database.CaptureNativeSemanticState()
		if err != nil {
			t.Fatalf("%s capture native semantic state: %v", label, err)
		}
		cacheToken, cacheOK := capture.Snapshot().StateToken()
		if err := capture.Close(); err != nil {
			t.Fatalf("%s close original native capture: %v", label, err)
		}
		t.Logf("%s backend=%+v cache=%+v", label, backendToken, cacheToken)
		if !backendOK || !cacheOK || backendToken != cacheToken {
			t.Errorf("%s backend and cache state differ: backend=%+v (ok=%t) cache=%+v (ok=%t)", label, backendToken, backendOK, cacheToken, cacheOK)
		}
		return backendToken
	}
	// Use the real pack producer to put a synced but unpublished segment on
	// disk. Committed packs already account for all their leaf files, so they
	// cannot be relied on to leave a segment for dry-run reconciliation.
	leaf := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true})
	if err := builder.AddLeafEntry(target, want, node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	builder.FinishNoNode()
	beforePrepare := sampleGCState("before preparing unpublished pack segment")
	prepared, err := database.backend.PrepareLeafGenerationPackStableClosure(ctx, [][]byte{leaf})
	if err != nil {
		t.Fatalf("prepare unpublished pack segment: %v", err)
	}
	defer func() {
		if prepared != nil {
			if err := prepared.Abandon(); err != nil {
				t.Error(err)
			}
		}
	}()
	segments := prepared.Segments()
	observed := prepared.Observations()
	if len(segments) != 1 || observed.ContentSyncs == 0 || observed.NamespaceSyncs == 0 {
		t.Fatalf("fixture requires one synced and promoted pack segment: segments=%v observations=%+v", segments, observed)
	}
	segment := segments[0]
	rawID := page.ValueLogSegmentID(segment.FileID)
	info, err := os.Stat(segment.Path)
	if rawID == 0 || reopened.generation[rawID] != 0 || err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		t.Fatalf("fixture requires a new physical segment outside the durable manifest: segment=%+v info=%v err=%v", segment, info, err)
	}
	if afterPrepare := sampleGCState("after preparing unpublished pack segment"); afterPrepare != beforePrepare {
		t.Fatalf("preparing the pack segment published state: before=%+v after=%+v", beforePrepare, afterPrepare)
	}
	runReconcile := func(label string) {
		t.Helper()
		beforeGC := sampleGCState("before " + label)
		gc, gcErr := database.LeafGenerationGC(ctx, LeafGenerationGCOptions{DryRun: true})
		afterGC := sampleGCState("after " + label)
		t.Logf("%s: %+v %v", label, gc, gcErr)
		if gcErr != nil || gc.GenerationsDeleted != 0 || gc.FilesDeleted != 0 {
			t.Errorf("%s: %+v %v", label, gc, gcErr)
		}
		if afterGC.LeafGenerationStateVersion <= beforeGC.LeafGenerationStateVersion {
			t.Errorf("fixture did not advance leaf generation state during %s: before=%+v after=%+v", label, beforeGC, afterGC)
		}
		afterGC.LeafGenerationStateVersion = beforeGC.LeafGenerationStateVersion
		if afterGC != beforeGC {
			t.Errorf("%s changed the logical root or command frontier: before=%+v after=%+v", label, beforeGC, afterGC)
		}
	}
	runReconcile("leaf generation GC dry run with promoted segment")
	abandonErr := prepared.Abandon()
	prepared = nil
	if abandonErr != nil {
		t.Fatalf("abandon unpublished pack segment: %v", abandonErr)
	}
	if _, err := os.Stat(segment.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned pack segment still present: %s, stat err=%v", segment.Path, err)
	}
	runReconcile("leaf generation GC dry run after abandonment")
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	collection, err = manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := collection.Get(target)
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("reopened BSON Get %q: bytes=%d want=%d err=%v", target, len(got), len(want), err)
	} else if n, ok := bson.Raw(got).Lookup("n").Int64OK(); !ok || n != 640 {
		t.Errorf("reopened BSON field n=(%d,%t), want int64(640)", n, ok)
	} else {
		t.Logf("reopened typed BSON target %q matched exactly", target)
	}
	if err := closeOwners(); err != nil {
		t.Fatalf("checked final manager and DB Close: %v", err)
	}
	t.Log("checked final manager and DB Close")
}
