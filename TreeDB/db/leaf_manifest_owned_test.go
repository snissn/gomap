package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestOwnedLeafManifestPublicationReopenAndPins(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	held := d.AcquireSnapshot()
	heldRoot := held.state.SystemRootPageID
	original, err := loadOwnedLeafManifest(held.idx.pager, heldRoot, held.idx.pager.PageCount())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := d.CheckpointOwnedLeafManifest(); err != nil {
			t.Fatal(err)
		}
	}
	for _, record := range d.durableRoot.slotRecord {
		if record.CommitSeq == 0 {
			continue
		}
		if !record.OwnedLeafManifest {
			t.Fatal("standalone record in owned store")
		}
		if _, err := loadOwnedLeafManifest(d.idx.Load().pager, record.SystemRootPageID, record.TotalPages); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 8; i++ {
		if _, err := d.PruneOwnedLeafManifestStep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	old, err := loadOwnedLeafManifest(held.idx.pager, heldRoot, held.idx.pager.PageCount())
	if err != nil || old.ManifestRevision != original.ManifestRevision {
		t.Fatalf("held root changed: %v", err)
	}
	if _, err := d.PrepareLeafGenerationManifestStableClosure(); err == nil {
		t.Fatal("owned mode minted independent manifest FD closure")
	}
	rev := d.leafGenerationManifest.ManifestRevision
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if !d.ownedLeafManifests || d.leafGenerationManifest.ManifestRevision != rev {
		t.Fatal("reopen lost owned format/revision")
	}
	entries, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if bytes.HasPrefix([]byte(entry.Name()), []byte("manifest")) {
			t.Fatalf("owned store wrote manifest sidecar %s", entry.Name())
		}
	}
}

func TestOwnedLeafManifestPhysicalFeatureRefusal(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.CheckpointOwnedLeafManifest(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, formatConfigFileName)); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, IgnoreFormatConfig: true}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatalf("physical format accepted without feature: %v", err)
	}
}

func TestOwnedLeafManifestCanonicalCorruptionAndCeiling(t *testing.T) {
	m := newLeafGenerationManifest(1)
	m.ManifestRevision = 1
	header, chunks, err := encodeOwnedLeafManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 64 || len(chunks) != 1 {
		t.Fatal("unexpected canonical shape")
	}
	m.Generations[0].FileIDs = make([]uint32, ownedLeafManifestMaxBytes)
	if _, _, err := encodeOwnedLeafManifest(m); err == nil {
		t.Fatal("oversized object admitted")
	}
}

func TestOwnedLeafManifestPublicRowsDependenciesAndVacuum(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 256, Compression: ValueLogCompressionOff}}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	pointerValue := bytes.Repeat([]byte("persistent owned payload|"), 64)
	ptr := appendPointersInNewSegment(t, dir, 0, 1, 1, 1, func(int) []byte { return pointerValue })[0]
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	pointerBatch := d.NewBatch().(*Batch)
	if err := pointerBatch.SetPointer([]byte("persistent-pointer"), ptr); err != nil {
		t.Fatal(err)
	}
	if err := pointerBatch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := pointerBatch.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("k%03d", i)), bytes.Repeat([]byte{byte(i + 1)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 32; i++ {
		value, err := d.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{byte(i + 1)}, 128)) {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	for _, resources := range d.durableRoot.slotResources {
		for _, descriptor := range resources.PhysicalDescriptors() {
			if descriptor.Kind == rootpublication.ResourceOuterLeafManifest {
				t.Fatal("pager manifest registered as FD")
			}
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	heldVacuum := d.AcquireSnapshot()
	heldVacuumManifest, err := loadOwnedLeafManifest(heldVacuum.idx.pager, heldVacuum.state.SystemRootPageID, heldVacuum.idx.pager.PageCount())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.VacuumIndexOnlineWithStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldVacuumManifest, err := loadOwnedLeafManifest(heldVacuum.idx.pager, heldVacuum.state.SystemRootPageID, heldVacuum.idx.pager.PageCount())
	if err != nil || oldVacuumManifest.ManifestRevision != heldVacuumManifest.ManifestRevision {
		t.Fatalf("relocation altered held intrinsic root: %v", err)
	}
	if value, err := heldVacuum.Get([]byte("persistent-pointer")); err != nil || !bytes.Equal(value, pointerValue) {
		t.Fatalf("held pointer after relocation: %v", err)
	}
	if err = heldVacuum.Close(); err != nil {
		t.Fatal(err)
	}
	physical, err := loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount())
	if err != nil || physical.ManifestRevision != d.leafGenerationManifest.ManifestRevision {
		t.Fatalf("live vacuum manifest root/view disagree: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		value, err := d.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{byte(i + 1)}, 128)) {
			t.Fatalf("reopen row %d: %v", i, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if value, err := d.Get([]byte("persistent-pointer")); err != nil || !bytes.Equal(value, pointerValue) {
		t.Fatalf("persistent value pointer after vacuum/reopen: %v", err)
	}
	for i := 0; i < 32; i++ {
		value, err := d.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{byte(i + 1)}, 128)) {
			t.Fatalf("offline reopen row %d: %v", i, err)
		}
	}
}

func TestOwnedLeafManifestFrozenOldReaderRefusesEverySlot(t *testing.T) {
	binary := os.Getenv("GOMAP_L_OLDREADER")
	if binary == "" {
		t.Skip("set GOMAP_L_OLDREADER to frozen797 refusal client")
	}
	for _, commits := range []int{0, 1, 3} {
		t.Run(fmt.Sprint(commits), func(t *testing.T) {
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < commits; i++ {
				if _, err := d.CheckpointOwnedLeafManifest(); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, formatConfigFileName)); err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command(binary, dir).CombinedOutput()
			if err != nil || bytes.Contains(output, []byte("OLD_READER_ACCEPTED")) {
				t.Fatalf("old reader accepted owned format after %d commits: %v %s", commits, err, output)
			}
			t.Logf("frozen797 refused after %d publications: %s", commits, output)
		})
	}
}

func TestOwnedLeafManifest512BoundedRetirement(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	held := d.AcquireSnapshot()
	oldRoot := held.state.SystemRootPageID
	original, err := loadOwnedLeafManifest(held.idx.pager, oldRoot, held.idx.pager.PageCount())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 512; i++ {
		if _, err := d.CheckpointOwnedLeafManifest(); err != nil {
			t.Fatal(err)
		}
	}
	old, err := loadOwnedLeafManifest(held.idx.pager, oldRoot, held.idx.pager.PageCount())
	if err != nil || old.ManifestRevision != original.ManifestRevision {
		t.Fatalf("held manifest changed: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	var entries, promoted, visits uint64
	for step := 0; step < 4096; step++ {
		result, err := d.PruneOwnedLeafManifestStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		w := result.Work
		if w.EntriesExamined > freelist.BoundedPruneMaxEntries || w.PromotedPages > freelist.BoundedPruneMaxPromotions || w.NodeVisits > freelist.BoundedPruneMaxNodeVisits || w.PageCredits > freelist.BoundedPruneMaxPageCredits || w.ByteCredits > freelist.BoundedPruneMaxByteCredits {
			t.Fatalf("unbounded step %+v", w)
		}
		entries += w.EntriesExamined
		promoted += w.PromotedPages
		visits += w.NodeVisits
		if w.Wrapped {
			if promoted == 0 {
				t.Fatal("no retirement progress")
			}
			t.Logf("steps=%d entries=%d promoted_pages=%d trie_visits=%d highwater=%d retired=%d", step+1, entries, promoted, visits, result.IndexHighWaterPages, result.RetiredPages)
			return
		}
	}
	t.Fatal("bounded cursor did not wrap")
}

type ownedManifestTestPageSource struct {
	image []byte
	reads int
}

func (s *ownedManifestTestPageSource) ReadPage(id uint64) ([]byte, error) {
	s.reads++
	if id != 2 {
		return nil, errors.New("unexpected page")
	}
	return s.image, nil
}

func TestOwnedLeafManifestRejectsMalformedIntrinsicObjects(t *testing.T) {
	for _, defect := range []string{"header-size", "dimension", "count", "reserved", "missing", "oversized", "ordinal", "trailing", "digest"} {
		t.Run(defect, func(t *testing.T) {
			m := newLeafGenerationManifest(1)
			m.ManifestRevision = 1
			header, chunks, err := encodeOwnedLeafManifest(m)
			if err != nil {
				t.Fatal(err)
			}
			keys := map[string][]byte{string(ownedLeafManifestHeaderKey): header, string(ownedLeafManifestChunkKey(0)): chunks[0]}
			switch defect {
			case "header-size":
				keys[string(ownedLeafManifestHeaderKey)] = header[:63]
			case "dimension":
				binary.LittleEndian.PutUint32(header[8:12], ownedLeafManifestMaxBytes+1)
			case "count":
				binary.LittleEndian.PutUint32(header[12:16], 2)
			case "reserved":
				header[63] = 1
			case "missing":
				delete(keys, string(ownedLeafManifestChunkKey(0)))
			case "oversized":
				keys[string(ownedLeafManifestChunkKey(0))] = make([]byte, ownedLeafManifestChunkBytes+1)
			case "ordinal":
				delete(keys, string(ownedLeafManifestChunkKey(0)))
				keys[string(ownedLeafManifestChunkKey(1))] = chunks[0]
			case "trailing":
				keys[string(ownedLeafManifestChunkKey(1))] = []byte("unexpected")
			case "digest":
				header[24] ^= 1
			}
			ordered := make([]string, 0, len(keys))
			for key := range keys {
				ordered = append(ordered, key)
			}
			sort.Strings(ordered)
			image := make([]byte, page.PageSize)
			builder := node.NewBuilder(image, page.PageTypeLeaf)
			builder.SetPageID(2)
			for _, key := range ordered {
				if err := builder.AddLeafEntry([]byte(key), keys[key], 0, page.ValuePtr{}); err != nil {
					t.Fatal(err)
				}
			}
			builder.Finish()
			page.UpdateChecksum(image)
			source := &ownedManifestTestPageSource{image: image}
			if _, err := loadOwnedLeafManifest(source, 2, 3); err == nil {
				t.Fatal("malformed owned object accepted")
			}
			if (defect == "dimension" || defect == "count") && source.reads != 1 {
				t.Fatalf("dimensions loaded chunks before refusal: %d", source.reads)
			}
		})
	}
}

func TestOwnedLeafManifestPublicationCutsKeepAuthority(t *testing.T) {
	for _, point := range []durabilitycut.Point{durabilitycut.BeforePublicationSealWrite, durabilitycut.AfterPublicationSealWrite, durabilitycut.BeforeMetaSync} {
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			held := d.AcquireSnapshot()
			old, err := loadOwnedLeafManifest(held.idx.pager, held.state.SystemRootPageID, held.idx.pager.PageCount())
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("owned publication cut")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == point {
					return injected
				}
				return nil
			})
			_, err = d.CheckpointOwnedLeafManifest()
			restore()
			if !errors.Is(err, injected) {
				t.Fatalf("missing publication cut: %v", err)
			}
			if point == durabilitycut.BeforeMetaSync {
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("ambiguous classification: %v", err)
				}
				if _, err = d.PruneOwnedLeafManifestStep(context.Background()); !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("ambiguous prune admitted: %v", err)
				}
			} else if _, err = d.CheckpointOwnedLeafManifest(); err != nil {
				t.Fatalf("retry: %v", err)
			}
			still, err := loadOwnedLeafManifest(held.idx.pager, held.state.SystemRootPageID, held.idx.pager.PageCount())
			if err != nil || still.ManifestRevision != old.ManifestRevision {
				t.Fatalf("cut altered held object: %v", err)
			}
			held.Close()
			if err = d.Close(); err != nil && !errors.Is(err, ErrRecoveryRequired) {
				t.Fatal(err)
			}
			d, err = Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount()); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnedLeafManifestPreparedAllowanceRejectsBeforeCommandWAL(t *testing.T) {
	for _, allocatorLimit := range []bool{false, true} {
		t.Run(fmt.Sprint(allocatorLimit), func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			cfg, _, err := LoadFormatConfig(dir)
			if err != nil {
				t.Fatal(err)
			}
			cfg.IndexOuterLeavesInValueLog = true
			cfg.RequiredFeatures = append(cfg.RequiredFeatures, RequiredFeatureOwnedLeafManifestV1)
			if err = SaveFormatConfig(dir, cfg); err != nil {
				t.Fatal(err)
			}
			d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			intent := mustRawKVCommandWALIntent(t, d, "cmd/owned-cap", "1")
			profile := d.PreparedRootPublicationBaseProfile()
			if profile.OwnedManifestOutputPageAllowance != 2176 {
				t.Fatalf("allowance=%d", profile.OwnedManifestOutputPageAllowance)
			}
			limits := &PreparedRootPublicationLimits{MaxTotalOutputPages: profile.OwnedManifestOutputPageAllowance - 1, FreelistCOW: PreparedFreelistCOWLimitsForPages(profile.PagerPages + profile.OwnedManifestOutputPageAllowance)}
			if allocatorLimit {
				limits.MaxTotalOutputPages = profile.OwnedManifestOutputPageAllowance
				limits.FreelistCOW.MaxAllocatedPages = profile.FreelistCOW.AllocatedPages
			}
			_, _, err = d.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilderWithPreparedLimits(
				[]OrderedRootDeltaPublishInput{{Iter: mustFrozenSystemMemtable(t, "root/b", "value").NewIterator(nil, nil)}},
				func() error { return nil }, intent, nil,
				func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
					t.Fatal("system builder ran after owned pre-WAL refusal")
					return nil, nil
				}, limits)
			if err == nil || intent.AssignedLSN() != 0 {
				t.Fatalf("owned pre-WAL limit accepted: err=%v LSN=%d", err, intent.AssignedLSN())
			}
			if err = d.commandWALPoisonedError(); err != nil {
				t.Fatalf("pre-WAL refusal poisoned DB: %v", err)
			}
		})
	}
}

func TestOwnedLeafManifestOlderSlotRecoveryAndOldReaderRefusal(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = d.CheckpointOwnedLeafManifest(); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "index.db"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var selected page.DurableMetaV1
	for slot := int64(0); slot < 2; slot++ {
		image := make([]byte, page.PageSize)
		if _, err = f.ReadAt(image, slot*page.PageSize); err != nil {
			t.Fatal(err)
		}
		meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
		if err != nil {
			t.Fatal(err)
		}
		if meta.CommitSeq > selected.CommitSeq {
			selected = meta
		}
	}
	bad := []byte{0xff}
	if _, err = f.WriteAt(bad, int64(selected.RootRecordPageID)*page.PageSize+400); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("owned older-slot fallback refused: %v", err)
	}
	if d.meta.CommitSeq >= selected.CommitSeq {
		t.Fatalf("did not select older owned slot: %d", d.meta.CommitSeq)
	}
	if !d.ownedLeafManifests {
		t.Fatal("older fallback lost ownership mode")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, formatConfigFileName)); err != nil {
		t.Fatal(err)
	}
	if binary := os.Getenv("GOMAP_L_OLDREADER"); binary != "" {
		output, err := exec.Command(binary, dir).CombinedOutput()
		if err != nil || bytes.Contains(output, []byte("OLD_READER_ACCEPTED")) {
			t.Fatalf("old binary selected older owned slot: %v %s", err, output)
		}
		t.Logf("frozen797 refused corrupt-newest/older-owned fallback: %s", output)
	}
}

// This opt-in whole GC fixture is the new-layout counterpart of frozen797's
// standalone revision ladder. Setup/publication work is retained separately;
// it never interprets internal promotions as durably unlinked files.
func TestR1OwnedManifestWholeGC5095(t *testing.T) {
	n, err := strconv.Atoi(os.Getenv("GOMAP_L_GC_REVISIONS"))
	if err != nil {
		t.Skip("opt-in whole GC capture")
	}
	if n < 1 || n > 512 {
		t.Fatal("outside frozen 128/512 ladder")
	}
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var setupA, setupB runtime.MemStats
	runtime.ReadMemStats(&setupA)
	setupStart := time.Now()
	for i := 0; i < n; i++ {
		if _, err = d.CheckpointOwnedLeafManifest(); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&setupB)
	setupElapsed := time.Since(setupStart)
	before := d.idx.Load().allocator.COWPrepareProfileV1()
	var a, b runtime.MemStats
	phaseDurations := make([]time.Duration, 0, 4096)
	var phaseStart time.Time
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
		} else {
			phaseDurations = append(phaseDurations, time.Since(phaseStart))
		}
	})
	defer unregister()
	runtime.ReadMemStats(&a)
	start := time.Now()
	stats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	elapsed := time.Since(start)
	runtime.ReadMemStats(&b)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ManifestRevisionsDeleted != 0 || stats.ManifestRevisionBytesDeleted != 0 {
		t.Fatal("internal reuse mislabeled as unlinked files")
	}
	after := d.idx.Load().allocator.COWPrepareProfileV1()
	for _, record := range d.durableRoot.slotRecord {
		if _, err = loadOwnedLeafManifest(d.idx.Load().pager, record.SystemRootPageID, record.TotalPages); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("revisions=%d elapsed_ns=%d allocated_bytes=%d allocations=%d setup_elapsed_ns=%d setup_allocated_bytes=%d fence_hold_p95_ns=%d fence_hold_p99_ns=%d fence_samples=%d before=%+v after=%+v stats=%+v", n, elapsed.Nanoseconds(), b.TotalAlloc-a.TotalAlloc, b.Mallocs-a.Mallocs, setupElapsed.Nanoseconds(), setupB.TotalAlloc-setupA.TotalAlloc, leafGenerationGCBenchmarkPercentile(phaseDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds(), len(phaseDurations), before, after, stats)
}

func TestOwnedLeafManifestDrainedPinsReuseAndHighWaterPlateau(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var samples []uint64
	var reused uint64
	for group := 0; group < 6; group++ {
		for i := 0; i < 16; i++ {
			before := d.idx.Load().allocator.COWPrepareProfileV1()
			oldRoot := d.meta.SystemRootPageID
			if _, err = d.CheckpointOwnedLeafManifest(); err != nil {
				t.Fatal(err)
			}
			if before.FreePages > 0 && d.meta.SystemRootPageID != oldRoot && d.meta.SystemRootPageID < before.HighWater {
				reused++
			}
		}
		if _, err = d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
			t.Fatal(err)
		}
		profile := d.idx.Load().allocator.COWPrepareProfileV1()
		samples = append(samples, profile.HighWater)
		if profile.RetiredPages > 128 {
			t.Fatalf("drained retirement debt=%d", profile.RetiredPages)
		}
	}
	t.Logf("highwater=%v reused_system_roots=%d", samples, reused)
	if reused == 0 {
		t.Fatal("promoted free pages were never actually reused")
	}
	minimum, maximum := samples[2], samples[2]
	for _, n := range samples[2:] {
		if n < minimum {
			minimum = n
		}
		if n > maximum {
			maximum = n
		}
	}
	if maximum-minimum > 128 {
		t.Fatalf("equal-revision drained highwater did not plateau: %v", samples)
	}
}

func TestOwnedLeafManifestCanceledAndClosedMaintenance(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := d.idx.Load().allocator.COWPrepareProfileV1()
	if _, err = d.PruneOwnedLeafManifestStep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled step: %v", err)
	}
	if after := d.idx.Load().allocator.COWPrepareProfileV1(); after != before {
		t.Fatal("canceled step mutated allocator")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = d.CheckpointOwnedLeafManifest(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed publication: %v", err)
	}
	if _, err = d.PruneOwnedLeafManifestStep(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed prune: %v", err)
	}
}

func TestOwnedLeafManifestQueuedBuildGroupAndMixedFormatRefusal(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true, rootPublicationFixedDelay: 100 * time.Millisecond}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(leafLog)
	defer leafLog.Close()
	held := d.AcquireSnapshot()
	old, err := loadOwnedLeafManifest(held.idx.pager, held.state.SystemRootPageID, held.idx.pager.PageCount())
	if err != nil {
		t.Fatal(err)
	}
	before := d.rootPublication.coordinator.Stats()
	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	pruneDone := make(chan error, 1)
	for i := 0; i < 2; i++ {
		b := d.NewPhysicalBatch().(*Batch)
		if err = b.Set([]byte(fmt.Sprintf("group/%d", i)), []byte("queued owned value")); err != nil {
			t.Fatal(err)
		}
		if err = b.SetRootPublicationBuildGroup(group, i == 1); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			err = b.Write()
		} else {
			err = b.WriteSync()
		}
		if err != nil {
			t.Fatal(err)
		}
		b.Close()
		if i == 0 {
			now := d.rootPublication.coordinator.Stats()
			if now.VisibleCommitSeq != before.VisibleCommitSeq || now.DurableCommitSeq != before.DurableCommitSeq {
				t.Fatal("private candidate escaped build group")
			}
			started := make(chan struct{})
			go func() { close(started); _, err := d.PruneOwnedLeafManifestStep(context.Background()); pruneDone <- err }()
			<-started
			select {
			case err := <-pruneDone:
				t.Fatalf("private-build writer fence escaped: %v", err)
			default:
			}
		}
	}
	group.Close()
	if err := <-pruneDone; err != nil {
		t.Fatal(err)
	}
	if d.rootPublication.coordinator.Stats().DurableCommitSeq != before.DurableCommitSeq+1 {
		t.Fatal("queued group did not publish exactly one root")
	}
	live, err := loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount())
	if err != nil {
		t.Fatal(err)
	}
	if live.ManifestRevision <= old.ManifestRevision {
		t.Fatal("queued root lost intrinsic revision")
	}
	still, err := loadOwnedLeafManifest(held.idx.pager, held.state.SystemRootPageID, held.idx.pager.PageCount())
	if err != nil || still.ManifestRevision != old.ManifestRevision {
		t.Fatalf("queued root altered held object: %v", err)
	}
	held.Close()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if value, err := d.Get([]byte(fmt.Sprintf("group/%d", i))); err != nil || string(value) != "queued owned value" {
			t.Fatalf("replayed group row: %q %v", value, err)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, _, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	kept := cfg.RequiredFeatures[:0]
	for _, feature := range cfg.RequiredFeatures {
		if feature != RequiredFeatureOwnedLeafManifestV1 {
			kept = append(kept, feature)
		}
	}
	cfg.RequiredFeatures = kept
	if err = SaveFormatConfig(dir, cfg); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("owned feature removal accepted: %v", err)
	}
	legacyDir := t.TempDir()
	legacy, err := Open(Options{Dir: legacyDir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if converted, err := Open(Options{Dir: legacyDir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		if converted != nil {
			converted.Close()
		}
		t.Fatalf("legacy store silently converted: %v", err)
	}
}
