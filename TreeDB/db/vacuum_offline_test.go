package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestVacuumIndexOffline_PreservesDataAndShrinksFile(t *testing.T) {
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:               dir,
		ChunkSize:         chunkSize,
		KeepRecent:        1,
		PreferAppendAlloc: true, // intentionally bloat index.db
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	value := bytes.Repeat([]byte("v"), 200) // inline-ish to force page pressure
	for round := 0; round < 8; round++ {
		b := d.NewBatch()
		for i := 0; i < 5000; i++ {
			// Rewrite the same keyset to create lots of retired pages (index bloat)
			// when PreferAppendAlloc is enabled.
			k := []byte(fmt.Sprintf("k%06d", i))
			if err := b.Set(k, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	if err := VacuumIndexOffline(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	verify, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open after: %v", err)
	}
	defer func() { _ = verify.Close() }()

	got, err := verify.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOffline_CommandWALPreservesDataAndShrinksFile(t *testing.T) {
	dir := t.TempDir()
	chunkSize := int64(4 * 1024 * 1024)

	d, err := Open(Options{
		Dir:                    dir,
		ChunkSize:              chunkSize,
		KeepRecent:             1,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
		PreferAppendAlloc:      true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	value := bytes.Repeat([]byte("v"), 200)
	for i := 0; i < 128; i++ {
		b := d.NewBatch()
		k := []byte(fmt.Sprintf("k%06d", i))
		if err := b.Set(k, value); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}
	if beforeInfo.Size() < chunkSize || beforeInfo.Size()%chunkSize != 0 {
		t.Fatalf("index.db before=%d want a positive multiple of chunk size %d", beforeInfo.Size(), chunkSize)
	}

	if err := VacuumIndexOffline(Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected command-WAL vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	verify, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open after: %v", err)
	}
	defer func() { _ = verify.Close() }()
	got, err := verify.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOffline_OuterLeavesInValueLog_PreservesLeafRefs(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	}

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	baseLeafLog := &rewriteTestLeafPageLog{db: d, dir: dir}
	if err := baseLeafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	defer func() { _ = baseLeafLog.Close() }()
	d.SetLeafPageLog(baseLeafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 24; version++ {
		b := d.NewBatch()
		for i := 0; i < 256; i++ {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	stateBefore := d.State()
	if stateBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	leafRefsBefore := collectLeafRefIDsFromRoot(t, d, stateBefore.RootPageID)
	if len(leafRefsBefore) == 0 {
		t.Fatalf("expected outer-leaf refs before offline vacuum")
	}

	indexPath := filepath.Join(dir, indexFileName)
	infoBefore, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close before vacuum: %v", err)
	}

	if err := VacuumIndexOffline(opts); err != nil {
		t.Fatalf("vacuum offline: %v", err)
	}

	infoAfter, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if infoAfter.Size() > infoBefore.Size()*4 {
		t.Fatalf("offline vacuum index inflation too large: before=%d after=%d", infoBefore.Size(), infoAfter.Size())
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after vacuum: %v", err)
	}
	defer func() { _ = reopen.Close() }()

	stateAfter := reopen.State()
	if stateAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	leafRefsAfter := collectLeafRefIDsFromRoot(t, reopen, stateAfter.RootPageID)
	if len(leafRefsAfter) == 0 {
		t.Fatalf("expected outer-leaf refs after offline vacuum")
	}
	if len(leafRefsAfter) != len(leafRefsBefore) {
		t.Fatalf("leaf-ref count changed across offline vacuum: before=%d after=%d", len(leafRefsBefore), len(leafRefsAfter))
	}

	for _, version := range []int{1, 12, 24} {
		for _, idx := range []int{0, 127, 255} {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, idx))
			got, err := reopen.Get(key)
			if err != nil {
				t.Fatalf("get version=%d key=%d after vacuum: %v", version, idx, err)
			}
			if len(got) != len(val) {
				t.Fatalf("value length mismatch version=%d key=%d: got=%d want=%d", version, idx, len(got), len(val))
			}
			if got[0] != byte(version) {
				t.Fatalf("value content mismatch version=%d key=%d: got[0]=%d want=%d", version, idx, got[0], byte(version))
			}
		}
	}
}

func TestVacuumIndexOffline_OuterLeavesInValueLog_MixedUserRootFallsBackToClone(t *testing.T) {
	dir := t.TempDir()
	baseOpts := Options{
		Dir:               dir,
		ChunkSize:         64 * 1024,
		KeepRecent:        1,
		Durability:        DurabilityWALOffRelaxed,
		PreferAppendAlloc: true,
	}

	d, err := Open(baseOpts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	value := bytes.Repeat([]byte("m"), 64)
	batch := d.NewBatch()
	for i := 0; i < 12000; i++ {
		key := []byte(fmt.Sprintf("mixed/%08d", i))
		value[0] = byte(i % 251)
		if err := batch.Set(key, value); err != nil {
			t.Fatalf("set key=%d: %v", i, err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatalf("write mixed batch: %v", err)
	}
	_ = batch.Close()

	stateBefore := d.State()
	if stateBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	rootBefore := stateBefore.RootPageID
	if rootBefore == 0 {
		t.Fatalf("expected non-zero root before vacuum")
	}
	rootPage, err := d.Pager().Get(rootBefore)
	if err != nil {
		t.Fatalf("get root page: %v", err)
	}
	if node.NewNode(rootPage).Type() != page.PageTypeInternal {
		t.Fatalf("expected internal user root before vacuum setup")
	}

	clonePath := filepath.Join(t.TempDir(), "index-clone.db")
	clonePager, err := pager.Open(clonePath, baseOpts.ChunkSize)
	if err != nil {
		t.Fatalf("open clone pager: %v", err)
	}
	defer func() { _ = clonePager.Close() }()
	if _, err := clonePager.Alloc(2); err != nil {
		t.Fatalf("alloc clone meta pages: %v", err)
	}
	cloneRoot, err := vacuumClonePagerTreeWithLeafRefs(d.Pager(), rootBefore, &pagerAllocator{p: clonePager}, clonePager, true)
	if err != nil {
		t.Fatalf("clone pager tree with base-delta option: %v", err)
	}
	if !vacuumTestPagerTreeHasInternalBaseDelta(t, clonePager, cloneRoot) {
		t.Fatalf("expected clone fallback helper to preserve IndexInternalBaseDelta encoding in cloned internal pages")
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close before vacuum: %v", err)
	}

	vacuumOpts := baseOpts
	vacuumOpts.IndexOuterLeavesInValueLog = true
	if err := VacuumIndexOffline(vacuumOpts); err != nil {
		t.Fatalf("vacuum offline with mixed root: %v", err)
	}

	reopen, err := Open(vacuumOpts)
	if err != nil {
		t.Fatalf("reopen after vacuum: %v", err)
	}
	defer func() { _ = reopen.Close() }()

	stateAfter := reopen.State()
	if stateAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	rootAfter, err := reopen.Pager().Get(stateAfter.RootPageID)
	if err != nil {
		t.Fatalf("get root after vacuum: %v", err)
	}
	rootNodeAfter := node.NewNode(rootAfter)
	if rootNodeAfter.Type() != page.PageTypeInternal {
		t.Fatalf("expected internal user root after vacuum, got %v", rootNodeAfter.Type())
	}

	for _, idx := range []int{0, 5999, 11999} {
		key := []byte(fmt.Sprintf("mixed/%08d", idx))
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get key=%d after vacuum: %v", idx, err)
		}
		if len(got) != len(value) {
			t.Fatalf("value length mismatch key=%d: got=%d want=%d", idx, len(got), len(value))
		}
		if got[0] != byte(idx%251) {
			t.Fatalf("value mismatch key=%d got[0]=%d want=%d", idx, got[0], byte(idx%251))
		}
	}
}

func vacuumTestPagerTreeHasInternalBaseDelta(t *testing.T, p *pager.Pager, rootID uint64) bool {
	t.Helper()
	seen := make(map[uint64]struct{})
	var walk func(uint64) bool
	walk = func(id uint64) bool {
		if id == 0 {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
		data, err := p.Get(id)
		if err != nil {
			t.Fatalf("get page %d: %v", id, err)
		}
		n := node.NewNode(data)
		if n.Type() != page.PageTypeInternal {
			return false
		}
		if n.InternalBaseDeltaEnabled() {
			return true
		}
		for i := uint16(0); i < n.Count(); i++ {
			childRef, err := n.GetInternalChildRef(i)
			if err != nil {
				t.Fatalf("get child ref page=%d index=%d: %v", id, i, err)
			}
			if childRef.Kind == page.ChildRefPage && walk(childRef.Page) {
				return true
			}
		}
		return false
	}
	return walk(rootID)
}

func TestVacuumIndexOffline_CrashPointsRecoverOnOpen(t *testing.T) {
	failpoints := []vacuumFailpoint{
		vacuumFailAfterNewSync,
		vacuumFailAfterReady,
		vacuumFailAfterRenameOld,
		vacuumFailAfterRenameNew,
	}

	for _, fp := range failpoints {
		fp := fp
		t.Run(string(fp), func(t *testing.T) {
			dir := t.TempDir()
			chunkSize := int64(64 * 1024)

			d, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("set: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			err = vacuumIndexOffline(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1}, fp)
			if err == nil {
				t.Fatalf("expected failpoint error")
			}
			if !errors.Is(err, errVacuumFailpoint) {
				t.Fatalf("unexpected error: %v", err)
			}

			reopen, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopen.Close() }()

			val, err := reopen.Get([]byte("k"))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if string(val) != "v" {
				t.Fatalf("bad value: %q", val)
			}
		})
	}
}

func TestResetLeafGenerationAfterOfflineVacuum_RemovesStaleFilesAfterManifest(t *testing.T) {
	dir := t.TempDir()
	leafDir := LeafLogDirPath(dir)
	if err := os.MkdirAll(leafDir, 0o700); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}
	rawFileID, err := valuelog.EncodeSegmentID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("encode segment id: %v", err)
	}
	segmentPath := leafGenerationFallbackPath(dir, rawFileID)
	if err := os.WriteFile(segmentPath, []byte("stale leaf segment"), 0o600); err != nil {
		t.Fatalf("write stale segment: %v", err)
	}
	indexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID)
	if err := os.WriteFile(indexPath, []byte("stale index"), 0o600); err != nil {
		t.Fatalf("write stale length index: %v", err)
	}
	manifest := newLeafGenerationManifest(41)
	changed, err := manifest.registerCurrentGenerationFileID(rawFileID, 41)
	if err != nil {
		t.Fatalf("register stale file id: %v", err)
	}
	if !changed {
		t.Fatal("expected manifest to record stale file id")
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("save stale manifest: %v", err)
	}

	if err := resetLeafGenerationAfterOfflineVacuum(dir, 42, nil); err != nil {
		t.Fatalf("reset leaf generation: %v", err)
	}
	if _, err := os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("stale segment still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("stale length index still exists or stat failed: %v", err)
	}
	reset, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("load reset manifest: %v", err)
	}
	if !ok {
		t.Fatal("expected reset manifest")
	}
	if len(reset.Generations) != 1 || len(reset.Generations[0].FileIDs) != 0 || reset.Generations[0].CreatedCommitSeq != 42 {
		t.Fatalf("unexpected reset manifest: %+v", reset)
	}
}

func TestResetLeafGenerationAfterOfflineVacuum_WritesResetManifestBeforeDeletion(t *testing.T) {
	dir := t.TempDir()
	leafDir := LeafLogDirPath(dir)
	if err := os.MkdirAll(leafDir, 0o700); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}
	rawFileID, err := valuelog.EncodeSegmentID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("encode segment id: %v", err)
	}
	segmentPath := leafGenerationFallbackPath(dir, rawFileID)
	if err := os.WriteFile(segmentPath, []byte("stale leaf segment"), 0o600); err != nil {
		t.Fatalf("write stale segment: %v", err)
	}
	manifest := newLeafGenerationManifest(41)
	changed, err := manifest.registerCurrentGenerationFileID(rawFileID, 41)
	if err != nil {
		t.Fatalf("register stale file id: %v", err)
	}
	if !changed {
		t.Fatal("expected manifest to record stale file id")
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("save stale manifest: %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	syncDirFn = func(_ string) error {
		return errors.New("stop after reset manifest")
	}
	if err := resetLeafGenerationAfterOfflineVacuum(dir, 42, nil); err == nil {
		t.Fatal("expected injected sync error")
	}
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("expected stale segment to remain after injected sync error: %v", err)
	}
	reset, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("load reset manifest: %v", err)
	}
	if !ok {
		t.Fatal("expected reset manifest")
	}
	if len(reset.Generations) != 1 || len(reset.Generations[0].FileIDs) != 0 || reset.Generations[0].CreatedCommitSeq != 42 {
		t.Fatalf("unexpected reset manifest after injected sync error: %+v", reset)
	}
}

func TestOpen_IndexOuterLeavesInValueLog_RecoversPendingOfflineVacuumReset(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	leafDir := LeafLogDirPath(dir)
	rawFileID, err := valuelog.EncodeSegmentID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("encode segment id: %v", err)
	}
	segmentPath := leafGenerationFallbackPath(dir, rawFileID)
	if err := os.WriteFile(segmentPath, []byte("stale leaf segment"), 0o600); err != nil {
		t.Fatalf("write stale segment: %v", err)
	}
	indexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID)
	if err := os.WriteFile(indexPath, []byte("stale index"), 0o600); err != nil {
		t.Fatalf("write stale length index: %v", err)
	}
	manifest := newLeafGenerationManifest(1)
	changed, err := manifest.registerCurrentGenerationFileID(rawFileID, 1)
	if err != nil {
		t.Fatalf("register stale file id: %v", err)
	}
	if !changed {
		t.Fatal("expected manifest to record stale file id")
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("save stale manifest: %v", err)
	}
	if err := writeLeafGenerationResetPendingAfterOfflineVacuum(dir); err != nil {
		t.Fatalf("write reset marker: %v", err)
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}
	if _, err := os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("stale segment still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("stale length index still exists or stat failed: %v", err)
	}
	reset, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("load reset manifest: %v", err)
	}
	if !ok {
		t.Fatal("expected reset manifest")
	}
	if len(reset.Generations) != 1 || len(reset.Generations[0].FileIDs) != 0 {
		t.Fatalf("unexpected reset manifest: %+v", reset)
	}
	if _, err := os.Stat(leafGenerationResetPendingAfterOfflineVacuumPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("reset marker still exists or stat failed: %v", err)
	}
}
