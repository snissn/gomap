package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

	if err := resetLeafGenerationAfterOfflineVacuum(dir, 42); err != nil {
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
	if err := resetLeafGenerationAfterOfflineVacuum(dir, 42); err == nil {
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
