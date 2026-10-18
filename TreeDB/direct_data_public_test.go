package treedb_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	tree "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func directDATAFixtureDir(t *testing.T, name string) string {
	t.Helper()
	if root := os.Getenv("GOMAP_P5_RETAIN_FIXTURE_ROOT"); root != "" {
		dir := filepath.Join(root, t.Name(), name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	return t.TempDir()
}

func directDATAOptions(dir string) tree.Options {
	opts := tree.OptionsFor(tree.ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.ValueLog.PointerThreshold = 128
	opts.ValueLog.ForcePointers = false
	return opts
}

func directDATAReadOnlyOptions(opts tree.Options) tree.Options {
	// The public read-only role has no cache and rejects cow_btree itself.
	opts.ReadOnly = true
	opts.MemtableMode = "btree"
	return opts
}

type directDATASlot struct {
	meta   page.DurableMetaV1
	record rootpublication.DurableRootRecordV1
}

func directDATAReadSlots(t *testing.T, dir string) [2]directDATASlot {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, "maindb", "index.db.primary")); !os.IsNotExist(err) {
		t.Fatalf("selected DATA route has PRIMARY file: %v", err)
	}
	f, err := os.Open(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var slots [2]directDATASlot
	for i := range slots {
		image := make([]byte, page.PageSize)
		if _, err := f.ReadAt(image, int64(i*page.PageSize)); err != nil {
			t.Fatal(err)
		}
		if !page.VerifyChecksumNonMutating(image) {
			t.Fatalf("DATA meta slot %d checksum", i)
		}
		meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
		if err != nil {
			t.Fatalf("DATA meta slot %d: %v", i, err)
		}
		if _, err := f.ReadAt(image, int64(meta.RootRecordPageID)*int64(page.PageSize)); err != nil {
			t.Fatal(err)
		}
		record, err := rootpublication.DecodeDurableRootRecordV1(image, meta.RootRecordPageID, meta.RootRecordDigest)
		if err != nil {
			t.Fatalf("DATA record slot %d: %v", i, err)
		}
		if record.CommitSeq != meta.CommitSeq || record.UserRootPageID < 2 || record.UserRootPageID >= record.TotalPages || primaryarena.IsPage(record.UserRootPageID) {
			t.Fatalf("not a bound DATA root: meta=%+v record=%+v", meta, record)
		}
		if record.ParentRecordPageID != 0 {
			if _, err := f.ReadAt(image, int64(record.ParentRecordPageID)*int64(page.PageSize)); err != nil {
				t.Fatal(err)
			}
			parent, err := rootpublication.DecodeDurableRootRecordV1(image, record.ParentRecordPageID, record.ParentRecordDigest)
			if err != nil || parent.CommitSeq != record.ParentCommitSeq || parent.DurableSeq+1 != record.DurableSeq {
				t.Fatalf("exact DATA parent slot %d: parent=%+v error=%v record=%+v", i, parent, err, record)
			}
		}
		slots[i] = directDATASlot{meta: meta, record: record}
	}
	return slots
}

func directDATAValues(t *testing.T, get func([]byte) ([]byte, error), expected map[string][]byte) {
	t.Helper()
	for _, key := range []string{"a", "b", "c", "d"} {
		got, err := get([]byte(key))
		want, exists := expected[key]
		if !exists {
			if got != nil || (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) {
				t.Fatalf("absent %s: %q %v", key, got, err)
			}
		} else if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: length=%d error=%v want length=%d", key, len(got), err, len(want))
		}
	}
}

type directDATAEpochs struct {
	database                *tree.DB
	opts                    tree.Options
	held                    tree.Snapshot
	first, second           map[string][]byte
	firstSlots, secondSlots [2]directDATASlot
	authority               [2][]durabilitycut.Event
}

func directDATAObserveCheckpoint(t *testing.T, database *tree.DB, dir string) []durabilitycut.Event {
	t.Helper()
	var mu sync.Mutex
	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == filepath.Join(dir, "maindb") && event.Resource == durabilitycut.ResourceMeta &&
			(event.Point == durabilitycut.BeforeMetaWrite || event.Point == durabilitycut.AfterMetaWrite || event.Point == durabilitycut.BeforeMetaSync || event.Point == durabilitycut.AfterMetaSync) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		}
		return nil
	})
	defer restore()
	if err := database.Checkpoint(); err != nil {
		t.Fatal("explicit checkpoint", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]durabilitycut.Event(nil), events...)
}

func directDATAWriteEpochs(t *testing.T, requireDATA bool) *directDATAEpochs {
	t.Helper()
	opts := directDATAOptions(directDATAFixtureDir(t, "data"))
	database, err := tree.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if database != nil {
			_ = database.Close()
		}
	})
	stats := database.Stats()
	t.Logf("actual profile=%s mode=%s redo=%s root=%s", stats["treedb.profile.resolved"], stats["treedb.cache.memtable_mode"], stats["treedb.write_path.redo_log"], stats["treedb.durable_root.user_root_page"])
	if stats["treedb.profile.resolved"] != string(tree.ProfileNoWALFast) || stats["treedb.cache.memtable_mode"] != "cow_btree" || stats["treedb.write_path.redo_log"] != "off" {
		t.Fatalf("fixture profile activation: %+v", stats)
	}
	if requireDATA {
		if _, err := os.Stat(filepath.Join(opts.Dir, "maindb", "index.db.primary")); !os.IsNotExist(err) {
			t.Fatalf("NoWAL public Open selected PRIMARY; DATA required: %v", err)
		}
	}
	e := &directDATAEpochs{database: database, opts: opts, first: map[string][]byte{"a": []byte("old-inline"), "b": bytes.Repeat([]byte("old-pointer/"), 64), "c": []byte("delete-later")}, second: map[string][]byte{"a": []byte("new-inline"), "b": bytes.Repeat([]byte("new-pointer/"), 64), "d": []byte("inserted")}}
	for _, key := range []string{"a", "b", "c"} {
		if err := database.Set([]byte(key), e.first[key]); err != nil {
			t.Fatal(err)
		}
	}
	e.authority[0] = directDATAObserveCheckpoint(t, database, opts.Dir)
	e.held = database.AcquireSnapshot()
	if e.held == nil {
		t.Fatal("first snapshot unavailable")
	}
	t.Cleanup(func() { _ = e.held.Close() })
	entry, err := e.held.GetEntryExact([]byte("b"))
	if err != nil || entry.ValuePtr.FileID == 0 {
		t.Fatalf("actual persistent pointer evidence: %+v %v", entry, err)
	}
	directDATAValues(t, e.held.Get, e.first)
	if requireDATA {
		e.firstSlots = directDATAReadSlots(t, opts.Dir)
	}
	for _, key := range []string{"a", "b", "d"} {
		if err := database.Set([]byte(key), e.second[key]); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	e.authority[1] = directDATAObserveCheckpoint(t, database, opts.Dir)
	directDATAValues(t, e.held.Get, e.first)
	directDATAValues(t, database.Get, e.second)
	if requireDATA {
		e.secondSlots = directDATAReadSlots(t, opts.Dir)
	}
	for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"} {
		if got := database.Stats()[key]; got != "0" {
			t.Fatalf("NoWAL volatile ordinary ACK unexpectedly used WAL: %s=%q", key, got)
		}
	}
	return e
}

func TestNoWALActuallyUsesDATASealAndRecovery(t *testing.T) {
	e := directDATAWriteEpochs(t, true)
	var previous uint64
	for epoch, events := range e.authority {
		want := []durabilitycut.Point{durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync}
		if len(events) != len(want) {
			t.Fatalf("explicit checkpoint %d authority: %+v", epoch+1, events)
		}
		for i, event := range events {
			if event.Point != want[i] || event.Path != filepath.Join(e.opts.Dir, "maindb", "index.db") {
				t.Fatalf("checkpoint %d not DATA authority: %+v", epoch+1, event)
			}
			if i < 2 && ((event.Offset != 0 && event.Offset != int64(page.PageSize)) || event.Length != int64(page.PageSize)) {
				t.Fatalf("checkpoint %d not one DATA meta page: %+v", epoch+1, event)
			}
		}
		slots := e.firstSlots
		if epoch == 1 {
			slots = e.secondSlots
		}
		selected := slots[events[0].Offset/int64(page.PageSize)]
		if selected.meta.CommitSeq <= previous || selected.record.CommitSeq != selected.meta.CommitSeq {
			t.Fatalf("checkpoint %d authority not bound to advancing record: %+v", epoch+1, selected)
		}
		previous = selected.meta.CommitSeq
		t.Logf("explicit checkpoint %d DATA slot=%d commit=%d recordPage=%d digest=%x", epoch+1, events[0].Offset/int64(page.PageSize), selected.meta.CommitSeq, selected.meta.RootRecordPageID, selected.meta.RootRecordDigest)
	}
	if err := e.held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.database.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := tree.Open(directDATAReadOnlyOptions(e.opts))
	if err != nil {
		t.Fatal("public RO DATA reopen", err)
	}
	defer ro.Close()
	directDATAValues(t, ro.Get, e.second)
	directDATAReadSlots(t, e.opts.Dir)
}

func TestDirectDATAOldViewSurvivesFallbackRecovery(t *testing.T) {
	e := directDATAWriteEpochs(t, true)
	if err := e.held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.database.Close(); err != nil {
		t.Fatal(err)
	}
	slots := directDATAReadSlots(t, e.opts.Dir)
	newest := 0
	if slots[1].meta.CommitSeq > slots[0].meta.CommitSeq {
		newest = 1
	}
	older := 1 - newest
	if slots[older].meta.CommitSeq == 0 || slots[older].meta.CommitSeq >= slots[newest].meta.CommitSeq {
		t.Fatalf("not two selectable DATA roots: %+v", slots)
	}
	if slots != e.secondSlots {
		t.Fatal("Close unexpectedly changed DATA slot evidence; inspect before assigning fallback map")
	}
	ro, err := tree.Open(directDATAReadOnlyOptions(e.opts))
	if err != nil {
		t.Fatal(err)
	}
	directDATAValues(t, ro.Get, e.second)
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.opts.Dir, "maindb", "index.db")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	offset := int64(newest)*int64(page.PageSize) + int64(page.PageHeaderSize)
	if _, err := f.ReadAt(b[:], offset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 1
	if _, err := f.WriteAt(b[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err = tree.Open(directDATAReadOnlyOptions(e.opts))
	if err != nil {
		t.Fatal("public DATA fallback reopen", err)
	}
	defer ro.Close()
	if got := ro.Stats()["treedb.durable_root.commit_seq"]; got != strconv.FormatUint(slots[older].meta.CommitSeq, 10) {
		t.Fatalf("fallback=%s want exact older commit=%d", got, slots[older].meta.CommitSeq)
	}
	directDATAValues(t, ro.Get, e.first)
}

// The archive was generated by the genuine pinned original public producer.
// Its source, toolchain, binary and per-file hashes are in the adjacent JSON.
func directDATAExtractPRIMARYFixture(t *testing.T, dir string) {
	t.Helper()
	data, err := os.ReadFile("testdata/p5-primary-7385.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != "b4589b1436cc961a18159c3cd84cb3e8f06af4a224d6362cd12e040e229e6c8e" {
		t.Fatalf("original PRIMARY fixture archive hash: %s", got)
	}
	z, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	archive := tar.NewReader(z)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || !filepath.IsLocal(header.Name) {
			t.Fatalf("fixture entry %q type=%d", header.Name, header.Typeflag)
		}
		path := filepath.Join(dir, header.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode))
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(f, archive)
		closeErr := f.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			t.Fatal(err)
		}
	}
}

type directDATAFixtureFile struct {
	fingerprint string
	identity    os.FileInfo
}

func directDATAFixtureHashes(t *testing.T, dir string) map[string]directDATAFixtureFile {
	t.Helper()
	files := make(map[string]directDATAFixtureFile)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files[rel] = directDATAFixtureFile{fingerprint: fmt.Sprintf("%s/%d/%x", info.Mode(), info.Size(), sha256.Sum256(data)), identity: info}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestDirectDATAPersistedPRIMARYRequiresRebuild(t *testing.T) {
	dir := directDATAFixtureDir(t, "original-primary")
	directDATAExtractPRIMARYFixture(t, dir)
	before := directDATAFixtureHashes(t, dir)
	for _, readOnly := range []bool{false, true} {
		opts := directDATAOptions(dir)
		if readOnly {
			opts = directDATAReadOnlyOptions(opts)
		}
		database, err := tree.Open(opts)
		if database != nil {
			_ = database.Close()
		}
		if !errors.Is(err, backenddb.ErrLegacyFormatRebuildRequired) || !strings.Contains(strings.ToLower(err.Error()), "rebuild") {
			t.Fatalf("PRIMARY readOnly=%v: DB=%v error=%v; want explicit pre-alpha rebuild refusal", readOnly, database != nil, err)
		}
		after := directDATAFixtureHashes(t, dir)
		if len(after) != len(before) {
			t.Fatalf("PRIMARY refusal changed file inventory readOnly=%v", readOnly)
		}
		for name, original := range before {
			current, ok := after[name]
			if !ok || current.fingerprint != original.fingerprint || !os.SameFile(original.identity, current.identity) {
				t.Fatalf("PRIMARY refusal mutated identity/content of %s readOnly=%v", name, readOnly)
			}
		}
	}
	retiredDir := filepath.Join(t.TempDir(), "uncreated")
	retired := directDATAOptions(retiredDir)
	retired.IndexPrimaryDirectory = true
	if database, err := tree.Open(retired); database != nil || !errors.Is(err, backenddb.ErrLegacyFormatRebuildRequired) {
		t.Fatalf("retired PRIMARY request: database=%v err=%v", database != nil, err)
	}
	if _, err := os.Stat(retiredDir); !os.IsNotExist(err) {
		t.Fatalf("retired request created directory: %v", err)
	}
	opts := directDATAOptions(directDATAFixtureDir(t, "rebuilt"))
	database, err := tree.Open(opts)
	if err != nil {
		t.Fatal("fresh rebuilt DATA directory", err)
	}
	if err := database.Set([]byte("rebuilt"), []byte("new-data")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get([]byte("rebuilt")); err != nil || string(got) != "new-data" {
		t.Fatalf("rebuilt DATA: %q %v", got, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
