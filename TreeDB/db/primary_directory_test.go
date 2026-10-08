package db

import (
	"bytes"
	"context"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
)

func primaryDBDirectory(t *testing.T, db *DB) node.PrimaryDirectoryView {
	t.Helper()
	image, err := db.idx.Load().pager.Get(db.meta.UserRootPageID)
	if err != nil {
		t.Fatal(err)
	}
	d, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestPrimaryDirectoryOrdinarySnapshotCoalesceReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, key := range []string{"", "a", "b", "c"} {
		if err = db.SetSync([]byte(key), []byte("old-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("initial directory count %d", primaryDBDirectory(t, db).Count())
	old := db.AcquireSnapshot()
	defer old.Close()
	for i := 0; i < 6; i++ {
		if err = db.SetSync([]byte("a"), []byte(fmt.Sprintf("new-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.DeleteSync([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err = db.SetSync(nil, nil); err != nil {
		t.Fatal(err)
	}
	d := primaryDBDirectory(t, db)
	if d.Count() != 4 {
		t.Fatalf("coalesced entries=%d", d.Count())
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "old-a" {
		t.Fatalf("old snapshot %q %v", got, err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "new-5" {
		t.Fatalf("current %q %v", got, err)
	}
	if got, err := db.Get(nil); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty key/value %#v %v", got, err)
	}
	if has, err := db.Has([]byte("b")); err != nil || has {
		t.Fatalf("delete %v %v", has, err)
	}
	values, err := db.GetMany([][]byte{nil, []byte("b"), []byte("a")})
	if err != nil || values[0] == nil || values[1] != nil || string(values[2]) != "new-5" {
		t.Fatalf("GetMany %#v %v", values, err)
	}
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err = it.Error(); err != nil {
		t.Fatal(err)
	}
	_ = it.Close()
	if fmt.Sprint(keys) != "[ a c]" {
		t.Fatalf("keys %q", keys)
	}
	reverse, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	keys = nil
	for ; reverse.Valid(); reverse.Next() {
		keys = append(keys, string(reverse.Key()))
	}
	if err = reverse.Error(); err != nil {
		t.Fatal(err)
	}
	_ = reverse.Close()
	if fmt.Sprint(keys) != "[c a ]" {
		t.Fatalf("reverse keys %q", keys)
	}
	_ = old.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "new-5" {
		t.Fatalf("reopen %q %v", got, err)
	}
	if err = db.SetSync([]byte("a"), []byte("after-reopen")); err != nil {
		t.Fatal(err)
	}
	if primaryDBDirectory(t, db).Count() != 4 {
		t.Fatal("reopen lost complete directory")
	}
}
func TestPrimaryDirectoryOrdinaryCapacityAndDeferredPhysicalExport(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < node.PrimaryDirectoryMaxEntries+5; i++ {
		if err = db.SetSync([]byte(fmt.Sprintf("k%03d", i)), []byte("initial")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	image, e := db.idx.Load().pager.Get(db.meta.UserRootPageID)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("last root type %v", node.NewNode(image).Type())
	d := primaryDBDirectory(t, db)
	_, baseSeq := d.Base()
	if baseSeq <= 1 {
		t.Fatal("capacity did not materialize base")
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cut, err := db.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	for i := 0; i < 6; i++ {
		if err = db.SetSync([]byte("k000"), []byte(fmt.Sprintf("new-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	var exported, primaryExported bytes.Buffer
	if err = cut.WriteToContext(context.Background(), &exported); err != nil {
		t.Fatal(err)
	}
	if cut.PrimarySizeBytes() == 0 {
		t.Fatal("missing primary projection")
	}
	if err = cut.WritePrimaryToContext(context.Background(), &primaryExported); err != nil {
		t.Fatal(err)
	}
	restore := t.TempDir()
	if err = os.WriteFile(filepath.Join(restore, "index.db"), exported.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(restore, primaryIndexFileName), primaryExported.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: restore})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("k000")); err != nil || string(got) != "initial" {
		t.Fatalf("deferred captured value %q %v", got, err)
	}
	if primaryDBDirectory(t, reopened).Count() > node.PrimaryDirectoryMaxEntries {
		t.Fatal("capacity exceeded")
	}
}

func TestPrimaryDirectoryPhysicalBaseValueLogRetention(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	walDir := ValueLogDirPath(dir)
	if err = os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(walDir, "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	oldValue := bytes.Repeat([]byte("old-value"), 128)
	ptr, err := writer.Append(0, nil, 1, oldValue)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)
	b := database.NewBatch()
	pb, ok := b.(interface {
		SetPointer([]byte, page.ValuePtr) error
	})
	if !ok {
		t.Fatal("pointer batch")
	}
	if err = pb.SetPointer([]byte("p"), ptr); err != nil {
		t.Fatal(err)
	}
	// The large ordinary batch genuinely materializes an empty-directory base.
	for i := 0; i < node.PrimaryDirectoryMaxEntries+5; i++ {
		if err = b.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if primaryDBDirectory(t, database).Count() != 0 {
		t.Fatal("expected materialized base")
	}
	if err = database.SetSync([]byte("p"), []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	counts, _, err := database.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts[fileID] != 1 {
		t.Fatalf("physically retained base pointer count=%d", counts[fileID])
	}
	if _, err = database.FragmentationReport(); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("reachable base segment removed: %v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if value, err := database.Get([]byte("p")); err != nil || string(value) != "replacement" {
		t.Fatalf("reopen %q %v", value, err)
	}
	counts, _, err = database.scanValueLogRefCounts(context.Background())
	if err != nil || counts[fileID] != 1 {
		t.Fatalf("reopen base dependencies %#v %v", counts, err)
	}
}
func TestPrimaryDirectoryLargeKeyFallbackAndEmptyBound(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, key := range [][]byte{nil, []byte("a"), bytes.Repeat([]byte("z"), 3920)} {
		if err = database.SetSync(key, []byte("v")); err != nil {
			t.Fatalf("key length %d: %v", len(key), err)
		}
		if value, err := database.Get(key); err != nil || string(value) != "v" {
			t.Fatalf("key length %d: %q %v", len(key), value, err)
		}
	}
	it, err := database.Iterator(nil, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if it.Valid() || it.Error() != nil {
		t.Fatalf("empty exclusive bound valid=%v error=%v", it.Valid(), it.Error())
	}
}

// Corrupting the newest physical record must leave the other real DB slot
// independently complete; recovery cannot borrow its winner from current head.
func TestPrimaryDirectoryRealSlotsCorruptionFallback(t *testing.T) {
	dir := t.TempDir()
	database, e := Open(Options{Dir: dir, IndexPrimaryDirectory: true})
	if e != nil {
		t.Fatal(e)
	}
	if e = database.SetSync([]byte("key"), []byte("older")); e != nil {
		t.Fatal(e)
	}
	if e = database.SetSync([]byte("key"), []byte("newest")); e != nil {
		t.Fatal(e)
	}
	newest := database.durableRoot.meta.RootRecordPageID
	olderSeq := database.durableRoot.slotRecord[database.durableRoot.slot^1].CommitSeq
	if olderSeq == 0 {
		t.Fatal("missing independent older slot")
	}
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	file, e := os.OpenFile(filepath.Join(dir, primaryIndexFileName), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	offset := int64(newest&^page.PrimaryBankNamespace)*page.PageSize + 128
	var value [1]byte
	if _, e = file.ReadAt(value[:], offset); e != nil {
		t.Fatal(e)
	}
	value[0] ^= 0x80
	if _, e = file.WriteAt(value[:], offset); e != nil {
		t.Fatal(e)
	}
	if e = file.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = file.Close(); e != nil {
		t.Fatal(e)
	}
	database, e = Open(Options{Dir: dir})
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	if database.meta.CommitSeq != olderSeq {
		t.Fatalf("recovered sequence %d, expected %d", database.meta.CommitSeq, olderSeq)
	}
	if got, e := database.Get([]byte("key")); e != nil || string(got) != "older" {
		t.Fatalf("older independent root %q %v", got, e)
	}
	if e = database.SetSync([]byte("key"), []byte("after-fallback")); e != nil {
		t.Fatal(e)
	}
}

func TestPrimaryDirectoryRequiredDependencyEmptyNoopReopen(t *testing.T) {
	dir := t.TempDir()
	if e := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); e != nil {
		t.Fatal(e)
	}
	database, e := Open(Options{Dir: dir, IndexPrimaryDirectory: true})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	first := database.durableRoot.record.Directory
	if first.RootPageID&page.PrimaryBankNamespace == 0 {
		t.Fatalf("dependency outside bank namespace: %+v", first)
	}
	for _, key := range []string{"a", "b", "c"} {
		if e = database.SetSync([]byte(key), []byte(key)); e != nil {
			t.Fatal(e)
		}
	}
	if database.durableRoot.record.Directory != first {
		t.Fatal("empty dependency no-op rebuilt physical root")
	}
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	database = nil
	database, e = Open(Options{Dir: dir})
	if e != nil {
		t.Fatal(e)
	}
	if e = database.SetSync([]byte("after"), []byte("reopen")); e != nil {
		t.Fatal(e)
	}
	if database.durableRoot.record.Directory != first {
		t.Fatal("reopen changed unchanged bank metadata")
	}
}
