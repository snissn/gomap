package caching

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCOWCoalescedBoundarySurvivesWritesBeforeKernelCapture(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	write := func(value string) {
		t.Helper()
		b := cache.NewBatch()
		defer b.Close()
		if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
			t.Fatal(err)
		}
		if err := b.Set([]byte("key"), []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
			lsn, err := backend.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
			if receiptErr := b.RecordCOWAppend(cache.cow.txnOwner, lsn, err, false); err == nil {
				err = receiptErr
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4097; i++ {
		write("prefix")
	}
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing retained opening")
	}
	defer held.Close()
	prefix := cache.cow.prefix.LogicalFrontiers()
	cache.testAfterCheckpointFrontierCapture = func() {
		// The original writer fence has ended, but CaptureAcceptedCut has not run.
		for i := 0; i < 4097; i++ {
			write("suffix")
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cache.testAfterCheckpointFrontierCapture = nil
	disk := backend.AcquireSnapshot()
	if disk == nil {
		t.Fatal("missing physical prefix")
	}
	token, ok := disk.StateToken()
	if !ok || token.AppliedCommandLSN != prefix.C {
		t.Fatalf("physical exact cut %+v prefix%+v", token, prefix)
	}
	if value, err := disk.Get([]byte("key")); err != nil || string(value) != "prefix" {
		t.Fatalf("physical suffix imported %q: %v", value, err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if value, err := held.Get([]byte("key")); err != nil || string(value) != "prefix" {
		t.Fatalf("held prefix changed %q: %v", value, err)
	}
	if value, err := cache.Get([]byte("key")); err != nil || string(value) != "suffix" {
		t.Fatalf("latest suffix %q: %v", value, err)
	}
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.CanonicalLSN != prefix.C || got.C != prefix.C+4097 {
		t.Fatalf("frontiers%+v prefix%+v", got, prefix)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if cache.cow.budget.Stats().TotalBytes != 0 {
		t.Fatal("final charge retained")
	}
}

func TestCOWNativeBSONBoundarySurvivesWritesBeforeKernelCapture(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	reader, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not select native collection publication")
	}
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("id")
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "id"}, {Key: "n", Value: int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err = cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	write := func(n int64) {
		t.Helper()
		typ, value, err := bson.MarshalValue(n)
		if err != nil {
			t.Fatal(err)
		}
		matched, modified, err := col.UpdateBSONSet(id, []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: value}}})
		if err != nil || !matched || !modified {
			t.Fatalf("BSON update %d: matched=%t modified=%t %v", n, matched, modified, err)
		}
	}
	check := func(doc []byte, err error, want int64) {
		t.Helper()
		if err != nil || bson.Raw(doc).Lookup("n").Int64() != want {
			t.Fatalf("BSON value=%v error=%v want n=%d", bson.Raw(doc), err, want)
		}
	}
	write(1)
	view, err := reader.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	write(2)
	prefix := cache.cow.prefix.LogicalFrontiers()
	cache.testAfterCheckpointFrontierCapture = func() {
		write(3)
		write(4)
		if count := cache.cow.prefix.AcceptedPendingCount(); count != 2 {
			t.Errorf("marked old and new native coverage ran together: slots=%d", count)
		}
	}
	err = cache.Checkpoint()
	cache.testAfterCheckpointFrontierCapture = nil
	if err != nil {
		t.Fatal(err)
	}
	disk := backend.AcquireSnapshot()
	if disk == nil {
		t.Fatal("missing physical native prefix")
	}
	defer disk.Close()
	token, ok := disk.StateToken()
	if !ok || token.AppliedCommandLSN != prefix.C {
		t.Fatalf("physical exact cut %+v prefix=%+v", token, prefix)
	}
	root, err := disk.GetAtRoot(token.SystemRootPageID, []byte("collections/root/docs/primary"))
	if err != nil || len(root) != 8 {
		t.Fatalf("physical primary root %x: %v", root, err)
	}
	doc, err = disk.GetAtRoot(binary.BigEndian.Uint64(root), id)
	check(doc, err, 2)
	doc, err = view.Get(id)
	check(doc, err, 1)
	doc, err = col.Get(id)
	check(doc, err, 4)
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.LogicalSequence != prefix.V || got.S.CanonicalLSN != prefix.C || got.C != prefix.C+2 {
		t.Fatalf("frontiers=%+v prefix=%+v", got, prefix)
	}
	if err = disk.Close(); err != nil {
		t.Fatal(err)
	}
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	if err = cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = cache.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
		t.Fatalf("checked close retained resources: %+v", stats)
	}
}
