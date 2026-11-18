package collections

import (
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONReturnsActualCollectionCustodyBeforeSeal(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("id")
	image := func(n int64) ([]byte, error) {
		return bson.Marshal(bson.D{{Key: "_id", Value: "id"}, {Key: "n", Value: n}})
	}
	doc, err := image(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	base := backend.State().AppliedCommandLSN
	captureCollection := func() (backenddb.NativeSemanticCapture, *nativeCollectionState) {
		t.Helper()
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := capture.Close(); err != nil {
				t.Error(err)
			}
		})
		engine, ok := capture.State().(*nativeEngineVersion)
		if !ok {
			t.Fatal("missing complete native root set")
		}
		state, ok := engine.names.Get("docs")
		if !ok {
			t.Fatal("missing collection state in native root set")
		}
		return capture, state
	}
	typ, value, err := bson.MarshalValue(int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: value}}}); err != nil || !matched || !modified {
		t.Fatalf("structured BSON update: matched=%t modified=%t err=%v", matched, modified, err)
	}
	old, covered := captureCollection()
	if matched, modified, err := col.Update(id, func([]byte) ([]byte, bool, error) {
		value, err := image(2)
		return value, true, err
	}); err != nil || !matched || !modified {
		t.Fatalf("generic BSON update: matched=%t modified=%t err=%v", matched, modified, err)
	}
	latest, fallback := captureCollection()
	if covered == fallback || latest.LogicalSequence() != old.LogicalSequence()+1 {
		t.Fatal("missing distinct retained collection generations")
	}
	// Both generations retain the same directory and preparation edges. The
	// generic update retains the extra original custody edge until a seal; the
	// actual structured update's worker releases exactly that edge before S.
	warm := time.NewTicker(time.Millisecond)
	defer warm.Stop()
	deadline := time.After(5 * time.Second)
	for covered.refs.Load()+1 != fallback.refs.Load() {
		select {
		case <-deadline:
			t.Fatalf("root-covered refs=%d generic refs=%d", covered.refs.Load(), fallback.refs.Load())
		case <-warm.C:
		}
	}
	if got := backend.State().AppliedCommandLSN; got != base {
		t.Fatalf("covered custody release relied on a physical seal: applied=%d base=%d", got, base)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := backend.State().AppliedCommandLSN; got != base+2 {
		t.Fatalf("physical seal omitted a command: applied=%d base=%d", got, base)
	}
}
