package collections

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Test-only observation delegates every read to the actual original capture.
// Private mutexes and charges are checked in caching's matching cold-bootstrap
// and held-physicalMu tests, without a shipping test API or mutable global hook.
type nativeBlockValueDelegatingCapture struct {
	backenddb.NativeSemanticCapture
	calls  int
	shapes []valuelog.COWRecordShape
}

func (c *nativeBlockValueDelegatingCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}
func (c *nativeBlockValueDelegatingCapture) NativeValueAtPointer(ptr page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	f, _, err := c.Snapshot().PinnedValueLogFile(ptr.FileID)
	if err != nil {
		return nil, err
	}
	limit := c.NativeAllocator().Limits().MaxInFlightBytes
	shape, err := valuelog.InspectCOWRecord(f, ptr, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return nil, err
	}
	c.calls++
	c.shapes = append(c.shapes, shape)
	return c.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, admit)
}

func TestNativeJSONBlockValueDefaultActivation(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
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
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(7)}, {Key: "bin", Value: bson.Binary{Subtype: 0x80, Data: bytes.Repeat([]byte("default-native-block-value|"), 2048)}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("value")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			read, err := manager.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := read.Close(); err != nil {
					t.Error(err)
				}
			})
			observed := &nativeBlockValueDelegatingCapture{NativeSemanticCapture: read.capture}
			read.capture = observed
			// No private workspace growth, codec preparation or prior public value
			// read. Native entry traversal may warm leaf scratch separately.
			for i := range 2 {
				got, err := read.Get([]byte("value"))
				if err != nil || !bytes.Equal(got, doc) || len(got) != cap(got) {
					t.Fatalf("public Get%d: len=%d/%v", i, len(got), err)
				}
				got[0] ^= 255
			}
			if observed.calls != 2 {
				t.Fatalf("public value delegation count=%d, expected actual pointer route twice", observed.calls)
			}
			for _, shape := range observed.shapes {
				if shape.DictID != 0 || (shape.Compressed && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4) {
					t.Fatalf("default eligible shape missing: %+v", shape)
				}
				t.Logf("actual default public pointer shape: %+v (raw=%t)", shape, !shape.Compressed)
			}
			if got, err := manager.nativeJSON.Get("docs", []byte("value")); err != nil || !bytes.Equal(got, doc) {
				t.Fatal("manager Get", err)
			}
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := fresh.Close(); err != nil {
					t.Error(err)
				}
			})
			freshBackend, err := fresh.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			freshManager := NewCollectionManager(freshBackend)
			t.Cleanup(func() {
				if err := freshManager.Close(); err != nil {
					t.Error(err)
				}
			})
			freshCol, err := freshManager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := freshCol.Get([]byte("value")); err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("fresh public Get: len=%d err=%v", len(got), err)
			}
			if err := freshManager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fresh.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
