package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func encodeOuterLeafBlockForTest(t *testing.T, start, count int, value []byte) []byte {
	t.Helper()
	entries := make([]outerleaf.TypedEntry, 0, count)
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("k%08d", start+i))
		v := append([]byte(nil), value...)
		entries = append(entries, outerleaf.TypedEntry{
			Key:   k,
			Kind:  outerleaf.EntryKindInline,
			Value: v,
		})
	}
	payload, err := outerleaf.EncodeTypedEntries(nil, entries, 0, 16)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	return payload
}

// Regression: when leaves are sparse fence anchors, parent separators must use
// the full right-start key. Shortened separators can route point reads into the
// wrong child and drop valid keys.
func TestGroupedFencePointGetAcrossSplitUsesFullSeparator(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                     dir,
		Durability:              DurabilityWALOffRelaxed,
		LeafPrefixCompression:   true,
		IndexColumnarLeaves:     true,
		IndexPackedValuePtr:     true,
		IndexInternalBaseDelta:  true,
		IndexOuterLeafMode:      IndexOuterLeafModeV2FencePtr,
		PreferAppendAlloc:       true,
		DisablePiggybackCompaction: true,
		ValueLog: ValueLogOptions{
			ReadIntegrity:              IntegritySkipChecksums,
			ForcePointers:              true,
			OuterLeafBlockCacheEntries: 16384,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const total = 10000
	const blockSize = 39
	const frameK = 16
	const writeBatch = 257
	value := bytes.Repeat([]byte{'x'}, 80)

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(dir, "wal", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(segPath), 0755); err != nil {
		t.Fatal(err)
	}
	w, err := valuelog.NewWriter(segPath, fileID)
	if err != nil {
		t.Fatal(err)
	}

	type anchorPtr struct {
		key []byte
		ptr page.ValuePtr
	}
	anchors := make([]anchorPtr, 0, total/blockSize+1)
	rid := uint64(1)
	for start := 0; start < total; {
		recs := make([]valuelog.Record, 0, frameK)
		keys := make([][]byte, 0, frameK)
		for j := 0; j < frameK && start < total; j++ {
			remaining := total - start
			n := blockSize
			if remaining < n {
				n = remaining
			}
			payload := encodeOuterLeafBlockForTest(t, start, n, value)
			recs = append(recs, valuelog.Record{RID: rid, Value: payload})
			keys = append(keys, []byte(fmt.Sprintf("k%08d", start)))
			rid++
			start += n
		}
		ptrs, err := w.AppendFrame(0, nil, recs)
		if err != nil {
			t.Fatalf("AppendFrame: %v", err)
		}
		for i := range ptrs {
			anchors = append(anchors, anchorPtr{key: keys[i], ptr: ptrs[i]})
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := d.valueLogManager.Refresh(); err != nil {
		t.Fatalf("valueLogManager.Refresh: %v", err)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	for i := 0; i < len(anchors); i += writeBatch {
		end := i + writeBatch
		if end > len(anchors) {
			end = len(anchors)
		}
		b := d.NewBatch().(*Batch)
		for _, ap := range anchors[i:end] {
			if err := b.SetPointer(ap.key, ap.ptr); err != nil {
				t.Fatalf("SetPointer(%s): %v", ap.key, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("WriteSync anchors: %v", err)
		}
		_ = b.Close()
	}

	for i := 7220; i <= 7253; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v, err := d.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if len(v) != len(value) {
			t.Fatalf("Get(%s) len=%d want=%d", k, len(v), len(value))
		}
	}
}
