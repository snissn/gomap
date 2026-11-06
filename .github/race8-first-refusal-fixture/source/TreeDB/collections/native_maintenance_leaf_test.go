package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type nativeMaintenanceLeafControl struct {
	calls int
	err   error
}

func (c *nativeMaintenanceLeafControl) MatchesNativeMaintenanceSnapshots(before, after *backenddb.Snapshot) bool {
	return false
}

func (c *nativeMaintenanceLeafControl) ReadNativeMaintenanceDictionaryLeaf(snapshot *backenddb.Snapshot, ptr page.LeafLogPtr, shape valuelog.COWRecordShape, dst []byte, call valuelog.COWBasisCall) ([]byte, error) {
	c.calls++
	return nil, c.err
}

func TestNativeMaintenanceLeafOldRefusalAndBlockIsolation(t *testing.T) {
	for _, mode := range []string{"raw", "snappy", "lz4", "dictionary"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			samples := make([][]byte, 4)
			var want []byte
			for sample := range samples {
				leaf := make([]byte, page.PageSize)
				builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
				for i := 0; i < 16; i++ {
					if err := builder.AddLeafEntry([]byte(fmt.Sprintf("a%03d", sample*16+i)), bytes.Repeat([]byte("old-inline-dictionary-value|"), 4), node.FlagInline, page.ValuePtr{}); err != nil {
						t.Fatal(err)
					}
				}
				builder.FinishNoNode()
				if sample == 0 {
					want = leaf
				}
				var err error
				samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
				if err != nil {
					t.Fatal(err)
				}
			}
			var definition []byte
			var id uint64
			if mode == "dictionary" {
				var err error
				definition, err = zstd.BuildDict(zstd.BuildDictOptions{ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(definition)
				id = binary.BigEndian.Uint64(digest[:8])
			}
			leafDir := filepath.Join(dir, "leaf_vlog")
			if err := os.MkdirAll(leafDir, 0700); err != nil {
				t.Fatal(err)
			}
			fileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 1)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(leafDir, fmt.Sprintf("value-l%d-%06d.log", valuelog.ReservedLeafLogLaneID, 1))
			writer, err := valuelog.NewWriter(path, fileID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "snappy" {
				writer.SetBlockCompression(valuelog.BlockCodecSnappy, true)
			} else if mode == "lz4" {
				writer.SetBlockCompression(valuelog.BlockCodecLZ4, true)
			}
			ptrs, err := writer.AppendFrame(id, definition, []valuelog.Record{{RID: 1, Value: samples[0]}})
			closeErr := writer.Close()
			if err != nil || closeErr != nil || len(ptrs) != 1 {
				t.Fatalf("real leaf producer: append=%v close=%v count=%d", err, closeErr, len(ptrs))
			}
			ptr, err := page.LeafLogPtrFromValuePtr(ptrs[0])
			if err != nil {
				t.Fatal(err)
			}
			owner, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := owner.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			snapshot := owner.AcquireStableSnapshot()
			if snapshot == nil {
				t.Fatal("missing real pinned leaf snapshot")
			}
			defer func() {
				if err := snapshot.Close(); err != nil {
					t.Error(err)
				}
			}()
			file, _, err := snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
			if err != nil {
				t.Fatal(err)
			}
			shape, err := valuelog.InspectCOWLeafRecord(file, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20})
			if err != nil || (mode == "raw") == shape.Compressed || (mode == "dictionary") != (shape.DictID != 0) {
				t.Fatalf("actual selected producer: mode=%s compressed=%t dictionary=%t error=%v", mode, shape.Compressed, shape.DictID != 0, err)
			}
			if (mode == "snappy" && shape.Codec != valuelog.BlockCodecSnappy) || (mode == "lz4" && shape.Codec != valuelog.BlockCodecLZ4) {
				t.Fatal("block codec did not activate")
			}
			admitted := 0
			admit := func(uint64) (func(), error) {
				admitted++
				return func() {}, nil
			}
			var dst [page.PageSize]byte
			got, err := readNativeSnapshotLeafWithAdmission(snapshot, 1<<20, admit, ptr, dst[:])
			control := &nativeMaintenanceLeafControl{err: errors.New("selected private dictionary capability")}
			if mode == "dictionary" {
				if !errors.Is(err, valuelog.ErrCOWReadUnsupported) || admitted != 0 {
					t.Fatalf("original inspected dictionary refusal: error=%v admitted=%d", err, admitted)
				}
				_, err = readNativeSnapshotLeafWithMaintenanceAdmission(snapshot, 1<<20, admit, ptr, dst[:], valuelog.COWBasisCall{}, control)
				if err != control.err || control.calls != 1 || admitted != 0 {
					t.Fatal("new scope was not selected at exact original dictionary guard")
				}
			} else {
				if err != nil || !bytes.Equal(got, want) || len(got) != page.PageSize || &got[0] != &dst[0] {
					t.Fatalf("original block/raw read: %v", err)
				}
				got, err = readNativeSnapshotLeafWithMaintenanceAdmission(snapshot, 1<<20, admit, ptr, dst[:], valuelog.COWBasisCall{}, control)
				if err != nil || !bytes.Equal(got, want) || control.calls != 0 {
					t.Fatalf("nondictionary branch entered provider: calls=%d error=%v", control.calls, err)
				}
			}
		})
	}
}
