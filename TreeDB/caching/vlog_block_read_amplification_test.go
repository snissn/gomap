package caching

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Check the persisted byte boundary and pointer order through both planners,
// including worker preparation and a block candidate that falls back to raw.
func TestValueLogBlockFrames_BoundedOrdinaryPayload(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, workers := range []bool{false, true} {
			if queued && workers { // Queued non-dictionary plans use the direct writer.
				continue
			}
			for _, opaque := range []bool{false, true} {
				t.Run(fmt.Sprintf("queued_%t/workers_%t/opaque_%t", queued, workers, opaque), func(t *testing.T) {
					dir := t.TempDir()
					fileID, _ := valuelog.EncodeFileID(0, 1)
					path := valuelog.SegmentPath(dir, fileID)
					writer, err := valuelog.NewWriter(path, fileID)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = writer.Close() })
					db := &DB{closeCh: make(chan struct{}), valueLogCompressionMode: uint8(vlogCompressionAuto), valueLogAutoPolicy: uint8(vlogAutoBalanced), valueLogBlockCodec: valuelog.BlockCodecSnappy, valueLogBlockTargetBytes: 4096, forceValueLogPointers: true, valueLogThreshold: 1, lanes: []lane{{id: 0, vlog: writer}}}
					l := &db.lanes[0]
					observeLaneVlogBlockRatio(l, valuelog.BlockCodecSnappy, 2, 1)
					if workers {
						db.startVlogDictPreparer(l)
					}
					t.Cleanup(func() { close(db.closeCh); db.wg.Wait() })
					records := rawSizingRecords(64, []int{1024, 1024, 30 << 10, 40 << 10, 0})
					for i := range records {
						if !opaque {
							clear(records[i].Value)
							if len(records[i].Value) >= 8 {
								binary.LittleEndian.PutUint64(records[i].Value, records[i].RID)
							}
						}
					}
					var ptrs []page.ValuePtr
					if queued {
						requests := make([]vlogWriteRequest, len(records))
						for i, rec := range records {
							ack := &vlogAck{}
							ack.wg.Add(1)
							requests[i] = vlogWriteRequest{rid: rec.RID, value: rec.Value, writeMode: vlogWriteBlock, blockCodec: valuelog.BlockCodecSnappy, durability: journalDurabilitySync, ack: ack}
						}
						db.flushVlogRequests(l, requests)
						for i := range requests {
							ack := requests[i].ack
							ack.wg.Wait()
							if ack.err != nil {
								t.Fatal(ack.err)
							}
							ptrs = append(ptrs, ack.ptr)
						}
					} else {
						ptrs, err = db.appendValueLog(l, 0, nil, records, journalDurabilitySync)
						if err != nil {
							t.Fatal(err)
						}
						defer putValueLogPtrs(ptrs)
					}
					if workers && l.vlogPrepWorkers == 0 {
						t.Fatal("did not exercise worker preparation")
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					compressed, packedBoundary := false, false
					frameCount, recordCount := 0, 0
					for off := 0; off < len(data); {
						size := int(binary.LittleEndian.Uint32(data[off+16 : off+20]))
						payload := data[off+valuelog.HeaderSize : off+valuelog.HeaderSize+size]
						rawBytes, count := len(payload), 1
						if data[off+5]&recordFlagGroupedTest != 0 {
							_, _, offsets, _, err := valuelog.DecodeFrame(payload)
							if err != nil {
								t.Fatal(err)
							}
							rawBytes, count = int(offsets[len(offsets)-1]), len(offsets)-1
							compressed = compressed || payload[1]&1 != 0
						}
						if rawBytes > 32<<10 && count != 1 {
							t.Fatalf("decoded payload=%d exceeds 32 KiB with %d records", rawBytes, count)
						}
						packedBoundary = packedBoundary || rawBytes == 32<<10 && count >= 3
						for j := 0; j < count; j++ {
							if ptrs[recordCount+j].Offset != uint64(off+4) || int(page.ValuePtrSubIndex(ptrs[recordCount+j])) != j {
								t.Fatalf("record %d: incorrect frame/subindex pointer", recordCount+j)
							}
						}
						frameCount++
						recordCount += count
						off += valuelog.HeaderSize + size
					}
					if !packedBoundary || recordCount != len(records) {
						t.Fatalf("packed boundary=%t records=%d, want %d", packedBoundary, recordCount, len(records))
					}
					stats := snapshotLaneVlogBlockK(l)
					if stats.Count[0] != uint64(frameCount) || stats.Sum[0] != uint64(len(records)) {
						t.Fatalf("group stats count=%d sum=%d, want emitted %d/%d", stats.Count[0], stats.Sum[0], frameCount, len(records))
					}
					if compressed == opaque {
						t.Fatalf("compressed=%t, opaque=%t: did not exercise expected kept/rejected block", compressed, opaque)
					}
					manager, err := valuelog.NewManager(dir)
					if err != nil {
						t.Fatal(err)
					}
					defer manager.Close()
					for i, ptr := range ptrs {
						got, err := manager.ReadAppend(ptr, nil)
						if err != nil || !bytes.Equal(got, records[i].Value) {
							t.Fatalf("reopened value[%d]: %v", i, err)
						}
					}
				})
			}
		}
	}
}

func TestValueLogBlockFrameBoundary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sizes []int
		k     int
		ends  []int
	}{
		{"exact", []int{0, 16 << 10, 16 << 10, 1}, 255, []int{3, 4}},
		{"oversized_singleton", []int{(32 << 10) + 1, 0, 1}, 255, []int{1, 3}},
		{"mixed", []int{1, 32 << 10, 1, 1}, 255, []int{1, 2, 4}},
		{"k_ceiling", []int{1, 1, 1, 1}, 2, []int{2, 4}},
		{"minimum_k", []int{0, 0}, 0, []int{1, 2}},
		{"max_k", make([]int, 256), 256, []int{255, 256}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := make([]valuelog.Record, len(tc.sizes))
			for i, size := range tc.sizes {
				records[i].Value = make([]byte, size)
			}
			start := 0
			for _, want := range tc.ends {
				end := nextValueLogFrameEnd(records, start, tc.k, 32<<10)
				if end != want {
					t.Fatalf("start=%d end=%d, want %d", start, end, want)
				}
				start = end
			}
			if start != len(records) {
				t.Fatal("unconsumed records")
			}
		})
	}
	// Exempt policies retain their original K; the writer still validates it.
	if got := nextValueLogFrameEnd(make([]valuelog.Record, 256), 0, 256, 0); got != 256 {
		t.Fatalf("uncapped end=%d, want 256", got)
	}
}

func TestValueLogBlockRawLimitEligibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     vlogCompressionMode
		policy   vlogAutoPolicy
		write    vlogCompressionWriteMode
		retained bool
		template bool
		leaf     bool
		want     int
	}{
		{name: "ordinary", mode: vlogCompressionAuto, write: vlogWriteBlock, want: 32 << 10},
		{name: "raw", mode: vlogCompressionAuto, write: vlogWriteOff},
		{name: "dict_write", mode: vlogCompressionAuto, write: vlogWriteDict},
		{name: "retained", mode: vlogCompressionAuto, write: vlogWriteBlock, retained: true},
		{name: "template", mode: vlogCompressionAuto, write: vlogWriteBlock, template: true},
		{name: "leaf", mode: vlogCompressionAuto, write: vlogWriteBlock, leaf: true},
		{name: "size", mode: vlogCompressionAuto, write: vlogWriteBlock, policy: vlogAutoSize},
		{name: "throughput", mode: vlogCompressionAuto, write: vlogWriteBlock, policy: vlogAutoThroughput},
		{name: "explicit_block", mode: vlogCompressionBlock, write: vlogWriteBlock},
		{name: "explicit_dict", mode: vlogCompressionDict, write: vlogWriteBlock},
		{name: "default_dict", mode: vlogCompressionDefault, write: vlogWriteBlock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{valueLogCompressionMode: uint8(tc.mode), valueLogAutoPolicy: uint8(tc.policy), valueLogTemplateEnabled: tc.template, indexOuterLeavesInValueLog: tc.leaf}
			l := &lane{}
			if tc.leaf {
				db.leafLog.id = leafLogLaneID
				l = &db.leafLog
			}
			if got := db.valueLogBlockRawLimit(l, tc.write, tc.retained); got != tc.want {
				t.Fatalf("limit=%d, want %d", got, tc.want)
			}
		})
	}
}
