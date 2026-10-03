package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Exercise the two production raw planners, rather than only their K chooser.
func TestValueLogRawFrames_DurableSizingAndFallback(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, shape := range []struct {
			name   string
			sizes  []int
			mode   vlogCompressionMode
			target int
		}{
			{"off_4k", []int{4096}, vlogCompressionOff, 4096},
			{"off_4k_writer_fallback", []int{4096}, vlogCompressionOff, 4096},
			{"missing_dict_raw", []int{4096}, vlogCompressionDefault, 4096},
			{"auto_block_reject_characterization", []int{4096}, vlogCompressionAuto, 4096},
			{"off_small", []int{128}, vlogCompressionOff, 4096},
			{"off_mixed", []int{128, 2048, 512}, vlogCompressionOff, 4096},
			{"off_oversized", []int{8192, 128}, vlogCompressionOff, 4096},
			{"off_configured_target", []int{512}, vlogCompressionOff, 1024},
			{"auto_raw_bypass", []int{512}, vlogCompressionAuto, 4096},
		} {
			t.Run(fmt.Sprintf("queued_%t/%s", queued, shape.name), func(t *testing.T) {
				dir := t.TempDir()
				fileID, _ := valuelog.EncodeFileID(0, 1)
				path := valuelog.SegmentPath(dir, fileID)
				writer, err := valuelog.NewWriter(path, fileID)
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				db := &DB{closeCh: make(chan struct{}), valueLogCompressionMode: uint8(shape.mode), valueLogBlockTargetBytes: shape.target, forceValueLogPointers: true, valueLogThreshold: 1, lanes: []lane{{id: 0, vlog: writer}}}
				if shape.name == "off_4k_writer_fallback" {
					// Hide optional raw batch/stats capabilities to cover AppendFrame.
					db.lanes[0].vlog = struct{ valueWriter }{writer}
				}
				dictID := uint64(0)
				writeMode := vlogWriteOff
				if shape.name == "missing_dict_raw" {
					db.dictStore = &queueDictStore{}
					dictID = 99
					writeMode = vlogWriteDict
				}
				records := rawSizingRecords(32, shape.sizes)
				var ptrs []page.ValuePtr
				if queued {
					requests := make([]vlogWriteRequest, len(records))
					for i, rec := range records {
						ack := &vlogAck{}
						ack.wg.Add(1)
						requests[i] = vlogWriteRequest{rid: rec.RID, value: rec.Value, dictID: dictID, writeMode: writeMode, durability: journalDurabilitySync, ack: ack}
					}
					db.flushVlogRequests(&db.lanes[0], requests)
					for i := range requests {
						ack := requests[i].ack
						ack.wg.Wait()
						if ack.err != nil {
							t.Fatal(ack.err)
						}
						ptrs = append(ptrs, ack.ptr)
					}
				} else {
					ptrs, err = db.appendValueLog(&db.lanes[0], dictID, nil, records, journalDurabilitySync)
					if err != nil {
						t.Fatal(err)
					}
					defer putValueLogPtrs(ptrs)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				oversizedGrouped := false
				for off := 0; off < len(data); {
					size := int(binary.LittleEndian.Uint32(data[off+16 : off+20]))
					payload := data[off+valuelog.HeaderSize : off+valuelog.HeaderSize+size]
					rawBytes, count := len(payload), 1
					if data[off+5]&recordFlagGroupedTest != 0 {
						_, _, offsets, body, err := valuelog.DecodeFrame(payload)
						if err != nil {
							t.Fatal(err)
						}
						if payload[1]&1 != 0 {
							t.Fatal("expected raw frame")
						}
						rawBytes, count = len(body), len(offsets)-1
					}
					if rawBytes > shape.target && count != 1 {
						oversizedGrouped = true
						if shape.name == "auto_block_reject_characterization" && !queued {
							// Block bootstrap/rejection keeps its separate grouping policy.
							// This is measured scope, not the chooser-selected raw path.
							off += valuelog.HeaderSize + size
							continue
						}
						t.Fatalf("raw grouped payload=%d exceeds target=%d with %d values", rawBytes, shape.target, count)
					}
					off += valuelog.HeaderSize + size
				}
				if shape.name == "auto_block_reject_characterization" && !queued && !oversizedGrouped {
					t.Fatal("auto block bootstrap/rejection grouping changed; reassess characterization")
				}
				manager, err := valuelog.NewManager(dir)
				if err != nil {
					t.Fatal(err)
				}
				for i, ptr := range ptrs {
					got, err := manager.ReadAppend(ptr, nil)
					if err != nil || !bytes.Equal(got, records[i].Value) {
						t.Fatalf("reopened value[%d]: err=%v", i, err)
					}
				}
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				// Reopen again after mutating persisted payload bytes: CRC remains required.
				data[len(data)-1] ^= 0xff
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				manager, err = valuelog.NewManager(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer manager.Close()
				if _, err := manager.ReadAppend(ptrs[len(ptrs)-1], nil); !errors.Is(err, valuelog.ErrCorrupt) {
					t.Fatalf("corrupt raw payload: got %v, want ErrCorrupt", err)
				}
			})
		}
	}
}

func rawSizingRecords(count int, sizes []int) []valuelog.Record {
	records := make([]valuelog.Record, count)
	state := uint32(1)
	for i := range records {
		value := make([]byte, sizes[i%len(sizes)])
		for j := range value {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	return records
}

func BenchmarkValueLogRawFrameRead(b *testing.B) {
	for _, size := range []int{512, 4096} {
		b.Run(fmt.Sprintf("value_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			fileID, _ := valuelog.EncodeFileID(0, 1)
			path := valuelog.SegmentPath(dir, fileID)
			writer, err := valuelog.NewWriter(path, fileID)
			if err != nil {
				b.Fatal(err)
			}
			db := &DB{closeCh: make(chan struct{}), valueLogCompressionMode: uint8(vlogCompressionOff), valueLogBlockTargetBytes: 4096, lanes: []lane{{id: 0, vlog: writer}}}
			records := rawSizingRecords(256, []int{size})
			ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilitySync)
			if err != nil {
				b.Fatal(err)
			}
			defer putValueLogPtrs(ptrs)
			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				b.Fatal(err)
			}
			crcBytes := make([]int, len(ptrs))
			for i, ptr := range ptrs {
				start := int(ptr.Offset) - 4
				crcBytes[i] = valuelog.HeaderSize - 4 + int(binary.LittleEndian.Uint32(data[start+16:start+20]))
			}
			manager, err := valuelog.NewManager(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer manager.Close()
			// Admit the sealed mmap outside timing.
			if _, err := manager.ReadAppend(ptrs[0], nil); err != nil {
				b.Fatal(err)
			}
			before := manager.ReadStats().RecordCRCChecks
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			totalCRCBytes := uint64(0)
			for i := 0; i < b.N; i++ {
				idx := i % len(ptrs)
				got, err := manager.ReadAppend(ptrs[idx], nil)
				if err != nil || len(got) != size {
					b.Fatal(err)
				}
				totalCRCBytes += uint64(crcBytes[idx])
			}
			b.StopTimer()
			b.ReportMetric(float64(totalCRCBytes)/float64(b.N*size), "CRC-bytes/returned-byte")
			b.ReportMetric(float64(manager.ReadStats().RecordCRCChecks-before)/float64(b.N), "CRC-checks/op")
		})
	}
}
