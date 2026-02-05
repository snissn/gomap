package caching

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogDictFramePipeline_OrderAndPointers(t *testing.T) {
	const (
		dictID    = 1
		fileID    = 123
		valueSize = 1024
		valueCnt  = 256
		k         = 16
		workers   = 4
	)

	values := make([][]byte, valueCnt)
	base := bytes.Repeat([]byte("compressible-"), 64)
	for i := 0; i < valueCnt; i++ {
		v := make([]byte, valueSize)
		copy(v, base)
		binary.LittleEndian.PutUint32(v[valueSize-4:], uint32(i))
		values[i] = v
	}

	const historyBytes = 32 << 10
	history := make([]byte, 0, historyBytes)
	for _, v := range values {
		if len(history) >= historyBytes {
			break
		}
		need := historyBytes - len(history)
		if len(v) > need {
			history = append(history, v[:need]...)
		} else {
			history = append(history, v...)
		}
	}
	if len(history) < 8 {
		t.Fatalf("history too small: %d", len(history))
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: values[:128],
		History:  history,
		Level:    zstd.SpeedFastest,
	})
	if err != nil || len(dict) == 0 {
		t.Fatalf("BuildDict failed: %v", err)
	}

	records := make([]valuelog.Record, valueCnt)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: values[i]}
	}
	ptrs := make([]page.ValuePtr, len(records))

	run := func(t *testing.T, maxInFlight int64) {
		t.Helper()

		var sink bytes.Buffer
		writer := valuelog.NewWriterWithSink(&sink, fileID)
		db := &DB{
			closeCh:                          make(chan struct{}),
			valueLogDictFramePipelineWorkers: workers,
			valueLogDictFramePipelineMaxInFlightBytes: maxInFlight,
			valueLogDictFrameEncodeLevel:              zstd.SpeedFastest,
			valueLogDictFrameEnableEntropy:            false,
		}
		t.Cleanup(func() {
			close(db.closeCh)
			db.wg.Wait()
		})
		for i := range ptrs {
			ptrs[i] = page.ValuePtr{}
		}

		rawFrameBytes, storedPayloadBytes, frameRecords, framesTotal, framesAttempted, _, _, _, err :=
			db.appendValueLogDictFramesPipeline(writer, dictID, dict, records, k, ptrs)
		if err != nil {
			t.Fatalf("appendValueLogDictFramesPipeline: %v", err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if frameRecords != len(records) {
			t.Fatalf("expected frameRecords=%d got %d", len(records), frameRecords)
		}
		wantFrames := (len(records) + k - 1) / k
		if framesTotal != wantFrames {
			t.Fatalf("expected framesTotal=%d got %d", wantFrames, framesTotal)
		}
		if framesAttempted != wantFrames {
			t.Fatalf("expected framesAttempted=%d got %d", wantFrames, framesAttempted)
		}
		if rawFrameBytes <= 0 || storedPayloadBytes <= 0 {
			t.Fatalf("expected non-zero bytes: raw=%d stored=%d", rawFrameBytes, storedPayloadBytes)
		}

		dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		t.Cleanup(func() { dec.Close() })

		data := sink.Bytes()
		pos := 0
		recIdx := 0
		for pos < len(data) {
			if len(data)-pos < valuelog.HeaderSize {
				t.Fatalf("truncated header at pos=%d remaining=%d", pos, len(data)-pos)
			}
			header := data[pos : pos+valuelog.HeaderSize]
			bodyLen := int(binary.LittleEndian.Uint32(header[16:20]))
			bodyStart := pos + valuelog.HeaderSize
			bodyEnd := bodyStart + bodyLen
			if bodyLen <= 0 || bodyEnd > len(data) {
				t.Fatalf("invalid body len=%d at pos=%d remaining=%d", bodyLen, pos, len(data)-pos)
			}
			body := data[bodyStart:bodyEnd]

			fh, rids, offsets, payload, err := valuelog.DecodeFrame(body)
			if err != nil {
				t.Fatalf("DecodeFrame at pos=%d: %v", pos, err)
			}
			if fh.DictID != dictID {
				t.Fatalf("expected dictID=%d got %d", dictID, fh.DictID)
			}
			if fh.K == 0 || int(fh.K) != len(rids) {
				t.Fatalf("invalid k=%d rids=%d", fh.K, len(rids))
			}
			if len(offsets) != len(rids)+1 {
				t.Fatalf("invalid offsets=%d k=%d", len(offsets), len(rids))
			}

			rawPayload := payload
			if fh.Flags&valuelog.FrameFlagCompressed != 0 {
				out, err := dec.DecodeAll(payload, nil)
				if err != nil {
					t.Fatalf("DecodeAll at pos=%d: %v", pos, err)
				}
				rawPayload = out
			}
			if want := int(offsets[len(offsets)-1]); want != len(rawPayload) {
				t.Fatalf("payload len mismatch at pos=%d: want=%d got=%d", pos, want, len(rawPayload))
			}

			for slot := 0; slot < len(rids); slot++ {
				if recIdx >= len(records) {
					t.Fatalf("decoded too many records: recIdx=%d len=%d", recIdx, len(records))
				}
				want := records[recIdx]
				if rids[slot] != want.RID {
					t.Fatalf("rid mismatch recIdx=%d slot=%d: want=%d got=%d", recIdx, slot, want.RID, rids[slot])
				}
				start := offsets[slot]
				end := offsets[slot+1]
				if end < start || int(end) > len(rawPayload) {
					t.Fatalf("invalid offsets recIdx=%d slot=%d start=%d end=%d payload=%d", recIdx, slot, start, end, len(rawPayload))
				}
				got := rawPayload[start:end]
				if !bytes.Equal(got, want.Value) {
					t.Fatalf("value mismatch recIdx=%d slot=%d", recIdx, slot)
				}

				ptr := ptrs[recIdx]
				if ptr.FileID != fileID {
					t.Fatalf("ptr fileID mismatch recIdx=%d: want=%d got=%d", recIdx, fileID, ptr.FileID)
				}
				if ptr.Offset != uint64(pos+4) {
					t.Fatalf("ptr offset mismatch recIdx=%d: want=%d got=%d", recIdx, pos+4, ptr.Offset)
				}
				if !page.ValuePtrIsGrouped(ptr) {
					t.Fatalf("expected grouped ptr recIdx=%d: %+v", recIdx, ptr)
				}
				if gotSub := page.ValuePtrSubIndex(ptr); gotSub != uint8(slot) {
					t.Fatalf("ptr subIndex mismatch recIdx=%d: want=%d got=%d", recIdx, slot, gotSub)
				}

				recIdx++
			}
			pos = bodyEnd
		}
		if recIdx != len(records) {
			t.Fatalf("decoded records mismatch: want=%d got=%d", len(records), recIdx)
		}
		if pos != len(data) {
			t.Fatalf("pos mismatch: want=%d got=%d", len(data), pos)
		}
	}

	t.Run("concurrent", func(t *testing.T) { run(t, 1<<20) })
	t.Run("oversize_frame", func(t *testing.T) { run(t, 8<<10) })
}
