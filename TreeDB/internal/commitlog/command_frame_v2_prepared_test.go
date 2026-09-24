package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

// appendCommandV2BeforeValidationReuse is the writer body at 6012614. Keep
// the two validating public entry points as the same-binary reference arm;
// this is test-only, not a second production implementation.
func appendCommandV2BeforeValidationReuse(w *Writer, env CommandEnvelope) error {
	size, err := commandFrameV2EncodedSize(env)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	payload, err := EncodeCommandFrameV2To(w.scratch[:0], env)
	if err != nil {
		return err
	}
	if len(payload) != size {
		return ErrCorrupt
	}
	w.scratch = payload
	err = w.writeRawSegmentWithChecksum(payload, crc.Checksum(payload))
	if w.commandBufRetain > 0 && cap(w.scratch) > w.commandBufRetain {
		w.scratch = make([]byte, 0, w.commandBufRetain)
	}
	return err
}

func validationReuseRawEnvelope(t testing.TB, ops []RawKVOperation) CommandEnvelope {
	t.Helper()
	payload, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatal(err)
	}
	return CommandEnvelope{
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}
}

func validationReuseCollectionEnvelope(t testing.TB, rows int) CommandEnvelope {
	t.Helper()
	docs := make([]CollectionDocument, rows)
	for i := range docs {
		id := fmt.Sprintf("%08d", i)
		docs[i] = CollectionDocument{
			ID:       []byte(id),
			Document: []byte(fmt.Sprintf(`{"id":%q,"kind":"commit","operation":"create","did":"did:plc:%08d","time_us":1720000000000000,"text":"synthetic retained WAL workload"}`, id, i)),
		}
	}
	payload, err := EncodeCollectionInsertBatchByIDPayload("posts", docs)
	if err != nil {
		t.Fatal(err)
	}
	return CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindCollectionInsertBatchByID,
		Scope:           CommandScopeCollection,
		PayloadFormat:   PayloadFormatCollectionInsertBatchByIDV1,
		Payload:         payload,
	}
}

func TestWriterV2ValidationReuseBytesAndReopen(t *testing.T) {
	empty := validationReuseRawEnvelope(t, nil)
	empty.Payload = nil // Exercise normalization, not just an already encoded payload.
	inline := validationReuseRawEnvelope(t, []RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	rid := validationReuseRawEnvelope(t, []RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("a"), RID: 9},
		{Op: RawKVOpSetRID, Key: []byte("b"), RID: 2},
		{Op: RawKVOpSetRID, Key: []byte("c"), RID: 9},
	})
	_, preconditions, err := prepareCommandFrameV2ForEncode(rid)
	if err != nil {
		t.Fatal(err)
	}
	ridExplicit := rid
	ridExplicit.Preconditions = preconditions
	large := validationReuseRawEnvelope(t, []RawKVOperation{{Op: RawKVOpSet, Key: []byte("large"), Value: bytes.Repeat([]byte("x"), 256<<10)}})
	collection := validationReuseCollectionEnvelope(t, 128)
	for _, tc := range []struct {
		name string
		env  CommandEnvelope
	}{
		{"empty", empty}, {"inline", inline}, {"rid_auto_fence", rid},
		{"rid_explicit_fence", ridExplicit}, {"direct_large", large},
		{"collection", collection}, {"barrier", NewDurablePrefixBarrierV1(1, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, compress := range []bool{false, true} {
				t.Run(fmt.Sprintf("compress=%t", compress), func(t *testing.T) {
					var files [2][]byte
					for arm := range files {
						path := filepath.Join(t.TempDir(), "commit.log")
						w, err := NewWriterWithOptions(path, Options{Compress: compress, BufferSize: 4096, DeferredCommandBufferSize: 4096, DeferredCommandBufferRetainSize: 1024})
						if err != nil {
							t.Fatal(err)
						}
						for i := 0; i < 3; i++ {
							env := tc.env
							env.LSN = uint64(i + 1)
							if i == 1 && env.Kind != CommandKindDurablePrefixBarrier {
								env.DurabilityClass = CommandDurabilityRelaxed
							}
							if arm == 0 {
								err = appendCommandV2BeforeValidationReuse(w, env)
							} else {
								err = w.AppendCommandV2(env)
							}
							if err != nil {
								_ = w.Close()
								t.Fatal(err)
							}
						}
						if err := w.Sync(); err != nil {
							_ = w.Close()
							t.Fatal(err)
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						files[arm], err = os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						frames, err := ScanCommandFramesV2(path, Options{})
						if err != nil || len(frames) != 3 {
							t.Fatalf("reopen: frames=%d err=%v", len(frames), err)
						}
						for i, frame := range frames {
							want := tc.env
							want.LSN = uint64(i + 1)
							if i == 1 && want.Kind != CommandKindDurablePrefixBarrier {
								want.DurabilityClass = CommandDurabilityRelaxed
							}
							want, pre, err := prepareCommandFrameV2ForEncode(want)
							if err != nil {
								t.Fatal(err)
							}
							want.Preconditions = pre
							if frame.LSN != want.LSN || frame.Kind != want.Kind || frame.Version != CommandFrameVersionV2 || frame.DurabilityClass != want.DurabilityClass || !bytes.Equal(frame.Payload, want.Payload) || !reflect.DeepEqual(frame.Preconditions, want.Preconditions) {
								t.Fatalf("reopened frame %d differs from normalized input", i)
							}
						}
					}
					if !bytes.Equal(files[0], files[1]) {
						t.Fatal("optimized writer changed persisted bytes or CRC")
					}
				})
			}
		})
	}
}

func TestWriterV2ValidationReuseRejectsBeforeMutation(t *testing.T) {
	base := validationReuseRawEnvelope(t, []RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	for _, tc := range []struct {
		name   string
		mutate func(*CommandEnvelope)
	}{
		{"zero_lsn", func(e *CommandEnvelope) { e.LSN = 0 }},
		{"bad_version", func(e *CommandEnvelope) { e.Version = CommandFrameVersion }},
		{"bad_durability", func(e *CommandEnvelope) { e.DurabilityClass = 0 }},
		{"critical_flags", func(e *CommandEnvelope) { e.FeatureFlags = commandWALCriticalFlagsMask }},
		{"bad_payload", func(e *CommandEnvelope) { e.Payload = []byte{0xff} }},
		{"spurious_fence", func(e *CommandEnvelope) {
			e.Preconditions = []CommandExtension{{Type: CommandExtensionExternalRefFenceV1, Payload: []byte{1}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := base
			tc.mutate(&env)
			var referenceErr error
			for arm := 0; arm < 2; arm++ {
				path := filepath.Join(t.TempDir(), "commit.log")
				w, err := NewWriterWithOptions(path, Options{MaxSegmentSize: 32}) // Semantic errors still precede size errors.
				if err != nil {
					t.Fatal(err)
				}
				w.scratch = bytes.Repeat([]byte{0xa5}, 512)
				before := append([]byte(nil), w.scratch...)
				stats, size, durability := w.BufferStats(), w.Size(), w.DurabilityStats()
				if arm == 0 {
					err = appendCommandV2BeforeValidationReuse(w, env)
					referenceErr = err
				} else {
					err = w.AppendCommandV2(env)
				}
				if err == nil || referenceErr == nil || err.Error() != referenceErr.Error() {
					t.Fatalf("error=%v reference=%v", err, referenceErr)
				}
				if !bytes.Equal(before, w.scratch) || stats != w.BufferStats() || size != w.Size() || durability != w.DurabilityStats() {
					t.Fatal("rejected frame changed scratch, buffers, file size, or I/O counters")
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				contents, err := os.ReadFile(path)
				if err != nil || len(contents) != 0 {
					t.Fatalf("rejected frame wrote bytes: len=%d err=%v", len(contents), err)
				}
			}
		})
	}
}

func TestWriterV2ValidationReuseCapAndRevalidation(t *testing.T) {
	env := validationReuseRawEnvelope(t, []RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("k"), RID: 7}})
	size, err := commandFrameV2EncodedSize(env)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{size - 1, size} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			w, err := NewWriterWithOptions(filepath.Join(t.TempDir(), "commit.log"), Options{MaxSegmentSize: int64(limit)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close() })
			if err := w.AppendCommandV2(env); limit < size {
				if !errors.Is(err, ErrRecordTooLarge) || cap(w.scratch) != 0 || w.Size() != 0 {
					t.Fatalf("oversized append mutated writer: err=%v scratch=%d size=%d", err, cap(w.scratch), w.Size())
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
	w, err := NewWriterWithOptions(filepath.Join(t.TempDir(), "commit.log"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if err := w.AppendCommandV2(env); err != nil {
		t.Fatal(err)
	}
	before := w.Size()
	env.LSN++
	env.Payload = append([]byte(nil), env.Payload...)
	binary.LittleEndian.PutUint16(env.Payload[:2], 0xffff)
	if err := w.AppendCommandV2(env); err == nil || w.Size() != before {
		t.Fatalf("a later call reused earlier validation: err=%v size=%d want=%d", err, w.Size(), before)
	}
}

// This is a synthetic durable WAL benchmark, not a JSONBench load or ClickHouse
// result. Use -benchtime=5x -count=5 to bound bytes and retain both arms.
func BenchmarkWriterV2ValidationReuse(b *testing.B) {
	for _, rows := range []int{16, 16000} {
		env := validationReuseCollectionEnvelope(b, rows)
		for _, optimized := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/optimized=%t", rows, optimized), func(b *testing.B) {
				w, err := NewWriterWithOptions(filepath.Join(b.TempDir(), "commit.log"), Options{})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = w.Close() })
				b.SetBytes(int64(len(env.Payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					current := env
					current.LSN = uint64(i + 1)
					if optimized {
						err = w.AppendCommandV2(current)
					} else {
						err = appendCommandV2BeforeValidationReuse(w, current)
					}
					if err != nil {
						b.Fatal(err)
					}
					if err := w.Sync(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
