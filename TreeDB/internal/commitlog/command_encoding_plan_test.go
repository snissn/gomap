package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func encodingPlanEnvelope(t *testing.T, rid uint64) CommandEnvelope {
	t.Helper()
	op := RawKVOperation{Op: RawKVOpSet, Key: []byte("key"), Value: []byte("value")}
	if rid != 0 {
		op = RawKVOperation{Op: RawKVOpSetRID, Key: []byte("key"), RID: rid}
	}
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{op})
	if err != nil {
		t.Fatal(err)
	}
	return CommandEnvelope{Version: CommandFrameVersionV2, DurabilityClass: CommandDurabilityDurable,
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload}
}

// This compares the unchanged V1 section layout with the V2 wire header, and
// verifies that writer-owned bytes survive caller reuse before Flush. Existing
// V2 golden files independently bind the canonical RID fence and barrier bytes.
func TestCommandFrameV2SharedEncodingWireAndOwnership(t *testing.T) {
	collection, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{{ID: []byte("one"), Document: []byte(`{"name":"Ada"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  CommandEnvelope
	}{
		{"nil_raw", CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}},
		{"inline", encodingPlanEnvelope(t, 0)},
		{"rid", encodingPlanEnvelope(t, 42)},
		{"collection", CommandEnvelope{Kind: CommandKindCollectionInsertBatchByID, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1, Payload: collection}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := tc.env
			env.Version, env.DurabilityClass, env.LSN = CommandFrameVersionV2, CommandDurabilityRelaxed, 17
			env.Preconditions = []CommandExtension{{Type: 71, Payload: []byte("precondition")}}
			env.ResultAssertions = []CommandExtension{{Type: 72, Payload: []byte("assertion")}}
			want, err := EncodeCommandFrameV2(env)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeCommandFrameV2(want)
			if err != nil {
				t.Fatal(err)
			}
			legacy := decoded
			legacy.Version, legacy.DurabilityClass = CommandFrameVersion, 0
			legacyBytes, err := EncodeCommandFrame(legacy)
			if err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint16(legacyBytes[4:6], CommandFrameVersionV2)
			binary.LittleEndian.PutUint16(legacyBytes[6:8], CommandFrameVersionV2)
			binary.LittleEndian.PutUint16(legacyBytes[54:56], uint16(CommandDurabilityRelaxed))
			if !bytes.Equal(want, legacyBytes) {
				t.Fatal("V2 changed canonical section bytes")
			}
			w, err := NewWriter(filepath.Join(t.TempDir(), "command.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err := w.AppendCommandV2(env); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(w.scratch, want) {
				t.Fatal("public writer and encoder diverged")
			}
			clear(env.Payload)
			clear(env.Preconditions[0].Payload)
			clear(env.ResultAssertions[0].Payload)
			clear(w.scratch)
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			r, err := NewReader(w.f.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := r.ReadCommandFrameV2()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := EncodeCommandFrameV2(got)
			if err != nil || !bytes.Equal(actual, want) {
				t.Fatal("writer retained borrowed input or emission scratch", err)
			}
		})
	}
}

func TestCommandJournalV2EncodingRevalidatesMutableHooks(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "canonical_fence_and_extension", true: "reject_before_rotation"}[invalid], func(t *testing.T) {
			j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if _, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); err != nil {
				t.Fatal(err)
			}
			env := encodingPlanEnvelope(t, 42)
			env.Preconditions = []CommandExtension{{Type: 71, Payload: []byte("old")}}
			beforeSegment, beforeLSN := j.segmentSeq, j.NextLSN()
			lsn, err := j.AppendCommandObservedWithHooks(env, func() error {
				if invalid {
					env.Payload[0] = 0xff
				} else {
					binary.LittleEndian.PutUint64(env.Payload[len(env.Payload)-8:], 99)
					env.Preconditions[0].Payload = bytes.Repeat([]byte("new"), 40)
				}
				return nil
			}, nil)
			if invalid {
				if err == nil || lsn != 0 || j.segmentSeq != beforeSegment || j.NextLSN() != beforeLSN {
					t.Fatalf("invalid mutable hook rotated/reserved/appended: lsn=%d err=%v segment=%d next=%d", lsn, err, j.segmentSeq, j.NextLSN())
				}
				return
			}
			if err != nil || lsn != beforeLSN {
				t.Fatal(lsn, err)
			}
			if err := j.Flush(); err != nil {
				t.Fatal(err)
			}
			frames, err := ScanCommandFramesV2(j.path, Options{})
			if err != nil || len(frames) != 1 {
				t.Fatal("fresh post-hook frame", len(frames), err)
			}
			ops, err := DecodeRawKVBatchPayload(frames[0].Payload)
			if err != nil || len(ops) != 1 || ops[0].RID != 99 || len(frames[0].Preconditions[0].Payload) != 120 {
				t.Fatal("stale payload, canonical fence or extension lengths", err)
			}
		})
	}
}

func TestCommandJournalV2EncodingRevalidatesRotationCallback(t *testing.T) {
	env := encodingPlanEnvelope(t, 42)
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1, OnSegmentRotated: func(int64) {
		binary.LittleEndian.PutUint64(env.Payload[len(env.Payload)-8:], 99)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); err != nil {
		t.Fatal(err)
	}
	if lsn, err := j.AppendCommand(env); err != nil || lsn != 2 {
		t.Fatal(lsn, err)
	}
	if err := j.Flush(); err != nil {
		t.Fatal(err)
	}
	frames, err := ScanCommandFramesV2(j.path, Options{})
	if err != nil || len(frames) != 1 {
		t.Fatal("rotation reused stale canonical plan", len(frames), err)
	}
	ops, err := DecodeRawKVBatchPayload(frames[0].Payload)
	if err != nil || ops[0].RID != 99 || frames[0].LSN != 2 {
		t.Fatal("rotation/actual LSN mismatch", err)
	}
}

func TestCommandJournalV2EncodingActualLSNAndRollback(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{InitialLSN: ^uint64(0) - 1})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	restore := j.installBufferedWriterForTest(commandJournalFailWriter{}, 1)
	// Fixed V2 frames now enter owned memory directly. Seed older unaccepted
	// bytes so the required ordered drain still observes this exact failure
	// after reservation. Restore discards only the injected, unaccepted buffer.
	if _, err := j.writer.bw.Write([]byte{0xa5}); err != nil {
		t.Fatal(err)
	}
	if lsn, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); lsn != 0 || !errors.Is(err, errCommandJournalInjectedWrite) {
		t.Fatal("changed failed-write identity", lsn, err)
	}
	restore()
	if lsn, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); err != nil || lsn != ^uint64(0) {
		t.Fatal("failed write did not roll back final LSN", lsn, err)
	}
	if err := j.Flush(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(j.path)
	if err != nil {
		t.Fatal(err)
	}
	if lsn, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); err == nil || lsn != 0 {
		t.Fatal("exhausted journal appended", lsn, err)
	}
	after, err := os.ReadFile(j.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("exhaustion changed physical bytes", err)
	}
	r, err := NewReader(j.path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.ReadCommandFrameV2()
	if err != nil || got.LSN != ^uint64(0) {
		t.Fatal("synthetic rather than actual LSN emitted", got.LSN, err)
	}
	if _, err := r.ReadCommandFrameV2(); !errors.Is(err, io.EOF) {
		t.Fatal("extra exhausted frame", err)
	}
}

func TestCommandFrameV2SharedEncodingRejectsBeforeDestinationMutation(t *testing.T) {
	empty := encodingPlanEnvelope(t, 0)
	empty.LSN, empty.Payload = 1, []byte{}
	barrier := NewDurablePrefixBarrierV1(2, 1)
	barrier.ResultAssertions = []CommandExtension{{Type: 71, Payload: []byte("invalid")}}
	for _, env := range []CommandEnvelope{empty, barrier} {
		dst := bytes.Repeat([]byte{0xa5}, 512)
		want := bytes.Clone(dst)
		if _, err := EncodeCommandFrameV2To(dst[:0], env); err == nil {
			t.Fatal("invalid empty payload/barrier accepted")
		}
		if !bytes.Equal(dst, want) {
			t.Fatal("semantic rejection changed destination backing storage")
		}
	}
}

func TestCommandFrameV2SharedEncodingAliasedSections(t *testing.T) {
	env := encodingPlanEnvelope(t, 0)
	env.LSN = 1
	env.Preconditions = []CommandExtension{{Type: 71, Payload: []byte("literal-precondition")}}
	env.ResultAssertions = []CommandExtension{{Type: 72, Payload: []byte("literal-assertion")}}
	want, err := EncodeCommandFrameV2(env)
	if err != nil {
		t.Fatal(err)
	}
	// Both extension bodies overlap the destination header. The established
	// public encoder snapshots sections before touching dst; payload overlap
	// semantics are deliberately unchanged by this compatibility control.
	dst := make([]byte, 1024)
	copy(dst, env.Preconditions[0].Payload)
	copy(dst[32:], env.ResultAssertions[0].Payload)
	env.Preconditions[0].Payload = dst[:len(env.Preconditions[0].Payload)]
	env.ResultAssertions[0].Payload = dst[32 : 32+len(env.ResultAssertions[0].Payload)]
	got, err := EncodeCommandFrameV2To(dst[:0], env)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("destination overwrite corrupted borrowed extension bytes")
	}
}

func TestCommandJournalV2EncodingHookCapacityBeforeRotation(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1, MaxSegmentSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err := j.AppendCommand(encodingPlanEnvelope(t, 0)); err != nil {
		t.Fatal(err)
	}
	env := encodingPlanEnvelope(t, 0)
	env.ResultAssertions = []CommandExtension{{Type: 71, Payload: []byte("small")}}
	beforeSegment, beforeLSN := j.segmentSeq, j.NextLSN()
	lsn, err := j.AppendCommandObservedWithHooks(env, func() error {
		env.ResultAssertions[0].Payload = bytes.Repeat([]byte("large"), 100)
		return nil
	}, nil)
	if !errors.Is(err, ErrRecordTooLarge) || lsn != 0 || j.segmentSeq != beforeSegment || j.NextLSN() != beforeLSN {
		t.Fatalf("post-hook capacity was not checked before rotation/reservation: lsn=%d err=%v segment=%d next=%d", lsn, err, j.segmentSeq, j.NextLSN())
	}
}
