package commitlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func TestCommandV2PayloadCRCAndStreamDirectParity(t *testing.T) {
	payload := make([]byte, directCommandPayloadMinLen+137)
	for i := range payload {
		payload[i] = byte((i*31 + 17) & 0xff)
	}
	payloadCRC := ChecksumCommandPayload(payload)
	if payloadCRC != crc.Checksum(payload) {
		t.Fatalf("ChecksumCommandPayload mismatch: got=%#x want=%#x", payloadCRC, crc.Checksum(payload))
	}

	precond := []CommandExtension{{Type: 1, Payload: []byte("schema-v1-precondition")}}
	asserts := []CommandExtension{{Type: 2, Payload: []byte("post-state-assertion")}}

	makeEnv := func(withExtensions bool) CommandEnvelope {
		env := CommandEnvelope{
			Version:         CommandFrameVersionV2,
			DurabilityClass: CommandDurabilityRelaxed,
			Kind:            CommandKindCollectionInsertBatchByID,
			Scope:           CommandScopeCollection,
			PayloadFormat:   PayloadFormatCollectionInsertBatchByIDV1,
			Payload:         payload,
		}
		if withExtensions {
			env.Preconditions = precond
			env.ResultAssertions = asserts
		}
		return env
	}

	// Compare stream WAL bytes produced via HasPayloadCRC + TrustedPayload +
	// direct writev against the baseline untrusted full-CRC path.
	for _, withExt := range []bool{false, true} {
		name := "stream_plain"
		if withExt {
			name = "stream_with_extensions"
		}
		t.Run(name, func(t *testing.T) {
			// Build a valid canonical CollectionInsertBatchByID payload of >= directCommandPayloadMinLen.
			val := bytes.Repeat([]byte("{\"k\":\"v\"}"), (directCommandPayloadMinLen/9)+16)
			canonPayload, err := EncodeCollectionInsertBatchByIDPayload("docs", []CollectionDocument{
				{ID: []byte("doc-1"), Document: val},
			})
			if err != nil {
				t.Fatal(err)
			}
			env := makeEnv(withExt)
			env.LSN = 1
			env.Payload = canonPayload
			canonCRC := ChecksumCommandPayload(canonPayload)

			// Baseline: untrusted plan without precomputed payloadCRC written via
			// the non-direct scratch path on a stream Writer.
			basePath := filepath.Join(t.TempDir(), "base.log")
			wBase, err := NewWriter(basePath)
			if err != nil {
				t.Fatal(err)
			}
			planBase, err := prepareCommandFrameV2Encoding(env)
			if err != nil {
				_ = wBase.Close()
				t.Fatal(err)
			}
			encodedBase := planBase.encodeTo(nil)
			if err := wBase.writeRawSegmentWithChecksum(encodedBase, crc.Checksum(encodedBase)); err != nil {
				_ = wBase.Close()
				t.Fatal(err)
			}
			if err := wBase.Close(); err != nil {
				t.Fatal(err)
			}

			// Fast path: trusted plan with precomputed payloadCRC written via
			// appendCommandV2Prepared -> appendStreamCommandV2Direct (zero-copy writev).
			fastPath := filepath.Join(t.TempDir(), "fast.log")
			wFast, err := NewWriter(fastPath)
			if err != nil {
				t.Fatal(err)
			}
			planFast, err := prepareCommandFrameV2EncodingWithTrust(env, true)
			if err != nil {
				_ = wFast.Close()
				t.Fatal(err)
			}
			planFast.payloadCRC = canonCRC
			planFast.hasPayloadCRC = true
			if err := wFast.appendCommandV2Prepared(planFast); err != nil {
				_ = wFast.Close()
				t.Fatal(err)
			}
			if err := wFast.Close(); err != nil {
				t.Fatal(err)
			}

			lsnFast := env.LSN
			baseBytes, err := os.ReadFile(basePath)
			if err != nil {
				t.Fatal(err)
			}
			fastBytes, err := os.ReadFile(fastPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(baseBytes, fastBytes) {
				t.Fatalf("stream segment bytes differ: base=%d fast=%d", len(baseBytes), len(fastBytes))
			}

			// Verify reader replays cleanly and detects single-bit corruption.
			r, err := NewReader(fastPath)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.ReadCommandFrameV2()
			_ = r.Close()
			if err != nil {
				t.Fatalf("ReadCommand fast stream: %v", err)
			}
			if got.LSN != lsnFast || !bytes.Equal(got.Payload, canonPayload) {
				t.Fatalf("replayed envelope mismatch: lsn=%d payloadLen=%d", got.LSN, len(got.Payload))
			}
			if withExt && (len(got.Preconditions) != 1 || len(got.ResultAssertions) != 1) {
				t.Fatalf("replayed extensions mismatch: pre=%d assert=%d", len(got.Preconditions), len(got.ResultAssertions))
			}

			corruptPath := filepath.Join(t.TempDir(), "corrupt.log")
			corrupt := append([]byte(nil), fastBytes...)
			corrupt[len(corrupt)-1] ^= 0x01
			if err := os.WriteFile(corruptPath, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}
			rc, err := NewReader(corruptPath)
			if err != nil {
				t.Fatal(err)
			}
			_, err = rc.ReadCommandFrameV2()
			_ = rc.Close()
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt stream frame err=%v want ErrCorrupt", err)
			}
		})
	}

	if !FixedWALCreationSupported() {
		return
	}
	t.Run("fixed_with_extensions_and_payload_crc", func(t *testing.T) {
		val := bytes.Repeat([]byte("{\"a\":123}"), 4096)
		canonPayload, err := EncodeCollectionInsertBatchByIDPayload("docs", []CollectionDocument{
			{ID: []byte("doc-fixed"), Document: val},
		})
		if err != nil {
			t.Fatal(err)
		}
		env := makeEnv(true)
		env.Payload = canonPayload
		canonCRC := ChecksumCommandPayload(canonPayload)

		dir := t.TempDir()
		j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
		if err != nil {
			t.Fatal(err)
		}
		lsn, deferred, err := j.AppendCommandObservedWithOptions(env, CommandAppendOptions{
			ImmutableHooks:  true,
			TrustedPayload:  true,
			DeferFixedFlush: true,
			PayloadCRC:      canonCRC,
			HasPayloadCRC:   true,
		}, nil, nil)
		if err != nil {
			_ = j.Close()
			t.Fatal(err)
		}
		if !deferred {
			_ = j.Close()
			t.Fatal("expected DeferFixedFlush to defer fixed flush")
		}
		if err := j.Sync(); err != nil {
			_ = j.Close()
			t.Fatal(err)
		}
		segPath := j.Path()
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(segPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.ReadCommandFrameV2()
		_ = r.Close()
		if err != nil {
			t.Fatalf("ReadCommand fixed: %v", err)
		}
		if got.LSN != lsn || !bytes.Equal(got.Payload, canonPayload) || len(got.Preconditions) != 1 || len(got.ResultAssertions) != 1 {
			t.Fatalf("fixed replay mismatch: lsn=%d pre=%d assert=%d", got.LSN, len(got.Preconditions), len(got.ResultAssertions))
		}
	})
}

func TestFixedWALDeferFlushAndRotationNamespaceDebt(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	// Fill the initial 4 MiB fixed container with a 3 MiB frame, then append a
	// second 3 MiB frame with DeferFixedFlush + DeferRotationNamespace on a
	// Durable-class frame to force a rotation whose parent-directory sync is
	// retained on activeToken.Namespace.
	docVal := bytes.Repeat([]byte("x"), 3<<20)
	payload, err := EncodeCollectionInsertBatchByIDPayload("docs", []CollectionDocument{
		{ID: []byte("doc-rot"), Document: docVal},
	})
	if err != nil {
		t.Fatal(err)
	}
	payloadCRC := ChecksumCommandPayload(payload)
	env := CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindCollectionInsertBatchByID,
		Scope:           CommandScopeCollection,
		PayloadFormat:   PayloadFormatCollectionInsertBatchByIDV1,
		Payload:         payload,
	}

	opts := CommandAppendOptions{
		ImmutableHooks:         true,
		TrustedPayload:         true,
		DeferFixedFlush:        true,
		DeferRotationNamespace: true,
		PayloadCRC:             payloadCRC,
		HasPayloadCRC:          true,
	}
	lsn1, def1, err := j.AppendCommandObservedWithOptions(env, opts, nil, nil)
	if err != nil || !def1 {
		t.Fatalf("append 1 lsn=%d def=%v err=%v", lsn1, def1, err)
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}

	var capturedRotations []*CommandJournalStableRotation
	lsn2, def2, err := j.AppendCommandObservedWithOptions(env, opts, nil, func(_ uint64, rotations []*CommandJournalStableRotation) error {
		capturedRotations = append(capturedRotations, rotations...)
		return nil
	})
	if err != nil || !def2 {
		t.Fatalf("append 2 lsn=%d def=%v err=%v", lsn2, def2, err)
	}
	defer func() {
		for _, rot := range capturedRotations {
			rot.Release()
		}
	}()
	if len(capturedRotations) != 1 {
		t.Fatalf("rotations=%d want 1", len(capturedRotations))
	}
	if capturedRotations[0].Active == nil || capturedRotations[0].Active.Namespace() == nil {
		t.Fatal("expected deferred rotation to retain unstabilized Active.Namespace obligation")
	}
	// Stabilize the deferred namespace and sync the active segment as
	// CommandWALCapturedPrefix.Sync does in production.
	if err := capturedRotations[0].Active.Namespace().Stabilize(); err != nil {
		t.Fatalf("stabilize deferred rotation namespace: %v", err)
	}
	if err := j.Sync(); err != nil {
		t.Fatalf("sync rotated segment: %v", err)
	}
	seg2Path := j.Path()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(seg2Path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadCommandFrameV2()
	_ = r.Close()
	if err != nil || got.LSN != lsn2 || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("replay rotated segment lsn=%d want=%d err=%v", got.LSN, lsn2, err)
	}
}

func TestFixedWALMaxIncomingSuppressesBulk64MiBAhead(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	// Step into a 32 MiB fixed container with 3 MiB bulk frames (>= fixedWALBulkSteppingMinIncoming).
	bigFrame := fixedWALLifecycleFrame(t, 3<<20)
	// First append into 4 MiB, second rotates to 32 MiB via bulk stepping.
	for i := 0; i < 7; i++ {
		if _, err := j.AppendCommand(bigFrame); err != nil {
			t.Fatal(err)
		}
		if err := j.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	j.mu.Lock()
	capBytes := j.writer.fixed.capacity
	maxInc := j.writer.fixed.maxIncoming
	aheadBefore := j.fixedAhead
	j.mu.Unlock()
	if capBytes < 32<<20 {
		t.Fatalf("capacity=%d want >= 32 MiB", capBytes)
	}
	if maxInc < fixedWALBulkSteppingMinIncoming {
		t.Fatalf("maxIncoming=%d want >= %d", maxInc, fixedWALBulkSteppingMinIncoming)
	}
	if aheadBefore != nil {
		t.Fatal("unexpected speculative 64 MiB .wal-ahead reserve spawned for multi-MiB bulk workload")
	}

	// Now append a tiny 72-byte barrier frame while >50% full: maxIncoming must
	// still suppress spawning a wasted 64 MiB .wal-ahead reserve.
	smallBarrier := CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindDurablePrefixBarrier,
		Scope:           CommandScopeSystem,
		PayloadFormat:   PayloadFormatDurablePrefixBarrierV1,
	}
	if _, err := j.AppendCommand(smallBarrier); err != nil {
		t.Fatal(err)
	}
	if err := j.Flush(); err != nil {
		t.Fatal(err)
	}
	j.mu.Lock()
	aheadAfter := j.fixedAhead
	j.mu.Unlock()
	if aheadAfter != nil {
		t.Fatal("trailing small barrier on 32 MiB bulk container spawned unwanted 64 MiB .wal-ahead reserve")
	}
}
