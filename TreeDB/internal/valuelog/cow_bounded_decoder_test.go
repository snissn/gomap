package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/snissn/compress/huff0"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func cowBoundedTestOwner(t *testing.T, raw, input uint64) (*memtable.COWBudget, *memtable.COWExternalLease, uint64) {
	t.Helper()
	sizes, err := COWBoundedDecoderRetentionSizes(raw, input, max(raw, 2048))
	if err != nil {
		t.Fatal(err)
	}
	base := retainedalloc.AllocationCharge(sizes.OwnerWrapper) + retainedalloc.AllocationCharge(raw) + retainedalloc.AllocationCharge(input)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AcquireExternal(base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		lease.Close()
		budget.Close()
		if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
			t.Errorf("terminal ownership: %+v", s)
		}
	})
	return budget, lease, base
}

func TestCOWBoundedDecoderDictionaryOriginalReceiptAndReset(t *testing.T) {
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte("immutable-borrowed-definition-and-owned-result"), 128)
		samples[i][0] = byte(i)
	}
	def, err := buildBenchDict(317, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(def)
	id := binary.BigEndian.Uint64(digest[:8])
	budget, lease, base := cowBoundedTestOwner(t, 1<<20, 1<<20)
	d, err := NewCOWBoundedDecoder(id, def, &def, 1<<20, 1<<20, 1<<20, lease, base)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 2; i++ {
		body, header, err := EncodeFrame(id, def, []Record{{RID: 1, Value: samples[i]}})
		if err != nil {
			t.Fatal(err)
		}
		if header.Flags&FrameFlagCompressed == 0 {
			t.Fatal("fixture did not compress")
		}
		f, p, _ := cowTestRecord(t, body, true, 0)
		shape, err := InspectCOWRecord(f, p, cowTestLimits)
		if err != nil {
			t.Fatal(err)
		}
		if got := cowTestRead(t, f, p, shape, d.Decode); !bytes.Equal(got, samples[i]) {
			t.Fatal("dictionary result")
		}
		// The caller may overwrite/reuse both buffers immediately after Decode.
		// The following operation reuses the same validated immutable definition.
	}
	if d.work.retained == 0 || budget.Stats().ExternalLeases != 1 {
		t.Fatal("continuation did not retain SAME receipt")
	}
	d.Close()
	d.Close()
	if d.decoder != nil || d.dictionary.Owner != nil || d.work.retained != 0 {
		t.Fatal("refund before final alias detach")
	}
	if got := budget.Stats().ExternalBytes; got < base {
		t.Fatalf("caller base prematurely refunded: %d < %d", got, base)
	}
}

func TestCOWBoundedDecoderRefusalIsNotQuantumRetry(t *testing.T) {
	budget, lease, base := cowBoundedTestOwner(t, 1<<20, 1<<20)
	// A true original receipt refusal occurs before library control allocation.
	stats := budget.Stats()
	headroom := budget.Limits().MaxInFlightBytes - stats.ReservedBytes
	blocker, err := budget.AcquireExternal(headroom - 1024)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewCOWBoundedDecoder(0, nil, nil, 1<<20, 1<<20, 1<<20, lease, base)
	if d != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("constructor capacity: %p %v", d, err)
	}
	blocker.Close()
	d, err = NewCOWBoundedDecoder(0, nil, nil, 1<<20, 1<<20, 1<<20, lease, base)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// Reuse the bounded engine's maintained rich-literal input shape: mixed
	// low-alphabet literals and repeated 1KiB spans. The fastest encoder can
	// store pure random literals raw, which legitimately needs no Retain.
	// Prove actual growth before applying pressure to this exact frame.
	want := make([]byte, 96<<10)
	x := uint32(1)
	for i := range want {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		want[i] = byte(x & 63)
		if i%2048 >= 1024 {
			want[i] = want[i-1024]
		}
	}
	payload, err := encodeBlockPayload(BlockCodecZSTD, want, nil)
	if err != nil {
		t.Fatal(err)
	}
	cold := d.work.retained
	if got, err := d.Decode(FrameHeader{Version: FrameVersion, Flags: FrameFlagCompressed, K: 1, Reserved: byte(BlockCodecZSTD)}, payload, make([]byte, len(want))); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("unpressured exact-frame witness: %v", err)
	}
	if d.work.retained <= cold {
		t.Fatalf("fixture needs actual retained growth: cold=%d warm=%d", cold, d.work.retained)
	}
	d.Close()
	// Reconstruct only the cold continuation, then force that witnessed new
	// allocation to refuse on the SAME original receipt. Do not retry capacity.
	d, err = NewCOWBoundedDecoder(0, nil, nil, 1<<20, 1<<20, 1<<20, lease, base)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	stats = budget.Stats()
	headroom = budget.Limits().MaxInFlightBytes - stats.ReservedBytes
	blocker, err = budget.AcquireExternal(headroom - 1024)
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Decode(FrameHeader{Version: FrameVersion, Flags: FrameFlagCompressed, K: 1, Reserved: byte(BlockCodecZSTD)}, payload, make([]byte, len(want)))
	blocker.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) || !d.Closed() || d.work.retained != 0 {
		t.Fatalf("failed operation custody: closed=%v retained=%d err=%v", d.Closed(), d.work.retained, err)
	}
}

func TestCOWBoundedDecoderExactOutputConcurrentCloseAndProvenance(t *testing.T) {
	_, lease, base := cowBoundedTestOwner(t, 1<<20, 1<<20)
	invalid := []byte("not a complete dictionary")
	digest := sha256.Sum256(invalid)
	id := binary.BigEndian.Uint64(digest[:8])
	if d, err := NewCOWBoundedDecoder(id+1, invalid, &invalid, 1<<20, 1<<20, 1<<20, lease, base); d != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if d, err := NewCOWBoundedDecoder(id, invalid, &invalid, 1<<20, 1<<20, 1<<20, lease, base); d != nil || err == nil {
		t.Fatal("malformed original definition accepted")
	}
	d, err := NewCOWBoundedDecoder(0, nil, nil, 1<<20, 1<<20, 1<<20, lease, base)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("record"), 128)
	payload, err := encodeBlockPayload(BlockCodecZSTD, want, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame := FrameHeader{Version: FrameVersion, Flags: FrameFlagCompressed, K: 1, Reserved: byte(BlockCodecZSTD)}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]byte, len(want))
			got, err := d.Decode(frame, payload, dst)
			if err != nil && !errors.Is(err, ErrCOWDecoderClosed) {
				t.Error(err)
			}
			if err == nil && (!bytes.Equal(got, want) || &got[0] != &dst[0]) {
				t.Error("caller-owned exact output changed")
			}
		}()
	}
	d.Close()
	wg.Wait()
	if _, err := d.Decode(frame, payload, make([]byte, len(want))); !errors.Is(err, ErrCOWDecoderClosed) {
		t.Fatal(err)
	}
}

func TestCOWBoundedDecoderFiniteQuantumOverflowAndNoProgress(t *testing.T) {
	w := cowDecoderWork{}
	w.beginOperation()
	if err := w.nextQuantum(); err != nil {
		t.Fatal(err)
	}
	if w.Reserve(cowDecoderQuantumRecords+1, 1) {
		t.Fatal("oversized quantum unit accepted")
	}
	if err := w.nextQuantum(); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatalf("no-progress continuation: %v", err)
	}
	w.beginOperation()
	_ = w.nextQuantum()
	w.totalBytes = ^uint64(0)
	if w.Reserve(1, 1) || !errors.Is(w.err, ErrCOWReadCapacity) {
		t.Fatal("cumulative work overflow")
	}
}

func TestCOWBoundedDecoderConcatenatedFramesAndCallerBounds(t *testing.T) {
	_, lease, base := cowBoundedTestOwner(t, 1<<20, 1<<20)
	d, err := NewCOWBoundedDecoder(0, nil, nil, 1<<20, 1<<20, 1<<20, lease, base)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a := bytes.Repeat([]byte("first-frame"), 100)
	b := bytes.Repeat([]byte("second-frame"), 100)
	pa, err := encodeBlockPayload(BlockCodecZSTD, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := encodeBlockPayload(BlockCodecZSTD, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload := append(append([]byte(nil), pa...), pb...)
	want := append(append([]byte(nil), a...), b...)
	frame := FrameHeader{Version: FrameVersion, Flags: FrameFlagCompressed, K: 1, Reserved: byte(BlockCodecZSTD)}
	dst := make([]byte, len(want))
	got, err := d.Decode(frame, payload, dst)
	if err != nil || !bytes.Equal(got, want) || &got[0] != &dst[0] {
		t.Fatalf("concatenated exact output: %v", err)
	}
	if _, err := d.Decode(frame, make([]byte, (1<<20)+1), dst); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal("input cap", err)
	}
	frame.DictID = 1
	if _, err := d.Decode(frame, payload, dst); !errors.Is(err, ErrMissingDict) {
		t.Fatal("producer authentication", err)
	}
	frame.DictID = 0
	frame.Reserved = byte(BlockCodecLZ4)
	if _, err := d.Decode(frame, payload, dst); !errors.Is(err, ErrCOWReadUnsupported) {
		t.Fatal("adapter accepted another codec", err)
	}
}

// This follows the maintained cleanup API directly: an absent optional FSE
// advances a private phase with no Reserve. A false result is not by itself a
// refused work unit. Both cases were previously collapsed by the adapter.
func TestCOWBoundedDecoderCleanupPhaseYieldAndOversizedUnit(t *testing.T) {
	var table huff0.TableState
	w := cowDecoderWork{}
	w.beginOperation()
	sawPhaseOnly := false
	for {
		if err := w.nextQuantum(); err != nil {
			t.Fatalf("legal cleanup phase rejected: %v", err)
		}
		done, err := table.ClearStep(&w)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if w.quantum.Records == 0 && w.quantum.Bytes == 0 {
			sawPhaseOnly = true
		}
	}
	if !sawPhaseOnly {
		t.Fatal("maintained empty Huffman cleanup did not exercise phase-only yield")
	}
	// A different outcome: an actual requested unit cannot fit even a wholly
	// unused quantum. Keep finite refusal, no replenishment toward success.
	w.beginOperation()
	if err := w.nextQuantum(); err != nil {
		t.Fatal(err)
	}
	if w.Reserve(cowDecoderQuantumRecords+1, 1) {
		t.Fatal("oversized work accepted")
	}
	if err := w.nextQuantum(); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatalf("oversized fresh quantum was retried: %v", err)
	}
}
