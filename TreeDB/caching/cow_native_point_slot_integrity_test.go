//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowNativeSlotExternalLeafFixture(t *testing.T, codec int) (*DB, *backenddb.DB, *NativeSemanticCapture, uint64, page.LeafLogPtr, []byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: 128})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	compression := vlogCompressionBlock
	if codec < 0 {
		compression = vlogCompressionOff
	}
	db, err := Open(dir, backend, Options{TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256, ValueLogCompression: uint8(compression), ValueLogBlockCodec: uint8(max(codec, 0))})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("native-slot-leaf"), 8)
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	t.Cleanup(func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	})
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing captured root")
	}
	var key, leaf [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("actual external leaf")
	_, err = capture.Snapshot().GetEntryExactWithFixedScratch([]byte("a000"), key[:], leaf[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) { ptr = p; return nil, stop })
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		t.Fatal("fixture did not reach real external leaf", ptr, err)
	}
	f, _, err := capture.Snapshot().PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), db.cow.readLimits())
	codecs := []valuelog.BlockCodec{valuelog.BlockCodecSnappy, valuelog.BlockCodecLZ4, valuelog.BlockCodecZSTD}
	if err != nil || shape.Compressed != (codec >= 0) || (codec >= 0 && shape.Codec != codecs[codec]) {
		t.Fatalf("actual codec=%d shape=%+v err=%v", codec, shape, err)
	}
	return db, backend, capture, token.RootPageID, ptr, want
}

func TestCOWNativeEntrySlotExternalLeafIntegrity(t *testing.T) {
	for codec := -1; codec <= 2; codec++ {
		t.Run(fmt.Sprintf("codec=%d", codec), func(t *testing.T) {
			db, backend, capture, root, ptr, want := cowNativeSlotExternalLeafFixture(t, codec)
			basis := capture.cut.basis
			read := func() (node.LeafEntry, error) {
				return cowNativeEntryUsingExtra(t, basis, capture, root, []byte("a000"), true)
			}
			if codec == 2 {
				if _, err := read(); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
					t.Fatal("native dictionary-free ZSTD policy", err)
				}
				if basis.reader != nil || basis.readSlots[1].reader != nil {
					t.Fatal("native codec refusal retried another owner")
				}
				// A generic read's verified cache cannot grant native codec eligibility.
				if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
					t.Fatal("generic ZSTD control", err)
				}
				if _, err := read(); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
					t.Fatal("verified cache bypassed native codec policy", err)
				}
				return
			}
			f, _, err := capture.Snapshot().PinnedValueLogFile(ptr.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			restore := func() {
				t.Helper()
				if _, err := writer.WriteAt(original, 0); err != nil {
					t.Fatal(err)
				}
				if err := writer.Truncate(int64(len(original))); err != nil {
					t.Fatal(err)
				}
			}
			defer restore()
			crc := int64(ptr.Offset - 4)
			if _, err := writer.WriteAt([]byte{original[crc] ^ 1}, crc); err != nil {
				t.Fatal(err)
			}
			if _, err := read(); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatal("cold CRC bypassed", err)
			}
			if basis.reader != nil || basis.readSlots[1].reader != nil {
				t.Fatal("real read failure retried another traversal owner")
			}
			restore()
			hits := cowLeafCacheCounter(t, backend, "hits")
			var saved []byte
			for i := 0; i < 4; i++ {
				e, err := read()
				if err != nil || !bytes.Equal(e.Value, want) || len(e.Value) != cap(e.Value) || e.Key != nil {
					t.Fatal("owned external inline", e, err)
				}
				if i == 0 {
					saved = e.Value
					saved[0] ^= 1
				}
			}
			if cowLeafCacheCounter(t, backend, "hits") <= hits || saved[0] == want[0] {
				t.Fatal("slot did not reuse verified cache or overwrote owned output")
			}
			// Inspection must precede a verified hit, including a replaced record header.
			at := int64(ptr.Offset)
			if _, err := writer.WriteAt([]byte{original[at] ^ 1}, at); err != nil {
				t.Fatal(err)
			}
			if _, err := read(); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatal("verified cache bypassed replaced record inspection", err)
			}
			restore()
			if err := writer.Truncate(at + 1); err != nil {
				t.Fatal(err)
			}
			if _, err := read(); err == nil {
				t.Fatal("verified cache bypassed truncated record")
			}
			restore()
			if e, err := read(); err != nil || !bytes.Equal(e.Value, want) {
				t.Fatal("restored physical record", e, err)
			}
		})
	}
}

type cowNativeSlotRefusingDictionary struct {
	calls *int
	err   error
}

func (p cowNativeSlotRefusingDictionary) PrepareDictionaryReadDefinition(uint64, valuelog.COWReadLimits, int, func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
	*p.calls++
	return nil, p.err
}

func TestCOWNativeEntrySlotPackedDictionaryErrors(t *testing.T) {
	db, backend, _, _, want := cowNativePackedDictionaryLeafFixture(t)
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	defer capture.Close()
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing packed root")
	}
	basis := capture.cut.basis
	original := capture.dictionary
	read := func() (node.LeafEntry, error) {
		return cowNativeEntryUsingExtra(t, basis, capture, token.RootPageID, []byte("pack-03000"), true)
	}
	capture.dictionary = nil
	if _, err := read(); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatal("slot lost original provider eligibility", err)
	}
	for _, refusal := range []error{memtable.ErrCOWCapacity, memtable.ErrCOWClosed, errCOWNativeDefinitionRefused} {
		calls := 0
		capture.dictionary = cowNativeSlotRefusingDictionary{calls: &calls, err: refusal}
		if _, err := read(); !errors.Is(err, refusal) || calls != 1 {
			t.Fatalf("real traversal/provider error retried: calls=%d err=%v want=%v", calls, err, refusal)
		}
	}
	capture.dictionary = original
	for i := 0; i < 4; i++ {
		e, err := read()
		if err != nil || !bytes.Equal(e.Value, want) {
			t.Fatal("slot original dictionary read", e, err)
		}
		e.Value[0] ^= 1
	}
	if basis.reader != nil || basis.readSlots[1].reader != nil || basis.readSlots[0].reader == nil {
		t.Fatal("dictionary traversal did not stay on selected extra owner")
	}
	if db.cow.physicalReader == nil || basis.readSlots[0].reader.snapshot != nil || len(basis.readSlots[0].reader.codecs) != 0 {
		t.Fatal("slot retained caller or duplicated shared dictionary decoder")
	}
}
