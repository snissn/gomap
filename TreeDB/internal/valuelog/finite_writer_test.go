package valuelog

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/page"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestFiniteWriterLoanRetainsBackingAndAuthority(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "value.log"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.SetBlockCompression(BlockCodecLZ4, true)
	historical := []byte("retained prior caller")
	w.rawWritevIovs = make([][]byte, 0, 3)
	w.rawWritevIovs[:cap(w.rawWritevIovs)][2] = historical
	var charged uint64
	backing, err := NewFiniteWriterBacking(2, func(n uint64) error {
		if n > math.MaxUint64-charged {
			return ErrFiniteWriterLoan
		}
		charged += n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	loan, err := w.BeginFiniteWriterLoan(backing, 1, page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range w.rawWritevIovs[:cap(w.rawWritevIovs)] {
		if alias != nil {
			t.Fatal("loan retained historical writev tail alias")
		}
	}
	data := bytes.Repeat([]byte{0x61}, page.PageSize)
	ptr, _, err := w.AppendOneFrameWithStats(0, nil, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	if ptr.FileID != 1 || ptr.Offset == 0 {
		t.Fatal("missing persistent pointer")
	}
	if backing.Close() == nil {
		t.Fatal("closed ledger during active loan")
	}
	if _, err = w.BeginFiniteWriterLoan(backing, 1, page.PageSize); err == nil {
		t.Fatal("overlapping loan")
	}
	if err = w.Flush(); err == nil {
		t.Fatal("serializer mutated active loan backing")
	}
	capBefore := cap(w.appendBuf)
	pending := bytes.Clone(w.appendBuf)
	amount := charged
	if err = loan.Close(); err != nil {
		t.Fatal(err)
	}
	if cap(w.appendBuf) != capBefore || !bytes.Equal(w.appendBuf, pending) {
		t.Fatal("close changed pending persistent bytes")
	}
	loan, err = w.BeginFiniteWriterLoan(backing, 1, page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	afterLoan := charged
	if _, _, err = w.AppendOneFrameWithStats(0, nil, 2, data); err != nil {
		t.Fatal(err)
	}
	if charged != afterLoan {
		t.Fatal("warm append allocated/charged duplicate persistent backing")
	}
	if afterLoan <= amount {
		t.Fatal("new scoped loan wrapper was not charged")
	}
	before := w.Size()
	if _, _, err = w.AppendOneFrameWithStats(1, []byte("dictionary"), 3, data); err == nil {
		t.Fatal("unsupported dictionary")
	}
	if _, _, err = w.AppendOneFrameWithStats(0, nil, 3, append(data, 0)); err == nil {
		t.Fatal("oversize value")
	}
	if _, _, err = w.AppendRawFramesWritevInto([]Record{{RID: 3, Value: data}}, 1, make([]page.ValuePtr, 1)); err == nil {
		t.Fatal("unbounded writev route")
	}
	if w.Size() != before {
		t.Fatal("refused shape changed physical output")
	}
	if err = loan.Close(); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = backing.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.BeginFiniteWriterLoan(backing, 1, page.PageSize); err == nil {
		t.Fatal("closed request reused")
	}
}

func TestFiniteWriterLoanReservationFailurePrecedesGrowth(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "value.log"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	reject := false
	denied := errors.New("credit denied")
	b, err := NewFiniteWriterBacking(1, func(uint64) error {
		if reject {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	l, err := w.BeginFiniteWriterLoan(b, 1, page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	reject = true
	before := w.Size()
	oldScratch, oldAppend := cap(w.scratch), cap(w.appendBuf)
	if _, _, err = w.AppendOneFrameWithStats(0, nil, 1, []byte("a")); !errors.Is(err, denied) {
		t.Fatalf("wrong refusal: %v", err)
	}
	if w.Size() != before || cap(w.scratch) != oldScratch || cap(w.appendBuf) != oldAppend {
		t.Fatal("allocation/output preceded credit")
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFiniteWriterConstructorRefusesBeforeFileCreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-created.log")
	denied := errors.New("denied constructor")
	calls := 0
	b, err := NewFiniteWriterBacking(1, func(uint64) error {
		calls++
		if calls > 1 {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewWriterWithFiniteBacking(path, 1, nil, b); !errors.Is(err, denied) {
		t.Fatalf("wrong refusal: %v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file existed before constructor admission")
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}

// The owned route retains the existing frame partition and adaptive writev
// choice. Compare complete physical records and pointers, including tiny
// batches which naturally choose the ordinary contiguous fallback.
func TestFiniteWriterRawBatchMatchesOrdinary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		count    int
		buffered bool
	}{
		{"writev", 8, false}, {"fallback", 2, false}, {"buffered", 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := []string{filepath.Join(t.TempDir(), "owned.log"), filepath.Join(t.TempDir(), "ordinary.log")}
			writers := make([]*Writer, 2)
			for i := range writers {
				var err error
				writers[i], err = NewWriter(paths[i], 1)
				if err != nil {
					t.Fatal(err)
				}
				defer writers[i].Close()
			}
			var charged uint64
			backing, err := NewFiniteWriterBacking(1, func(n uint64) error { charged += n; return nil })
			if err != nil {
				t.Fatal(err)
			}
			loan, err := writers[0].BeginFiniteWriterLoan(backing, tc.count, tc.count*page.PageSize)
			if err != nil {
				t.Fatal(err)
			}
			records := make([]Record, tc.count)
			for i := range records {
				records[i] = Record{RID: uint64(i + 1), Value: bytes.Repeat([]byte{byte(i + 1)}, page.PageSize)}
			}
			results := make([][]page.ValuePtr, 2)
			for i, w := range writers {
				dst := make([]page.ValuePtr, len(records))
				if tc.buffered {
					results[i], _, err = w.AppendRawFramesBufferedInto(records, 3, dst)
				} else {
					results[i], _, err = w.AppendRawFramesWritevInto(records, 3, dst)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for i := range records {
				if results[0][i] != results[1][i] {
					t.Fatal("changed pointer or frame partition")
				}
			}
			if charged != backing.BackingBytes() {
				t.Fatal("backing debit mismatch")
			}
			for _, alias := range writers[0].rawWritevIovs[:cap(writers[0].rawWritevIovs)] {
				if alias != nil {
					t.Fatal("writev retained caller alias")
				}
			}
			if err = loan.Close(); err != nil {
				t.Fatal(err)
			}
			for _, w := range writers {
				if err = w.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			a, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(paths[1])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(a, b) {
				t.Fatal("changed full physical raw record bytes")
			}
			if err = backing.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestFiniteWriterRawBatchCreditBeforeOutput(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "raw.log"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	reject := false
	denied := errors.New("raw credit denied")
	backing, err := NewFiniteWriterBacking(1, func(uint64) error {
		if reject {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	loan, err := w.BeginFiniteWriterLoan(backing, 8, 8*page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	reject = true
	records := make([]Record, 8)
	for i := range records {
		records[i] = Record{RID: uint64(i + 1), Value: bytes.Repeat([]byte{1}, page.PageSize)}
	}
	before := w.Size()
	if _, _, err = w.AppendRawFramesWritevInto(records, 3, make([]page.ValuePtr, 8)); !errors.Is(err, denied) {
		t.Fatalf("refusal: %v", err)
	}
	if w.Size() != before || cap(w.appendBuf) != 0 || cap(w.rawWritevIovs) != 0 || cap(w.rawWritevMeta) != 0 {
		t.Fatal("growth/output before credit")
	}
	if err = loan.Close(); err != nil {
		t.Fatal(err)
	}
	if err = backing.Close(); err != nil {
		t.Fatal(err)
	}
}
