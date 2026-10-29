package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func nativeTextRecordTestState() textDocumentStateValue {
	return textDocumentStateValue{Fields: []textDocumentFieldState{{Field: "body", Length: 2, Terms: []textDocumentTermState{{Term: "term", Frequency: 2, Positions: []uint32{0, 1}, Offsets: []textTokenOffset{{Start: 0, End: 4}, {Start: 5, End: 9}}}}}}}
}

// Exercise the original finite parent at every allocation boundary, including
// later failure after earlier successful admissions. No receipt is refunded on
// failure; closing the unrelated pressure permits an owned decode to succeed.
func TestNativeTextStateCodecOriginalLeaseRefusal(t *testing.T) {
	state := nativeTextRecordTestState()
	raw := encodeTextDocumentStateValue(state)
	charges := []uint64{
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textDocumentFieldState{}))),
		memtable.COWAllocationCharge(uint64(len("body"))),
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textDocumentTermState{}))),
		memtable.COWAllocationCharge(uint64(len("term"))),
		memtable.COWAllocationCharge(2 * uint64(unsafe.Sizeof(uint32(0)))),
		memtable.COWAllocationCharge(2 * uint64(unsafe.Sizeof(textTokenOffset{}))),
	}
	for refused := range charges {
		t.Run(fmt.Sprint(refused), func(t *testing.T) {
			limits := memtable.DefaultCOWLimits()
			budget, err := memtable.NewCOWBudget(limits)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := budget.AcquireExternal(0)
			if err != nil {
				budget.Close()
				t.Fatal(err)
			}
			pressure, err := budget.AcquireExternal(0)
			if err != nil {
				lease.Close()
				budget.Close()
				t.Fatal(err)
			}
			defer func() {
				pressure.Close()
				lease.Close()
				budget.Close()
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Errorf("Close leaked: %+v", stats)
				}
			}()
			var prefix uint64
			for _, n := range charges[:refused] {
				prefix += n
			}
			if err := pressure.Resize(limits.MaxInFlightBytes - budget.Stats().ReservedBytes - prefix); err != nil {
				t.Fatal(err)
			}
			var charged uint64
			calls := 0
			admit := func(n uint64) error {
				calls++
				if n > ^uint64(0)-charged {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charged + n); err != nil {
					return err
				}
				charged += n
				return nil
			}
			got, err := decodeTextDocumentStateValueAdmitted(raw, admit)
			if !errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, ErrTextIndexStorageCorrupt) || got.Fields != nil {
				t.Fatalf("refusal returned state=%+v err=%v", got, err)
			}
			if charged != prefix || calls != refused+1 || budget.Stats().ReservedBytes != limits.MaxInFlightBytes {
				t.Fatalf("prefix changed: charged=%d want=%d calls=%d stats=%+v", charged, prefix, calls, budget.Stats())
			}
			pressure.Close()
			got, err = decodeTextDocumentStateValueAdmitted(raw, admit)
			if err != nil || !reflect.DeepEqual(got, state) {
				t.Fatalf("retry state=%+v err=%v", got, err)
			}
			clear(raw)
			if !reflect.DeepEqual(got, state) {
				t.Fatal("decoded state aliases source")
			}
			raw = encodeTextDocumentStateValue(state)
		})
	}
}

func TestNativeTextStateCodecMalformedAndParity(t *testing.T) {
	state := nativeTextRecordTestState()
	raw := encodeTextDocumentStateValue(state)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AcquireExternal(0)
	if err != nil {
		budget.Close()
		t.Fatal(err)
	}
	defer func() {
		lease.Close()
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("Close leaked: %+v", stats)
		}
	}()
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	for end := 0; end < len(raw); end++ {
		_, oldErr := decodeTextDocumentStateValue(raw[:end])
		got, err := decodeTextDocumentStateValueAdmitted(raw[:end], admit)
		if !errors.Is(oldErr, ErrTextIndexStorageCorrupt) || !errors.Is(err, ErrTextIndexStorageCorrupt) || err.Error() != oldErr.Error() || got.Fields != nil {
			t.Fatalf("truncation%d state=%+v old=%v new=%v", end, got, oldErr, err)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(raw), 0), appendTextUvarint([]byte{textStateValueVersion}, ^uint64(0))} {
		if got, err := decodeTextDocumentStateValueAdmitted(bad, admit); !errors.Is(err, ErrTextIndexStorageCorrupt) || got.Fields != nil {
			t.Fatalf("bad state=%+v err=%v", got, err)
		}
	}
	got, err := decodeTextDocumentStateValueAdmitted(raw, admit)
	legacy, oldErr := decodeTextDocumentStateValue(raw)
	if err != nil || oldErr != nil || charged == 0 || !reflect.DeepEqual(got, legacy) {
		t.Fatalf("parity state=%+v err=%v old=%v", got, err, oldErr)
	}
	before := charged
	badCount := appendTextUvarint([]byte{textStateValueVersion}, ^uint64(0))
	if _, err := decodeTextDocumentStateValueAdmitted(badCount, admit); !errors.Is(err, ErrTextIndexStorageCorrupt) || charged != before {
		t.Fatalf("malformed count admitted before bounds: %v", err)
	}
}

func TestNativeTextEmptyStopWordsValidateBeforeNoStorage(t *testing.T) {
	denied := errors.New("unexpected storage")
	admit := func(uint64) error { return denied }
	if words, err := nativeTextStopWords(TextAnalyzerSimple, &TextAnalyzerOptions{}, admit); err != nil || len(words) != 0 {
		t.Fatalf("empty declaration words=%v err=%v", words, err)
	}
	for _, opts := range []*TextAnalyzerOptions{{Stemmer: "unsupported"}, {Synonyms: map[string][]string{"a": {"b"}}}} {
		if _, err := nativeTextStopWords(TextAnalyzerSimple, opts, admit); !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("validation skipped: %v", err)
		}
	}
}
