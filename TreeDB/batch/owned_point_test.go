package batch

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
)

func TestOwnedPointBatchAdmissionAndTerminal(t *testing.T) {
	input := []Entry{{Key: []byte("a"), Value: []byte("first")}, {Key: []byte("b"), Type: OpDelete}}
	wantControl, _ := allocclass.ClassBytes(uint64(unsafe.Sizeof(Batch{})), true)
	wantEntries, _ := allocclass.ClassBytes(uint64(len(input))*uint64(unsafe.Sizeof(Entry{})), true)
	denied := errors.New("denied")
	calls := 0
	b, err := NewOwnedPointBatch(input, func(n uint64) error {
		calls++
		if calls == 1 && n != wantControl {
			t.Fatalf("control %d", n)
		}
		if calls == 2 {
			if n != wantEntries {
				t.Fatalf("entry class %d", n)
			}
			return denied
		}
		return nil
	})
	if !errors.Is(err, denied) || b != nil || calls != 2 {
		t.Fatalf("failed complete admission b=%v err=%v calls=%d", b, err, calls)
	}
	calls = 0
	if _, err = NewOwnedPointBatch([]Entry{input[1], input[0]}, func(uint64) error { calls++; return nil }); err == nil || calls != 0 {
		t.Fatal("malformed closure debited before validation")
	}
	b, err = NewOwnedPointBatch(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cap(b.entries) != len(input) || len(b.arenaChunks) != 0 {
		t.Fatal("owned plan borrowed spare backing")
	}
	input[0] = Entry{Key: []byte("replaced")}
	if string(b.SortedEntries()[0].Key) != "a" {
		t.Fatal("caller entry header mutation changed plan")
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	ordinary := Acquire(nil, 1024)
	defer ordinary.Close()
	if ordinary == b || b.entries != nil || b.lastKey != nil || !b.closed {
		t.Fatal("closed plan survived in a reusable pool")
	}
	if err = b.Set([]byte("old"), []byte("alias")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed mutation %v", err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}
