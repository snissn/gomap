package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Pure removal keeps old name-keyed state. A later mutation must consume the
// surviving name without ignoring corruption or admission of removed entries.
func TestNativeTypedIndexStateAfterDrop(t *testing.T) {
	value := []byte{1, 'v', 0}
	raw, err := encodeDocumentIndexState(documentIndexState{"a_removed": {value}, "z_keep": {value}})
	if err != nil {
		t.Fatal(err)
	}
	runtimes := []indexRuntime{{def: indexDefinition{name: "z_keep", valueType: IndexValueString}}}
	var encoder indexEncodeArena
	var charged uint64
	got, err := nativeTypedDecodeIndexState(raw, runtimes, &encoder, func(n uint64) error { charged += n; return nil })
	if err != nil || len(got) != 1 || len(got[0]) != 1 || !bytes.Equal(got[0][0], value) || charged == 0 {
		t.Fatalf("surviving typed state=%x charge=%d err=%v", got, charged, err)
	}
	var refused indexEncodeArena
	if _, err := nativeTypedDecodeIndexState(raw, runtimes, &refused, func(uint64) error { return memtable.ErrCOWCapacity }); !errors.Is(err, memtable.ErrCOWCapacity) || len(refused.states) != 0 {
		t.Fatalf("state refusal allocated output: states=%d err=%v", len(refused.states), err)
	}
	// The removed name must still be structurally validated. Its zero value
	// count is invalid even though only z_keep will be used by the final plan.
	broken := bytes.Clone(raw)
	broken[3+2+len("a_removed")+1] = 0
	for _, input := range [][]byte{broken, append(bytes.Clone(raw), 1)} {
		var scratch indexEncodeArena
		if _, err := nativeTypedDecodeIndexState(input, runtimes, &scratch, func(uint64) error { return nil }); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("malformed removed state accepted: %v", err)
		}
	}
	var missing indexEncodeArena
	if _, err := nativeTypedDecodeIndexState(raw, []indexRuntime{{def: indexDefinition{name: "missing"}}}, &missing, func(uint64) error { return nil }); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("missing surviving runtime accepted: %v", err)
	}
}
