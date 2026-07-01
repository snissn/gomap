package collections

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnPhysicalQueryAttachPreparedPayloadsBatchesContiguousSelectedBlocks(t *testing.T) {
	data := []byte("aaabbbbXccddeee")
	partColumn := typedcolumn.ColumnPartColumn{
		Definition: typedcolumn.ColumnDefinition{Name: "c"},
		Blocks: []typedcolumn.ColumnBlock{
			{Granule: typedcolumn.EncodedGranule{RawBytes: 3}},
			{Granule: typedcolumn.EncodedGranule{RawBytes: 4}},
			{Granule: typedcolumn.EncodedGranule{RawBytes: 2}},
			{Granule: typedcolumn.EncodedGranule{RawBytes: 5}},
		},
	}
	prepared := &typedColumnPreparedPartState{
		Columns: map[string]*typedColumnPreparedColumnState{
			"c": {
				Column: partColumn,
				BlockPlans: []typedColumnPreparedBlockPlan{
					{Index: 0, PayloadOffset: 10, PayloadLength: 3},
					{Index: 1, PayloadOffset: 13, PayloadLength: 4},
					{Index: 2, PayloadOffset: 18, PayloadLength: 2},
					{Index: 3, PayloadOffset: 20, PayloadLength: 5},
				},
			},
		},
	}
	adapterPart := &typedColumnAdapterPart{
		Part: &typedcolumn.ColumnPart{
			Columns: map[string]typedcolumn.ColumnPartColumn{"c": partColumn},
		},
	}

	type readCall struct {
		offset int
		length int
	}
	var calls []readCall
	readRange := func(offset int, length int, section bool) ([]byte, error) {
		if section {
			t.Fatalf("payload attach read unexpectedly marked section")
		}
		calls = append(calls, readCall{offset: offset, length: length})
		start := offset - 10
		return data[start : start+length], nil
	}

	decodedBytes, decodedBlocks, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, "c", "test", []bool{true, true, true, true}, readRange)
	if err != nil {
		t.Fatalf("attach prepared payloads: %v", err)
	}
	if decodedBytes != 14 || decodedBlocks != 4 {
		t.Fatalf("decoded bytes/blocks=%d/%d want 14/4", decodedBytes, decodedBlocks)
	}
	wantCalls := []readCall{{offset: 10, length: 7}, {offset: 18, length: 7}}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("read calls=%+v want %+v", calls, wantCalls)
	}
	gotColumn := adapterPart.Part.Columns["c"]
	gotPayloads := []string{
		string(gotColumn.Blocks[0].Granule.Payload),
		string(gotColumn.Blocks[1].Granule.Payload),
		string(gotColumn.Blocks[2].Granule.Payload),
		string(gotColumn.Blocks[3].Granule.Payload),
	}
	wantPayloads := []string{"aaa", "bbbb", "cc", "ddeee"}
	if !reflect.DeepEqual(gotPayloads, wantPayloads) {
		t.Fatalf("payloads=%q want %q", gotPayloads, wantPayloads)
	}
	for blockIdx, block := range gotColumn.Blocks {
		if block.Granule.PayloadRef.Kind != typedcolumn.PayloadRefInline || block.Granule.PayloadRef.Length != len(block.Granule.Payload) {
			t.Fatalf("block %d payload ref=%+v payload_len=%d", blockIdx, block.Granule.PayloadRef, len(block.Granule.Payload))
		}
	}
}
