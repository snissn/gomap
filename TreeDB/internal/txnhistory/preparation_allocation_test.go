package txnhistory

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/batch"
	"testing"
)

func TestPreparationAllocationSizesMatchCanonicalArenaPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []batch.Entry
		native  []Effect
		want    uint64
	}{
		{"inline", []batch.Entry{{Key: []byte("short"), Type: batch.OpPut}}, nil, 0},
		{"long-singleton", []batch.Entry{{Key: bytes.Repeat([]byte("k"), 33), Type: batch.OpPut}}, nil, 33},
		{"multi", []batch.Entry{{Key: []byte("one"), Type: batch.OpPut}, {Key: []byte("two"), Type: batch.OpPut}}, nil, 6},
		{"effect-collapse", make([]batch.Entry, 9), nil, 0},
		{"byte-collapse", []batch.Entry{{Key: bytes.Repeat([]byte("k"), 2049), Type: batch.OpPut}}, nil, 0},
		{"native", nil, []Effect{{Kind: PointEffect, Key: []byte("semantic")}}, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls [][5]uint64
			j, err := New(8, 4096, func(a, b, c, d, e uint64) (Lease, error) {
				calls = append(calls, [5]uint64{a, b, c, d, e})
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			calls = nil
			sizes := PreparationAllocationSizes(8, 4096, tc.entries, nil, false, tc.native)
			if sizes != ([5]uint64{0, 0, tc.want, 0, 0}) {
				t.Fatalf("wrong arena policy: got=%v want=%d", sizes, tc.want)
			}
			prepared, err := j.PrepareEffects(tc.entries, nil, false, tc.native)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Cancel()
			if tc.want == 0 {
				if len(calls) != 0 {
					t.Fatalf("non-arena branch allocated: %v", calls)
				}
			} else if len(calls) != 1 || calls[0] != sizes {
				t.Fatalf("canonical arena admission differs: calls=%v sizes=%v", calls, sizes)
			}
		})
	}
}
