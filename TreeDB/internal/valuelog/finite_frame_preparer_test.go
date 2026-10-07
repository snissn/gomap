package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestFiniteFramePreparerMatchesOrdinaryAndOwnsBody(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecLZ4, BlockCodecSnappy} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			var debit uint64
			owner, err := NewFiniteFramePreparer(2, 2*page.PageSize, func(n uint64) error { debit += n; return nil })
			if err != nil {
				t.Fatal(err)
			}
			records := []Record{{RID: 1, Value: bytes.Repeat([]byte("a"), page.PageSize)}, {RID: 2, Value: bytes.Repeat([]byte("b"), page.PageSize)}}
			body, stats, err := owner.Prepare(records, codec, 0, 0, DefaultKeepSafetyMargin, true)
			if err != nil {
				t.Fatal(err)
			}
			ordinary := NewFramePreparer()
			ordinary.SetBlockCompression(codec, true)
			ordinary.SetKeepPolicy(0, 0, DefaultKeepSafetyMargin)
			ordinary.SetEncodeSampleStride(0)
			expected, expectedStats, err := ordinary.PrepareFrameInto(nil, 0, nil, records)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body, expected) || stats.Records != expectedStats.Records || stats.RawPayloadBytes != expectedStats.RawPayloadBytes || stats.StoredPayloadBytes != expectedStats.StoredPayloadBytes || stats.Kept != expectedStats.Kept {
				t.Fatal("owned preparer changed ordinary codec/frame semantics")
			}
			if owner.Close() == nil {
				t.Fatal("closed borrowed body")
			}
			if _, _, err = owner.Prepare(records, codec, 0, 0, DefaultKeepSafetyMargin, true); err == nil {
				t.Fatal("overlapping prepared body")
			}
			clear(records[0].Value)
			if !bytes.Equal(body, expected) {
				t.Fatal("body retained caller source alias")
			}
			owner.ReleaseBody()
			before := debit
			if _, _, err = owner.Prepare(records, codec, 0, 0, DefaultKeepSafetyMargin, true); err != nil {
				t.Fatal(err)
			}
			if debit != before || debit != owner.BackingBytes() {
				t.Fatal("warm owned preparation acquired new backing")
			}
			owner.ReleaseBody()
			if err = owner.Close(); err != nil {
				t.Fatal(err)
			}
			if owner.body != nil || owner.preparer.rawScratch != nil || owner.preparer.blockScratch != nil {
				t.Fatal("closed preparer retained backing")
			}
		})
	}
}
func TestFiniteFramePreparerRefusesCreditBeforeAllocation(t *testing.T) {
	reject := false
	denied := errors.New("denied")
	owner, err := NewFiniteFramePreparer(1, page.PageSize, func(uint64) error {
		if reject {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reject = true
	if _, _, err = owner.Prepare([]Record{{RID: 1, Value: []byte("input")}}, BlockCodecLZ4, 0, 0, DefaultKeepSafetyMargin, true); !errors.Is(err, denied) {
		t.Fatalf("credit refusal: %v", err)
	}
	if cap(owner.body) != 0 || cap(owner.preparer.blockScratch) != 0 || owner.borrowed {
		t.Fatal("allocation preceded credit")
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
}
