package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWCanonicalMetadataIdentityAndRefusal(t *testing.T) {
	ptr := page.ValuePtr{FileID: 7, Offset: 128}
	entries := []batch.Entry{{Type: batch.OpPut, Key: []byte("inline"), Value: []byte("v"), Revision: 19},
		{Type: batch.OpPut, Key: []byte("external"), IsPtr: true, ValuePtr: ptr, Revision: 20},
		{Type: batch.OpPut, Key: []byte("materialized"), Value: []byte("owned"), IsPtr: true, ValuePtr: ptr, Revision: 21},
		{Type: batch.OpDelete, Key: []byte("deleted"), Revision: 22}}
	ops := []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: entries[0].Key, Value: entries[0].Value, Revision: 19},
		{Op: commitlog.RawKVOpSetRID, Key: entries[1].Key, RID: 42, Revision: 20},
		{Op: commitlog.RawKVOpSetMaterializedRID, Key: entries[2].Key, Value: entries[2].Value, RID: 42, Revision: 21},
		{Op: commitlog.RawKVOpDelete, Key: entries[3].Key, Revision: 22}}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(p page.ValuePtr) (uint64, bool) { return 42, p == ptr }
	if err := validateCOWCanonicalEntries(payload, entries, lookup); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		alter func()
	}{{"revision", func() { entries[0].Revision++ }},
		{"pointer", func() { entries[1].ValuePtr.Offset++ }},
		{"materialized", func() { entries[2].Value = []byte("other") }},
		{"delete", func() { entries[3].Type = batch.OpPut }}} {
		t.Run(test.name, func(t *testing.T) {
			saved := append([]batch.Entry(nil), entries...)
			test.alter()
			if err := validateCOWCanonicalEntries(payload, entries, lookup); err == nil {
				t.Fatal("mismatch accepted")
			}
			copy(entries, saved)
		})
	}
	if err := validateCOWCanonicalEntries(payload, entries, nil); err == nil {
		t.Fatal("missing metadata authority accepted")
	}
	if err := validateCOWCanonicalEntries(payload, entries[:3], lookup); err == nil {
		t.Fatal("count mismatch accepted")
	}
}
