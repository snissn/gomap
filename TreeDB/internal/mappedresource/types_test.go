package mappedresource

import "testing"

func TestKeyEqualityDistinguishesIdentityFields(t *testing.T) {
	base := Key{
		Class:      ClassTypedColumnAsset,
		Namespace:  "vectors",
		Kind:       "column_part",
		Generation: 7,
		PartID:     11,
		FileID:     3,
		Offset:     128,
		Length:     64,
		Checksum:   99,
		Version:    2,
		Encoding:   "little_endian",
		Section:    Section{Kind: "column_data", Column: "embedding", Ordinal: 4},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("base Validate: %v", err)
	}
	if !base.Equal(base) {
		t.Fatal("base key not equal to itself")
	}
	cases := map[string]func(Key) Key{
		"class":      func(k Key) Key { k.Class = ClassTypedRowAsset; return k },
		"namespace":  func(k Key) Key { k.Namespace = "other"; return k },
		"kind":       func(k Key) Key { k.Kind = "other"; return k },
		"generation": func(k Key) Key { k.Generation++; return k },
		"part_id":    func(k Key) Key { k.PartID++; return k },
		"file_id":    func(k Key) Key { k.FileID++; return k },
		"offset":     func(k Key) Key { k.Offset++; return k },
		"length":     func(k Key) Key { k.Length++; return k },
		"checksum":   func(k Key) Key { k.Checksum++; return k },
		"version":    func(k Key) Key { k.Version++; return k },
		"encoding":   func(k Key) Key { k.Encoding = "other"; return k },
		"section":    func(k Key) Key { k.Section.Ordinal++; return k },
	}
	for name, mutate := range cases {
		if got := base.Equal(mutate(base)); got {
			t.Fatalf("key Equal did not distinguish %s", name)
		}
	}
}

func TestScopeValidationRejectsLifetimeLessRequests(t *testing.T) {
	key := Key{Class: ClassTypedRowAsset, Namespace: "events", FileID: 1, Offset: 0, Length: 8}
	if err := (Scope{}).ValidateForKey(key); err == nil {
		t.Fatal("empty scope ValidateForKey err=nil, want failure")
	}
	if err := (Scope{Kind: ScopeSnapshot}).ValidateForKey(key); err == nil {
		t.Fatal("scope without id ValidateForKey err=nil, want failure")
	}
	if err := (Scope{Kind: ScopeSnapshot, ID: "snap-1", Namespace: "other"}).ValidateForKey(key); err == nil {
		t.Fatal("namespace mismatch ValidateForKey err=nil, want failure")
	}
	if err := (Scope{Kind: ScopeSnapshot, ID: "snap-1", Namespace: "events"}).ValidateForKey(key); err != nil {
		t.Fatalf("valid snapshot scope ValidateForKey: %v", err)
	}
}

func TestKeyValidateRejectsUnsafeRanges(t *testing.T) {
	if err := (Key{Class: ClassTypedRowAsset, Namespace: "events", FileID: 1, Offset: -1, Length: 8}).Validate(); err == nil {
		t.Fatal("negative offset Validate err=nil, want failure")
	}
	if err := (Key{Class: ClassTypedRowAsset, Namespace: "events", FileID: 1, Offset: 0, Length: -1}).Validate(); err == nil {
		t.Fatal("negative length Validate err=nil, want failure")
	}
	if err := (Key{Class: ClassTypedRowAsset, Namespace: "", FileID: 1, Offset: 0, Length: 8}).Validate(); err == nil {
		t.Fatal("typed row key without namespace Validate err=nil, want failure")
	}
	if err := (Key{Class: ClassTypedRowAsset, Namespace: "events", FileID: 0, Offset: 0, Length: 8}).Validate(); err == nil {
		t.Fatal("typed row key without file id Validate err=nil, want failure")
	}
}
