package collections

import "testing"

func TestCollectionMetaV1_RoundTripStable(t *testing.T) {
	meta := &CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: true,
		},
		Indexes: []IndexDefinition{
			{Name: "by_name", Field: "name", Unique: true},
			{Name: "by_email", Field: "contact.email"},
			{Name: "by_age", Field: "age", MultiKey: true},
		},
	}
	b, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded CollectionMeta
	if err := decoded.Decode(b); err != nil {
		t.Fatalf("decode: %v", err)
	}
	decoded2, err := decoded.Encode()
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if string(b) != string(decoded2) {
		t.Fatalf("stable encode mismatch")
	}

	if decoded.Name != "users" {
		t.Fatalf("name mismatch: %q", decoded.Name)
	}
	if len(decoded.Indexes) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(decoded.Indexes))
	}
}

func TestCollectionMeta_DefaultsAndFutureCompat(t *testing.T) {
	data := &CollectionMeta{
		Name: "users",
	}
	encoded, err := data.Encode()
	if err != nil {
		t.Fatalf("encode defaults payload: %v", err)
	}
	b := make([]byte, len(encoded))
	copy(b, encoded)

	var meta CollectionMeta
	if err := meta.Decode(b); err != nil {
		t.Fatalf("decode defaults: %v", err)
	}

	if meta.Version != collectionMetaVersion {
		t.Fatalf("version mismatch: %d", meta.Version)
	}
	if !meta.Options.RejectMissingFields {
		t.Fatalf("expected default RejectMissingFields=true")
	}

	b[0] = byte(collectionMetaVersion + 1)
	var future CollectionMeta
	if err := future.Decode(b); err == nil {
		t.Fatalf("expected future version rejection")
	}
}

func TestIndexDefCodec_RoundTrip(t *testing.T) {
	def := IndexDefinition{Name: "by_status", Field: "status", Unique: true}
	meta := &CollectionMeta{
		Name: "tickets",
		Indexes: []IndexDefinition{
			def,
		},
	}
	b, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded CollectionMeta
	if err := decoded.Decode(b); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(decoded.Indexes))
	}
	got := decoded.Indexes[0]
	if got.Name != def.Name || got.Field != def.Field || !got.Unique || got.MultiKey {
		t.Fatalf("index mismatch: %+v", got)
	}
}

func TestIndexDef_RejectAmbiguousPath(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "leading-dot", path: ".field"},
		{name: "trailing-dot", path: "field."},
		{name: "double-dot", path: "profile..email"},
		{name: "empty-segment", path: "profile.."},
		{name: "reserved-prefix", path: "$id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateIndexPath(tc.path); err == nil {
				t.Fatalf("expected error for %q", tc.path)
			}
		})
	}
}
