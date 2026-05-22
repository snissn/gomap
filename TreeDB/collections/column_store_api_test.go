package collections

import (
	"errors"
	"testing"
)

// Tests for rejectCreateIndexOnRetainedColumnField (added in this PR).

func TestRejectCreateIndexOnRetainedColumnFieldNilConfigAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{ColumnStore: nil},
	}
	def := IndexDefinition{Field: "email"}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for nil ColumnStore, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldDisabledAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: false,
			Columns: []ColumnStoreColumn{{Name: "email", Path: "email"}},
		}},
	}
	def := IndexDefinition{Field: "email"}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for disabled ColumnStore, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldFullRetainedPayloadAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadFull,
			Columns:         []ColumnStoreColumn{{Name: "time_us", Path: "time_us"}},
		}},
	}
	def := IndexDefinition{Field: "time_us"}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for full retained payload, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldEmptyFieldAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNone,
		}},
	}
	// Empty field should be allowed even with None payload.
	def := IndexDefinition{Field: ""}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for empty field, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldWhitespaceOnlyFieldAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNone,
		}},
	}
	// A whitespace-only field trims to empty and should be allowed.
	def := IndexDefinition{Field: "   "}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for whitespace-only field, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldNonePayloadRejectsAnyFieldM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNone,
		}},
	}
	def := IndexDefinition{Field: "kind"}
	err := rejectCreateIndexOnRetainedColumnField(meta, def)
	if err == nil {
		t.Fatal("expected error for RetainedPayloadNone with non-empty field, got nil")
	}
	if !containsString(err.Error(), "retained-payload-none") {
		t.Fatalf("error message missing 'retained-payload-none': %v", err)
	}
	if !containsString(err.Error(), "kind") {
		t.Fatalf("error message missing field name 'kind': %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldDeclaredColumnRejectedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNonColumn,
			Columns: []ColumnStoreColumn{
				{Name: "time_us", Path: "time_us"},
				{Name: "kind", Path: "kind"},
			},
		}},
	}
	// "kind" is a declared column - should be rejected.
	def := IndexDefinition{Field: "kind"}
	err := rejectCreateIndexOnRetainedColumnField(meta, def)
	if err == nil {
		t.Fatal("expected error for declared column field, got nil")
	}
	if !containsString(err.Error(), "kind") {
		t.Fatalf("error message missing field name 'kind': %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldUndeclaredColumnAllowedM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNonColumn,
			Columns: []ColumnStoreColumn{
				{Name: "time_us", Path: "time_us"},
			},
		}},
	}
	// "did" is not a declared column - should be allowed.
	def := IndexDefinition{Field: "did"}
	if err := rejectCreateIndexOnRetainedColumnField(meta, def); err != nil {
		t.Fatalf("expected nil error for undeclared column field, got %v", err)
	}
}

func TestRejectCreateIndexOnRetainedColumnFieldPathTrimmingM1634(t *testing.T) {
	meta := CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNonColumn,
			Columns: []ColumnStoreColumn{
				{Name: "time_us", Path: "  time_us  "}, // path with whitespace
			},
		}},
	}
	// Field "time_us" should match trimmed path "time_us".
	def := IndexDefinition{Field: "time_us"}
	err := rejectCreateIndexOnRetainedColumnField(meta, def)
	if err == nil {
		t.Fatal("expected error: field matches trimmed column path, got nil")
	}
}

// Tests for preparedBatchUpdatePrimaryDocument (new function in this PR).

func TestPreparedBatchUpdatePrimaryDocumentNoPrimaryDocM1634(t *testing.T) {
	item := preparedBatchUpdate{
		documentID:         []byte("id1"),
		document:           []byte(`{"a":1}`),
		hasPrimaryDocument: false,
	}
	got := preparedBatchUpdatePrimaryDocument(item)
	if string(got) != `{"a":1}` {
		t.Fatalf("expected document=%q got %q", `{"a":1}`, string(got))
	}
}

func TestPreparedBatchUpdatePrimaryDocumentWithPrimaryDocM1634(t *testing.T) {
	item := preparedBatchUpdate{
		documentID:         []byte("id1"),
		document:           []byte(`{"a":1,"b":2}`),
		primaryDocument:    []byte(`{"b":2}`), // retained payload without "a"
		hasPrimaryDocument: true,
	}
	got := preparedBatchUpdatePrimaryDocument(item)
	if string(got) != `{"b":2}` {
		t.Fatalf("expected primaryDocument=%q got %q", `{"b":2}`, string(got))
	}
}

func TestPreparedBatchUpdatePrimaryDocumentNilPrimaryDocM1634(t *testing.T) {
	item := preparedBatchUpdate{
		documentID:         []byte("id1"),
		document:           []byte(`{"x":99}`),
		primaryDocument:    nil,
		hasPrimaryDocument: true,
	}
	// hasPrimaryDocument=true but nil primaryDocument should return nil.
	got := preparedBatchUpdatePrimaryDocument(item)
	if got != nil {
		t.Fatalf("expected nil primaryDocument, got %q", string(got))
	}
}

// Tests for preparedBatchUpdatesPrimaryDocumentBytes (new function in this PR).

func TestPreparedBatchUpdatesPrimaryDocumentBytesEmptyM1634(t *testing.T) {
	got := preparedBatchUpdatesPrimaryDocumentBytes(nil)
	if got != 0 {
		t.Fatalf("empty slice: expected 0 bytes, got %d", got)
	}
	got = preparedBatchUpdatesPrimaryDocumentBytes([]preparedBatchUpdate{})
	if got != 0 {
		t.Fatalf("empty slice: expected 0 bytes, got %d", got)
	}
}

func TestPreparedBatchUpdatesPrimaryDocumentBytesUsesDocumentWhenNoPrimaryM1634(t *testing.T) {
	changed := []preparedBatchUpdate{
		{documentID: []byte("id1"), document: []byte(`{"a":1}`), hasPrimaryDocument: false},
	}
	// len("id1") + len(`{"a":1}`) = 3 + 7 = 10
	want := int64(3 + 7)
	got := preparedBatchUpdatesPrimaryDocumentBytes(changed)
	if got != want {
		t.Fatalf("expected %d bytes, got %d", want, got)
	}
}

func TestPreparedBatchUpdatesPrimaryDocumentBytesUsesPrimaryDocWhenSetM1634(t *testing.T) {
	changed := []preparedBatchUpdate{
		{
			documentID:         []byte("id1"),
			document:           []byte(`{"a":1,"b":2}`),
			primaryDocument:    []byte(`{"b":2}`),
			hasPrimaryDocument: true,
		},
	}
	// len("id1") + len(`{"b":2}`) = 3 + 7 = 10
	want := int64(3 + 7)
	got := preparedBatchUpdatesPrimaryDocumentBytes(changed)
	if got != want {
		t.Fatalf("expected %d bytes, got %d", want, got)
	}
}

func TestPreparedBatchUpdatesPrimaryDocumentBytesMultipleItemsM1634(t *testing.T) {
	changed := []preparedBatchUpdate{
		{documentID: []byte("a"), document: []byte("doc1"), hasPrimaryDocument: false},
		{documentID: []byte("bb"), document: []byte("doc22"), primaryDocument: []byte("p"), hasPrimaryDocument: true},
	}
	// item1: 1 + 4 = 5
	// item2: 2 + 1 = 3 (uses primaryDocument)
	want := int64(5 + 3)
	got := preparedBatchUpdatesPrimaryDocumentBytes(changed)
	if got != want {
		t.Fatalf("expected %d bytes, got %d", want, got)
	}
}

func TestPreparedBatchUpdatesPrimaryDocumentBytesNonNegativeM1634(t *testing.T) {
	// Verify the function never returns a negative value for typical inputs.
	// The underlying saturatingAddNonNegativeInt64 handles overflow protection.
	doc := make([]byte, 1024)
	id := make([]byte, 16)
	changed := make([]preparedBatchUpdate, 1000)
	for i := range changed {
		changed[i] = preparedBatchUpdate{documentID: id, document: doc}
	}
	got := preparedBatchUpdatesPrimaryDocumentBytes(changed)
	if got < 0 {
		t.Fatalf("expected non-negative value, got %d", got)
	}
	want := int64(1000 * (16 + 1024))
	if got != want {
		t.Fatalf("expected %d, got %d", want, got)
	}
}


// Tests for appendUpdateBatchPlanScratchDocument (bug fix in this PR: empty doc
// now returns non-nil empty slice instead of nil).

func TestAppendUpdateBatchPlanScratchDocumentNilScratchM1634(t *testing.T) {
	got := appendUpdateBatchPlanScratchDocument(nil, []byte("data"))
	if got != nil {
		t.Fatalf("nil scratch: expected nil result, got %q", string(got))
	}
}

func TestAppendUpdateBatchPlanScratchDocumentNilScratchEmptyDocM1634(t *testing.T) {
	got := appendUpdateBatchPlanScratchDocument(nil, nil)
	if got != nil {
		t.Fatalf("nil scratch with nil doc: expected nil, got non-nil")
	}
}

func TestAppendUpdateBatchPlanScratchDocumentEmptyDocReturnsNonNilM1634(t *testing.T) {
	// Key bug fix: previously returned nil for empty document, now returns
	// a non-nil empty slice backed by the arena. This distinguishes "not present"
	// from "present but empty".
	scratch := &updateBatchPlanScratch{}
	got := appendUpdateBatchPlanScratchDocument(scratch, []byte{})
	if got == nil {
		t.Fatal("empty document: expected non-nil empty slice, got nil (regression)")
	}
	if len(got) != 0 {
		t.Fatalf("empty document: expected len=0, got %d", len(got))
	}
}

func TestAppendUpdateBatchPlanScratchDocumentNilDocReturnsNonNilM1634(t *testing.T) {
	// Same as empty: nil document should also return non-nil empty slice.
	scratch := &updateBatchPlanScratch{}
	got := appendUpdateBatchPlanScratchDocument(scratch, nil)
	if got == nil {
		t.Fatal("nil document: expected non-nil empty slice, got nil (regression)")
	}
	if len(got) != 0 {
		t.Fatalf("nil document: expected len=0, got %d", len(got))
	}
}

func TestAppendUpdateBatchPlanScratchDocumentNonEmptyDocM1634(t *testing.T) {
	scratch := &updateBatchPlanScratch{}
	doc := []byte(`{"hello":"world"}`)
	got := appendUpdateBatchPlanScratchDocument(scratch, doc)
	if string(got) != string(doc) {
		t.Fatalf("expected %q got %q", string(doc), string(got))
	}
}

func TestAppendUpdateBatchPlanScratchDocumentArenaGrowsM1634(t *testing.T) {
	scratch := &updateBatchPlanScratch{}
	doc1 := []byte(`{"a":1}`)
	doc2 := []byte(`{"b":2}`)
	got1 := appendUpdateBatchPlanScratchDocument(scratch, doc1)
	got2 := appendUpdateBatchPlanScratchDocument(scratch, doc2)
	if string(got1) != string(doc1) {
		t.Fatalf("doc1=%q want %q", string(got1), string(doc1))
	}
	if string(got2) != string(doc2) {
		t.Fatalf("doc2=%q want %q", string(got2), string(doc2))
	}
}

func TestAppendUpdateBatchPlanScratchDocumentCapIsExactM1634(t *testing.T) {
	// Verify returned slice has cap == len so callers can't accidentally
	// share the backing array.
	scratch := &updateBatchPlanScratch{}
	doc := []byte("hello")
	got := appendUpdateBatchPlanScratchDocument(scratch, doc)
	if cap(got) != len(got) {
		t.Fatalf("expected cap(got)==len(got)=%d, got cap=%d", len(got), cap(got))
	}
}

// Tests for ErrColumnAssetReachabilityIncomplete (new error added in this PR).

func TestErrColumnAssetReachabilityIncompleteIsDistinctM1634(t *testing.T) {
	if ErrColumnAssetReachabilityIncomplete == nil {
		t.Fatal("ErrColumnAssetReachabilityIncomplete must not be nil")
	}
	if !errors.Is(ErrColumnAssetReachabilityIncomplete, ErrColumnAssetReachabilityIncomplete) {
		t.Fatal("errors.Is should return true for same sentinel error")
	}
	if errors.Is(ErrColumnAssetReachabilityIncomplete, ErrCollectionNotFound) {
		t.Fatal("ErrColumnAssetReachabilityIncomplete should not match ErrCollectionNotFound")
	}
}

func TestErrColumnAssetReachabilityIncompleteWrappingM1634(t *testing.T) {
	wrapped := errors.Join(ErrColumnAssetReachabilityIncomplete, errors.New("context detail"))
	if !errors.Is(wrapped, ErrColumnAssetReachabilityIncomplete) {
		t.Fatal("wrapped error should satisfy errors.Is for ErrColumnAssetReachabilityIncomplete")
	}
}

// containsString is a test helper that avoids import of strings package.
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || findString(s, substr))
}

func findString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
