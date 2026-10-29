package collections

import (
	"bytes"
	"encoding/json"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// A captured template dictionary must remain paired with its original stored
// rows when later mutations introduce new dictionary shapes and checkpoint.
func TestNativeTemplateHeldDictionaryCut(t *testing.T) {
	owner, _, collection := openNativeTextPublicFormat(t, t.TempDir(), treedb.ProfileNoWALFast, true, DocumentFormatTemplateV1)
	document := func(tenant, content, shape string) []byte {
		t.Helper()
		raw, err := json.Marshal(map[string]any{
			"tenant": tenant, "content": content,
			"nested":   map[string]any{shape: map[string]any{"label": tenant}},
			"repeated": []any{map[string]any{"label": tenant}, map[string]any{"label": "again"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := EncodeTemplateV1DocumentJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if _, err := collection.Insert([]byte("a"), document("one", "original policy", "shape_a")); err != nil {
		t.Fatal(err)
	}
	old, err := collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	stored, err := old.Get([]byte("a"))
	if err != nil || !bytes.HasPrefix(stored, []byte("TD1D")) {
		t.Fatalf("stored template: %x %v", stored, err)
	}
	stored = bytes.Clone(stored)
	if _, _, err := collection.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return document("two", "current policy", "only_in_new_cut"), true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	currentMaterializer, err := collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer currentMaterializer.Close()
	current, err := collection.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	currentJSON, err := currentMaterializer.StoredDocumentJSON(current)
	if err != nil || !bytes.Contains(currentJSON, []byte("only_in_new_cut")) {
		t.Fatalf("current template shape: %s %v", currentJSON, err)
	}
	heldMaterializer := &StoredDocumentJSONMaterializer{documentFormat: DocumentFormatTemplateV1, nativeRead: old}
	oldJSON, err := heldMaterializer.StoredDocumentJSON(stored)
	if err != nil || !bytes.Contains(oldJSON, []byte("shape_a")) || bytes.Contains(oldJSON, []byte("only_in_new_cut")) {
		t.Fatalf("held dictionary cut: %s %v", oldJSON, err)
	}
	if !bytes.Contains(oldJSON, []byte("original policy")) || bytes.Contains(oldJSON, []byte("current policy")) {
		t.Fatalf("held document changed across checkpoint: %s", oldJSON)
	}
}
