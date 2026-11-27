package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustCompileIndexRuntimes(t *testing.T, defs []IndexDefinition) []indexRuntime {
	t.Helper()
	out := make([]indexRuntime, len(defs))
	for i, def := range defs {
		rt, err := singleIndexRuntime(def)
		if err != nil {
			t.Fatalf("singleIndexRuntime(%q): %v", def.Name, err)
		}
		out[i] = rt
	}
	return out
}

func TestScanStrictJSONObjectRootFieldsParityAndErrors(t *testing.T) {
	runtimes := mustCompileIndexRuntimes(t, []IndexDefinition{
		{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		{Name: "city", Field: "city", ValueType: IndexValueString},
		{Name: "age", Field: "age", ValueType: IndexValueInt64},
		{Name: "score", Field: "score", ValueType: IndexValueDouble},
		{Name: "active", Field: "active", ValueType: IndexValueBool},
	})
	opts := collectionOptions{documentFormat: DocumentFormatJSON}

	validCases := []struct {
		name string
		doc  string
	}{
		{"empty_object", `{}`},
		{"whitespace_empty", " \t\r\n { \n\t } \r\n "},
		{"simple_scalars", `{"email":"a@b.com","city":"hnl","age":42,"score":3.14e+1,"active":true}`},
		{"negative_and_zero_numbers", `{"age":-0,"score":-12.5e-2,"active":false,"city":null}`},
		{"escaped_string_value", `{"email":"a\"b\\c\/d\b\f\n\r\t\u0041@b.com","city":"tok\u0079o"}`},
		{"utf8_string_value", `{"email":"café@example.com","city":"東京"}`},
		{"duplicate_key_last_wins", `{"city":"first","city":"second","email":"x@y.com"}`},
		{"nested_objects_and_arrays", `{"meta":{"tags":["a",1,true,false,null,{"k":"v"}],"empty":[]},"city":"sf","email":"n@e.com"}`},
		{"escaped_top_level_key_fallback", `{"em\u0061il":"fb@e.com","city":"la"}`},
		{"deep_nesting_fallback", `{"nested":` + strings.Repeat(`[`, 70) + `1` + strings.Repeat(`]`, 70) + `,"city":"deep"}`},
	}

	for _, tc := range validCases {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			var fastEnc indexEncodeArena
			got, ok, err := orderedJSONRootIndexStateForDocumentFastPath([]byte(tc.doc), runtimes, opts, &fastEnc)
			if !ok || err != nil {
				t.Fatalf("fast path ok=%t err=%v", ok, err)
			}
			// Compare against generic encoding/json fallback by forcing non-root path?
			// Instead verify state via documentIndexStateFromOrdered against manual
			// orderedIndexStateForDocument decoding.
			var refEnc indexEncodeArena
			// Run fallback explicitly using a multi-segment copy? No, compare against
			// encoding/json directly:
			want, err := decodeReferenceJSONIndexState([]byte(tc.doc), runtimes, opts, &refEnc)
			if err != nil {
				t.Fatalf("reference decode failed: %v", err)
			}
			for i := range runtimes {
				gv := got.valuesAt(i)
				wv := want.valuesAt(i)
				if len(gv) != len(wv) {
					t.Fatalf("runtime[%d] (%s) len=%d want %d", i, runtimes[i].def.name, len(gv), len(wv))
				}
				for j := range gv {
					if !bytes.Equal(gv[j], wv[j]) {
						t.Fatalf("runtime[%d] (%s)[%d] got=%x want=%x", i, runtimes[i].def.name, j, gv[j], wv[j])
					}
				}
			}
		})
	}

	invalidCases := []struct {
		name string
		doc  string
	}{
		{"empty", ``},
		{"whitespace_only", `   `},
		{"array_root", `[1,2,3]`},
		{"string_root", `"hello"`},
		{"number_root", `123`},
		{"trailing_comma_obj", `{"city":"a",}`},
		{"trailing_comma_arr", `{"x":[1,]}`},
		{"trailing_garbage", `{"city":"a"} extra`},
		{"unquoted_key", `{city:"a"}`},
		{"missing_colon", `{"city" "a"}`},
		{"unescaped_control_char_in_key", "{\"ci\x01ty\":\"a\"}"},
		{"unescaped_control_char_in_val", "{\"city\":\"a\nb\"}"},
		{"bad_escape", `{"city":"a\x20b"}`},
		{"short_unicode_escape", `{"city":"\u123"}`},
		{"non_hex_unicode_escape", `{"city":"\u123g"}`},
		{"leading_zero_number", `{"age":01}`},
		{"trailing_dot_number", `{"score":1.}`},
		{"incomplete_exp_number", `{"score":1e+}`},
		{"bad_true", `{"active":tru}`},
		{"bad_false", `{"active":fals}`},
		{"bad_null", `{"city":nul}`},
		{"unclosed_obj", `{"city":"a"`},
		{"unclosed_nested", `{"x":{"a":1}`},
		{"invalid_utf8_val", string([]byte{'{', '"', 'c', 'i', 't', 'y', '"', ':', '"', 0xff, '"', '}'})},
		{"invalid_utf8_key", string([]byte{'{', '"', 0xff, '"', ':', '1', '}'})},
	}

	for _, tc := range invalidCases {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			var fastEnc indexEncodeArena
			_, ok, err := orderedJSONRootIndexStateForDocumentFastPath([]byte(tc.doc), runtimes, opts, &fastEnc)
			if !ok || err == nil {
				t.Fatalf("expected fast path error for %q, got ok=%t err=%v", tc.doc, ok, err)
			}
		})
	}
}

func decodeReferenceJSONIndexState(doc []byte, runtimes []indexRuntime, opts collectionOptions, enc *indexEncodeArena) (orderedDocumentIndexState, error) {
	// Temporarily wrap each runtime with a 2-segment path? Even simpler, call
	// json.Decoder directly using the same logic as orderedIndexStateForDocumentWithArena.
	var decoded any
	if err := decodeStrictJSONObject(doc, &decoded); err != nil {
		return nil, err
	}
	obj := decoded.(map[string]any)
	state := enc.appendState(len(runtimes))
	for i, rt := range runtimes {
		val, found := extractIndexPathValue(obj, rt.path)
		if !found || val == nil {
			continue
		}
		var next []byte
		var err error
		enc.buf, next, err = appendIndexScalar(enc.buf, rt.def.valueType, val)
		if err != nil {
			return nil, err
		}
		state[i] = enc.appendSingleValueRef(next)
	}
	return state, nil
}

func decodeStrictJSONObject(doc []byte, out *any) error {
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	return dec.Decode(out)
}

func TestParallelPlanIndexStateAndEmitIndexStateRunParity(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			runtimes := mustCompileIndexRuntimes(t, []IndexDefinition{
				{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
				{Name: "city", Field: "city", ValueType: IndexValueString},
			})
			const count = 2500
			ids, docs := makeDurableRecoveryDocs(t, format, 0, count)
			planner := insertBatchPlanner{
				collection:          "users",
				primaryRoot:         collectionPrimaryRootName("users"),
				indexStateRoot:      collectionIndexStateRootName("users"),
				cachedIndexRuntimes: runtimes,
				options: collectionOptions{
					documentFormat: format,
				},
				enablePlanScratch: true,
			}
			plan, err := planner.planInsertBatch(ids, docs)
			if err != nil {
				t.Fatalf("planInsertBatch: %v", err)
			}
			defer func() {
				resetCollectionRunTables(plan.runs)
				plan.releaseScratch()
			}()
			if len(plan.uniqueProbeCandidates) != count {
				t.Fatalf("uniqueProbeCandidates=%d want %d", len(plan.uniqueProbeCandidates), count)
			}
			wantRuns := 3 // primary + email + city
			if format == DocumentFormatJSON {
				wantRuns = 4 // primary + indexState + email + city
			}
			if len(plan.runs) != wantRuns {
				t.Fatalf("runs=%d want %d", len(plan.runs), wantRuns)
			}
			if format == DocumentFormatJSON {
				if plan.runs[0].kind != collectionRootPrimary || plan.runs[1].kind != collectionRootIndexState {
					t.Fatalf("unexpected run order: %v, %v", plan.runs[0].kind, plan.runs[1].kind)
				}
				if got := plan.runs[1].table.Len(); got != count {
					t.Fatalf("indexState run len=%d want %d", got, count)
				}
			}

			// Error path: corrupt one document near the end of the batch and verify
			// parallel extraction reports an error cleanly without panicking or leaking.
			badDocs := append([][]byte(nil), docs...)
			if format == DocumentFormatJSON {
				badDocs[count-7] = []byte(`{"email":12345}`)
			} else {
				badDocs[count-7] = mustBSONCollectionDocument(t, bson.D{
					{Key: "_id", Value: fmt.Sprintf("doc-%06d", count-7)},
					{Key: "email", Value: int32(12345)},
				})
			}
			if _, err := planner.planInsertBatch(ids, badDocs); err == nil {
				t.Fatal("expected error for mismatched index field type in parallel batch")
			}
		})
	}
}
