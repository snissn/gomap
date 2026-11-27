package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoOpenCollectionForMutationNativeCacheAndDDLInvalidation(t *testing.T) {
	dir := t.TempDir()
	s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, 16)
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	ns := "app." + collName
	col1, err := s.Server.openCollectionForMutation(ns)
	if err != nil {
		t.Fatalf("openCollectionForMutation #1: %v", err)
	}
	col2, err := s.Server.openCollectionForMutation(ns)
	if err != nil {
		t.Fatalf("openCollectionForMutation #2: %v", err)
	}
	if col1 != col2 {
		t.Fatalf("expected native COW openCollectionForMutation cache hit, got distinct handles %p vs %p", col1, col2)
	}

	ctx := context.Background()
	createIdxCmd := mustDocument(t, bson.D{
		{Key: "createIndexes", Value: collName},
		{Key: "$db", Value: "app"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "name", Value: "payload_1"},
			{Key: "key", Value: bson.D{{Key: "payload", Value: int32(1)}}},
		}}},
	})
	resp, err := s.Server.commandResponse(ctx, "createIndexes", createIdxCmd, nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("createIndexes failed: resp=%s err=%v", resp, err)
	}
	col3, err := s.Server.openCollectionForMutation(ns)
	if err != nil {
		t.Fatalf("openCollectionForMutation after createIndexes: %v", err)
	}
	if col3 == col1 {
		t.Fatalf("expected createIndexes to invalidate collection cache (still got %p)", col3)
	}

	dropIdxCmd := mustDocument(t, bson.D{
		{Key: "dropIndexes", Value: collName},
		{Key: "$db", Value: "app"},
		{Key: "index", Value: "payload_1"},
	})
	resp, err = s.Server.commandResponse(ctx, "dropIndexes", dropIdxCmd, nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("dropIndexes failed: resp=%s err=%v", resp, err)
	}
	col4, err := s.Server.openCollectionForMutation(ns)
	if err != nil {
		t.Fatalf("openCollectionForMutation after dropIndexes: %v", err)
	}
	if col4 == col3 {
		t.Fatalf("expected dropIndexes to invalidate collection cache (still got %p)", col4)
	}

	// Verify concurrent UpdateOne + DeleteOne sharing the cached immutable
	// native COW Collection handle execute race-free.
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			for step := 1; step <= 10; step++ {
				cmd := buildUpdateOneCommand(t, collName, int64(i), int64(step))
				r, e := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
				if e != nil || !mongoCommandResponseOK(r) {
					errCh <- fmt.Errorf("update id=%d step=%d: resp=%s err=%v", i, step, r, e)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			delCmd := mustDocument(t, bson.D{
				{Key: "delete", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "deletes", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: int64(8 + i)}}},
					{Key: "limit", Value: int32(1)},
				}}},
			})
			r, e := s.Server.commandResponse(ctx, "delete", delCmd, nil, 0)
			if e != nil || !mongoCommandResponseOK(r) {
				errCh <- fmt.Errorf("delete id=%d: resp=%s err=%v", 8+i, r, e)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if got := findDocN(t, s.Server, collName, int64(i)); got != 10 {
			t.Fatalf("doc %d n=%d want 10", i, got)
		}
	}
}

func TestMarshalUpdateResponseParityWithBSONMarshal(t *testing.T) {
	cases := [][2]int32{
		{0, 0},
		{1, 0},
		{1, 1},
		{5, 3},
		{1000, 999},
	}
	for _, tc := range cases {
		matched, modified := tc[0], tc[1]
		got := marshalSimpleUpdateResponse(matched, modified)
		want, err := bson.Marshal(bson.D{
			{Key: "ok", Value: float64(1)},
			{Key: "n", Value: matched},
			{Key: "nModified", Value: modified},
		})
		if err != nil {
			t.Fatalf("bson.Marshal(%d,%d): %v", matched, modified, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("marshalSimpleUpdateResponse(%d,%d) mismatch:\n got=%x\nwant=%x", matched, modified, []byte(got), want)
		}
		if err := bson.Raw(got).Validate(); err != nil {
			t.Fatalf("marshalSimpleUpdateResponse(%d,%d) invalid BSON: %v", matched, modified, err)
		}
	}
}

func TestIDEqualityFilterValueFastPathAndFallbacks(t *testing.T) {
	oid := bson.NewObjectID()
	validCases := []struct {
		name string
		val  any
	}{
		{name: "int64", val: int64(42)},
		{name: "string", val: "doc-key"},
		{name: "objectid", val: oid},
		{name: "embedded_doc", val: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}},
	}
	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := bson.Marshal(bson.D{{Key: "_id", Value: tc.val}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := idEqualityFilterValue(wire.Document(raw), "update")
			if err != nil {
				t.Fatalf("idEqualityFilterValue(%s): %v", tc.name, err)
			}
			elems, err := bson.Raw(raw).Elements()
			if err != nil || len(elems) != 1 {
				t.Fatalf("elements: %v", err)
			}
			want := elems[0].Value()
			if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("got (%v, %x), want (%v, %x)", got.Type, got.Value, want.Type, want.Value)
			}
		})
	}

	errorDocs := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: mustDocument(t, bson.D{})},
		{name: "wrong_key", raw: mustDocument(t, bson.D{{Key: "x", Value: int32(1)}})},
		{name: "multi_field", raw: mustDocument(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "x", Value: int32(2)}})},
		{name: "regex_id", raw: mustDocument(t, bson.D{{Key: "_id", Value: bson.Regex{Pattern: "^a"}}})},
		{name: "malformed", raw: []byte{0xff, 0x00, 0x00, 0x00, 0x01}},
	}
	for _, tc := range errorDocs {
		t.Run("err_"+tc.name, func(t *testing.T) {
			if _, err := idEqualityFilterValue(wire.Document(tc.raw), "update"); err == nil {
				t.Fatalf("expected error for %s filter", tc.name)
			}
		})
	}
}

func TestUpdateInlineArrayAndSequenceFastParseParity(t *testing.T) {
	dir := t.TempDir()
	s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, 4)
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	ctx := context.Background()
	// 1. Inline single-element updates array (exercises inline single-pass fastParsed path).
	inlineCmd := buildUpdateOneCommand(t, collName, 0, 77)
	resp, err := s.Server.commandResponse(ctx, "update", inlineCmd, nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("inline update failed: resp=%s err=%v", resp, err)
	}
	if got := findDocN(t, s.Server, collName, 0); got != 77 {
		t.Fatalf("inline doc 0 n=%d want 77", got)
	}

	// 2. OP_MSG document sequence (exercises sequences fastParsed path).
	seqCmd := mustDocument(t, bson.D{
		{Key: "update", Value: collName},
		{Key: "$db", Value: "app"},
	})
	seqItem := mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: int64(1)}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(88)}}}}},
	})
	resp, err = s.Server.commandResponse(ctx, "update", seqCmd, []wire.DocumentSequence{
		{Identifier: "updates", Documents: []wire.Document{seqItem}},
	}, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("sequence update failed: resp=%s err=%v", resp, err)
	}
	if got := findDocN(t, s.Server, collName, 1); got != 88 {
		t.Fatalf("sequence doc 1 n=%d want 88", got)
	}

	// 3. Inline empty updates array must fall back cleanly and return n=0, ok=1.
	emptyInline := mustDocument(t, bson.D{
		{Key: "update", Value: collName},
		{Key: "$db", Value: "app"},
		{Key: "updates", Value: bson.A{}},
	})
	resp, err = s.Server.commandResponse(ctx, "update", emptyInline, nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("empty inline update failed: resp=%s err=%v", resp, err)
	}
	if matched, _ := bson.Raw(resp).Lookup("n").Int32OK(); matched != 0 {
		t.Fatalf("empty inline n=%d want 0", matched)
	}
}

func TestParseFastPureSetUpdateDocDuplicateAndNesting(t *testing.T) {
	// Scalar pure $set: no nesting, JSON & BSON supported.
	scalarDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "n", Value: int64(7)},
			{Key: "name", Value: "alice"},
			{Key: "flag", Value: true},
		}},
	})
	fields, setOK, bsonOK, needsNesting, handled := parseFastPureSetUpdateDocIntoWithNesting(scalarDoc, nil)
	if !handled || !setOK || !bsonOK || needsNesting || len(fields) != 3 {
		t.Fatalf("scalar: handled=%v setOK=%v bsonOK=%v needsNesting=%v len=%d", handled, setOK, bsonOK, needsNesting, len(fields))
	}

	// Dotted key must decline the fast top-level pure-$set path.
	dottedDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "a.b", Value: int32(1)},
		}},
	})
	if _, _, _, _, handled := parseFastPureSetUpdateDocIntoWithNesting(dottedDoc, nil); handled {
		t.Fatal("expected dotted field path to decline fast pure-$set path")
	}

	// Duplicate top-level key keeps last value.
	dupDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "n", Value: int32(2)},
		}},
	})
	fields, setOK, bsonOK, needsNesting, handled = parseFastPureSetUpdateDocIntoWithNesting(dupDoc, nil)
	if !handled || !setOK || !bsonOK || needsNesting || len(fields) != 1 {
		t.Fatalf("dup top-level: handled=%v setOK=%v bsonOK=%v needsNesting=%v len=%d", handled, setOK, bsonOK, needsNesting, len(fields))
	}
	if v, ok := fields[0].Value.Int32OK(); !ok || v != 2 {
		t.Fatalf("dup value=%v want 2", fields[0].Value)
	}

	// Build a value at max standalone container depth so wrapping it inside a
	// document triggers needsNesting=true, then verify overwriting that key
	// with a scalar recomputes needsNesting=false.
	deepVal := any(int32(1))
	for d := 0; d < mongoMutationMaxBSONNesting; d++ {
		deepVal = bson.D{{Key: "k", Value: deepVal}}
	}
	deepOnlyDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "meta", Value: deepVal},
		}},
	})
	fields, setOK, bsonOK, needsNesting, handled = parseFastPureSetUpdateDocIntoWithNesting(deepOnlyDoc, nil)
	if !handled || !setOK || !bsonOK || !needsNesting || len(fields) != 1 {
		t.Fatalf("deep nested: handled=%v setOK=%v bsonOK=%v needsNesting=%v len=%d", handled, setOK, bsonOK, needsNesting, len(fields))
	}

	dupOverwriteScalar := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "meta", Value: deepVal},
			{Key: "meta", Value: "plain"},
		}},
	})
	fields, setOK, bsonOK, needsNesting, handled = parseFastPureSetUpdateDocIntoWithNesting(dupOverwriteScalar, nil)
	if !handled || !setOK || !bsonOK || needsNesting || len(fields) != 1 {
		t.Fatalf("dup overwrite scalar: handled=%v setOK=%v bsonOK=%v needsNesting=%v len=%d", handled, setOK, bsonOK, needsNesting, len(fields))
	}

	// DateTime value: BSON-supported, JSON-unsupported (`setOK == false`, `bsonOK == true`).
	dtDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "ts", Value: bson.DateTime(1700000000000)},
		}},
	})
	fields, setOK, bsonOK, needsNesting, handled = parseFastPureSetUpdateDocIntoWithNesting(dtDoc, nil)
	if !handled || setOK || !bsonOK || needsNesting || len(fields) != 1 {
		t.Fatalf("datetime: handled=%v setOK=%v bsonOK=%v needsNesting=%v len=%d", handled, setOK, bsonOK, needsNesting, len(fields))
	}

	// Non-$set operator must not be handled by fast path.
	incDoc := mustDocument(t, bson.D{
		{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}},
	})
	if _, _, _, _, handled := parseFastPureSetUpdateDocIntoWithNesting(incDoc, nil); handled {
		t.Fatal("expected $inc to decline fast pure-$set path")
	}

	// Empty key inside $set must decline fast path so full validator returns error.
	emptyKeyDoc := mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "", Value: int32(1)}}},
	})
	if _, _, _, _, handled := parseFastPureSetUpdateDocIntoWithNesting(emptyKeyDoc, nil); handled {
		t.Fatal("expected empty field key to decline fast pure-$set path")
	}
}
