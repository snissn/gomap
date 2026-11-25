package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshalWireDoc(tb testing.TB, v any) wire.Document {
	tb.Helper()
	raw, err := bson.Marshal(v)
	if err != nil {
		tb.Fatalf("marshal bson: %v", err)
	}
	return wire.Document(raw)
}

func cowStatUint64(tb testing.TB, s *StandaloneServer, key string) uint64 {
	tb.Helper()
	stats := s.Stats()
	raw, ok := stats[key]
	if !ok {
		tb.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func extractFirstBatchDocs(tb testing.TB, resp wire.Document) []wire.Document {
	tb.Helper()
	raw := bson.Raw(resp)
	okVal, err := raw.LookupErr("ok")
	if err != nil {
		tb.Fatalf("response missing ok: %v", err)
	}
	okNum, ok := okVal.AsInt64OK()
	if !ok || okNum != 1 {
		tb.Fatalf("command failed: %s", raw.String())
	}
	arrVal, err := raw.LookupErr("cursor", "firstBatch")
	if err != nil {
		tb.Fatalf("response missing cursor.firstBatch: %v", err)
	}
	arr, ok := arrVal.ArrayOK()
	if !ok {
		tb.Fatalf("cursor.firstBatch not array")
	}
	vals, err := arr.Values()
	if err != nil {
		tb.Fatalf("array values: %v", err)
	}
	out := make([]wire.Document, 0, len(vals))
	for _, val := range vals {
		doc, ok := val.DocumentOK()
		if !ok {
			tb.Fatalf("firstBatch element not document")
		}
		out = append(out, wire.Document(bytes.Clone(doc)))
	}
	return out
}

func openStandaloneCOWBSONFixture(tb testing.TB, format collections.DocumentFormat, bsonV2EmailIndex bool, docCount int) (*StandaloneServer, string) {
	tb.Helper()
	server, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     tb.TempDir(),
		Profile: treedb.ProfileCommandWALRelaxed,
	})
	if err != nil {
		tb.Fatalf("OpenStandaloneServer: %v", err)
	}
	tb.Cleanup(func() {
		if err := server.Close(); err != nil {
			tb.Errorf("Close standalone server: %v", err)
		}
	})
	if !server.Backend.HasNativeSemanticBridge() || server.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
		tb.Fatalf("expected standalone cow_btree native semantic bridge, mode=%q", server.Stats()["treedb.cache.memtable_mode"])
	}

	const dbName = "app"
	const collName = "users"
	emailIdxType := collections.IndexValueString
	if bsonV2EmailIndex {
		emailIdxType = collections.IndexValueBSONOrderedV2
	}
	emailIdx := collections.IndexDefinition{
		Name:      "email_1",
		Field:     "email",
		ValueType: emailIdxType,
		Unique:    true,
	}
	if bsonV2EmailIndex {
		emailIdx.Components = []collections.IndexComponent{{Field: "email", Direction: collections.IndexDirectionAscending}}
	}
	indexes := []collections.IndexDefinition{
		emailIdx,
		{
			Name:      "age_1",
			Field:     "age",
			ValueType: collections.IndexValueInt64,
		},
	}
	if format == collections.DocumentFormatBSON {
		indexes = append(indexes, collections.IndexDefinition{
			Name:       "tag_age_1",
			Field:      "tag",
			ValueType:  collections.IndexValueBSONOrderedV2,
			Components: []collections.IndexComponent{{Field: "tag", Direction: collections.IndexDirectionAscending}, {Field: "age", Direction: collections.IndexDirectionAscending}},
		})
	}
	_, err = server.Collections.CreateCollection(&collections.CollectionMeta{
		Name:    dbName + "." + collName,
		Options: collections.CollectionOptions{DocumentFormat: format},
		Indexes: indexes,
	})
	if err != nil {
		tb.Fatalf("CreateCollection(%s): %v", format, err)
	}

	ctx := context.Background()
	tags := []string{"alpha", "beta", "gamma"}
	batch := make(bson.A, 0, 128)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		cmd := mustMarshalWireDoc(tb, bson.D{
			{Key: "insert", Value: collName},
			{Key: "$db", Value: dbName},
			{Key: "documents", Value: batch},
		})
		resp, err := server.Server.insertResponse(ctx, cmd, nil)
		if err != nil {
			tb.Fatalf("insertResponse: %v", err)
		}
		if okVal, _ := bson.Raw(resp).Lookup("ok").AsInt64OK(); okVal != 1 {
			tb.Fatalf("insert failed: %s", bson.Raw(resp).String())
		}
		batch = batch[:0]
	}
	for i := 0; i < docCount; i++ {
		batch = append(batch, bson.D{
			{Key: "_id", Value: fmt.Sprintf("user-%05d", i)},
			{Key: "age", Value: int64(20 + (i % 50))},
			{Key: "email", Value: fmt.Sprintf("user-%05d@example.com", i)},
			{Key: "seq", Value: int64(i)},
			{Key: "tag", Value: tags[i%len(tags)]},
		})
		if len(batch) == 128 {
			flush()
		}
	}
	flush()
	return server, collName
}

func TestFindMetaCaptureSingleCutAndNoLeaseLeak(t *testing.T) {
	for _, tc := range []struct {
		name             string
		bsonV2EmailIndex bool
	}{
		{name: "StringEmailIndex", bsonV2EmailIndex: false},
		{name: "BSONOrderedV2EmailIndex", bsonV2EmailIndex: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, collName := openStandaloneCOWBSONFixture(t, collections.DocumentFormatBSON, tc.bsonV2EmailIndex, 64)
			ctx := context.Background()

			// Warm collection cache before measuring COW capture counts.
			warmCmd := mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: "user-00010@example.com"}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			})
			if docs := extractFirstBatchDocs(t, mustFindResponse(t, server.Server, ctx, warmCmd)); len(docs) != 1 {
				t.Fatalf("warm find got %d docs want 1", len(docs))
			}

			baseLeases := cowStatUint64(t, server, "treedb.cache.cow.external_leases")
			baseActiveCuts := cowStatUint64(t, server, "treedb.cache.cow.active_cuts")
			baseViews := cowStatUint64(t, server, "treedb.cache.cow.views")

			cases := []struct {
				name string
				cmd  wire.Document
				want string
			}{
				{
					name: "FindOneByEmail",
					cmd: mustMarshalWireDoc(t, bson.D{
						{Key: "find", Value: collName},
						{Key: "$db", Value: "app"},
						{Key: "filter", Value: bson.D{{Key: "email", Value: "user-00025@example.com"}}},
						{Key: "limit", Value: int32(1)},
						{Key: "singleBatch", Value: true},
					}),
					want: "user-00025",
				},
				{
					name: "FindOneByPrimaryID",
					cmd: mustMarshalWireDoc(t, bson.D{
						{Key: "find", Value: collName},
						{Key: "$db", Value: "app"},
						{Key: "filter", Value: bson.D{{Key: "_id", Value: "user-00025"}}},
						{Key: "limit", Value: int32(1)},
						{Key: "singleBatch", Value: true},
					}),
					want: "user-00025",
				},
				{
					name: "FindOneByPrimaryIDProjected",
					cmd: mustMarshalWireDoc(t, bson.D{
						{Key: "find", Value: collName},
						{Key: "$db", Value: "app"},
						{Key: "filter", Value: bson.D{{Key: "_id", Value: "user-00025"}}},
						{Key: "projection", Value: bson.D{{Key: "email", Value: int32(1)}}},
						{Key: "limit", Value: int32(1)},
						{Key: "singleBatch", Value: true},
					}),
					want: "user-00025",
				},
			}

			for _, q := range cases {
				t.Run(q.name, func(t *testing.T) {
					beforeCaptures := cowStatUint64(t, server, "treedb.cache.cow.capture_calls_total")
					const iters = 10
					for i := 0; i < iters; i++ {
						docs := extractFirstBatchDocs(t, mustFindResponse(t, server.Server, ctx, q.cmd))
						if len(docs) != 1 {
							t.Fatalf("got %d docs want 1", len(docs))
						}
						if gotID := bson.Raw(docs[0]).Lookup("_id").StringValue(); gotID != q.want {
							t.Fatalf("got _id=%q want %q", gotID, q.want)
						}
					}
					afterCaptures := cowStatUint64(t, server, "treedb.cache.cow.capture_calls_total")
					capturesPerOp := float64(afterCaptures-beforeCaptures) / float64(iters)
					if capturesPerOp > 1.0 {
						t.Fatalf("%s performed %.1f COW captures/op (delta=%d over %d ops), want <= 1.0", q.name, capturesPerOp, afterCaptures-beforeCaptures, iters)
					}

					afterLeases := cowStatUint64(t, server, "treedb.cache.cow.external_leases")
					afterActiveCuts := cowStatUint64(t, server, "treedb.cache.cow.active_cuts")
					afterViews := cowStatUint64(t, server, "treedb.cache.cow.views")
					if afterLeases > baseLeases || afterActiveCuts > baseActiveCuts || afterViews > baseViews {
						t.Fatalf("COW leak after %s: leases=%d(base %d) active_cuts=%d(base %d) views=%d(base %d)",
							q.name, afterLeases, baseLeases, afterActiveCuts, baseActiveCuts, afterViews, baseViews)
					}
				})
			}
		})
	}
}

func mustFindResponse(tb testing.TB, s *Server, ctx context.Context, cmd wire.Document) wire.Document {
	tb.Helper()
	resp, err := s.findResponse(ctx, cmd, 1)
	if err != nil {
		tb.Fatalf("findResponse: %v", err)
	}
	return resp
}

func TestFindMetaCaptureEquivalenceAcrossFormatsAndShapes(t *testing.T) {
	formats := []collections.DocumentFormat{
		collections.DocumentFormatBSON,
		collections.DocumentFormatJSON,
		collections.DocumentFormatTemplateV1,
	}
	servers := make(map[collections.DocumentFormat]*StandaloneServer, len(formats))
	var collName string
	for _, format := range formats {
		servers[format], collName = openStandaloneCOWBSONFixture(t, format, false, 40)
	}

	ctx := context.Background()
	queries := []struct {
		name string
		cmd  wire.Document
	}{
		{
			name: "EmailEq",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: "user-00012@example.com"}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			}),
		},
		{
			name: "EmailInWithDuplicates",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"user-00005@example.com", "user-00019@example.com", "user-00005@example.com", "missing@example.com"}}}}}},
				{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
			}),
		},
		{
			name: "EmailMissing",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: "nobody@example.com"}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			}),
		},
		{
			name: "EmailInSkipLimit",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"user-00001@example.com", "user-00002@example.com", "user-00003@example.com"}}}}}},
				{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
				{Key: "skip", Value: int32(1)},
				{Key: "limit", Value: int32(1)},
			}),
		},
		{
			name: "PrimaryIDEq",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "user-00007"}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			}),
		},
		{
			name: "PrimaryIDProjected",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "user-00007"}}},
				{Key: "projection", Value: bson.D{{Key: "age", Value: int32(1)}, {Key: "email", Value: int32(1)}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			}),
		},
		{
			name: "PrimaryIDInWithDuplicates",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"user-00003", "user-00009", "user-00003", "missing"}}}}}},
				{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
			}),
		},
		{
			name: "AgeRangeWithLimit",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(25)}, {Key: "$lte", Value: int64(30)}}}}},
				{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}, {Key: "_id", Value: int32(1)}}},
				{Key: "limit", Value: int32(4)},
			}),
		},
		{
			name: "PureAgeRangeFastPath",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(22)}}}}},
				{Key: "limit", Value: int32(3)},
			}),
		},
		{
			name: "UnsortedScanUnindexedField",
			cmd: mustMarshalWireDoc(t, bson.D{
				{Key: "find", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "seq", Value: int64(17)}}},
				{Key: "limit", Value: int32(1)},
			}),
		},
	}

	// Verify compound index with hint on BSON collection.
	t.Run("CompoundWithHintBSON", func(t *testing.T) {
		hintCmd := mustMarshalWireDoc(t, bson.D{
			{Key: "find", Value: collName},
			{Key: "$db", Value: "app"},
			{Key: "filter", Value: bson.D{{Key: "tag", Value: "beta"}, {Key: "age", Value: bson.D{{Key: "$gte", Value: int64(25)}}}}},
			{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "hint", Value: "tag_age_1"},
			{Key: "limit", Value: int32(5)},
		})
		noHintCmd := mustMarshalWireDoc(t, bson.D{
			{Key: "find", Value: collName},
			{Key: "$db", Value: "app"},
			{Key: "filter", Value: bson.D{{Key: "tag", Value: "beta"}, {Key: "age", Value: bson.D{{Key: "$gte", Value: int64(25)}}}}},
			{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "limit", Value: int32(5)},
		})
		got := extractFirstBatchDocs(t, mustFindResponse(t, servers[collections.DocumentFormatBSON].Server, ctx, hintCmd))
		want := extractFirstBatchDocs(t, mustFindResponse(t, servers[collections.DocumentFormatBSON].Server, ctx, noHintCmd))
		if len(got) == 0 || len(got) != len(want) {
			t.Fatalf("hinted compound query returned %d docs want %d", len(got), len(want))
		}
		for i := range got {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("hinted compound doc[%d] mismatch:\ngot:  %s\nwant: %s", i, bson.Raw(got[i]).String(), bson.Raw(want[i]).String())
			}
		}
	})

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			var baseline []wire.Document
			for _, format := range formats {
				docs := extractFirstBatchDocs(t, mustFindResponse(t, servers[format].Server, ctx, q.cmd))
				if baseline == nil {
					baseline = docs
					continue
				}
				if len(docs) != len(baseline) {
					t.Fatalf("format %s returned %d docs, want %d (BSON baseline)", format, len(docs), len(baseline))
				}
				for i := range docs {
					if !bytes.Equal(docs[i], baseline[i]) {
						t.Fatalf("format %s doc[%d] mismatch:\ngot:  %s\nwant: %s", format, i, bson.Raw(docs[i]).String(), bson.Raw(baseline[i]).String())
					}
				}
			}
		})
	}

	// Verify explain/stats path (plan.stats != nil) and backward-compatible materializer helper.
	for _, format := range formats {
		col, err := servers[format].Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection(%s): %v", format, err)
		}
		plan, err := parseFindPlan(
			mustMarshalWireDoc(t, bson.D{{Key: "find", Value: collName}, {Key: "$db", Value: "app"}, {Key: "limit", Value: int32(1)}}),
			mustMarshalWireDoc(t, bson.D{{Key: "email", Value: "user-00012@example.com"}}),
		)
		if err != nil {
			t.Fatalf("parseFindPlan: %v", err)
		}
		plan.stats = &findExecutionStats{}
		res, err := servers[format].Server.executeFind(col, plan)
		if err != nil {
			t.Fatalf("executeFind with stats (%s): %v", format, err)
		}
		if len(res.docs) != 1 || plan.stats.stage != "secondary_equality_lookup" || plan.stats.indexName != "email_1" || plan.stats.documentsReturned != 1 {
			t.Fatalf("format %s unexpected explain stats: docs=%d stats=%+v", format, len(res.docs), *plan.stats)
		}

		// Backward compatibility: storedDocumentMaterializerForCollection + storedDocumentToBSON must keep working for unowned callers.
		mat, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			t.Fatalf("storedDocumentMaterializerForCollection(%s): %v", format, err)
		}
		key, err := encodePrimaryKey(bson.Raw(res.docs[0]).Lookup("_id"))
		if err != nil {
			t.Fatalf("encodePrimaryKey: %v", err)
		}
		stored, err := col.Get(key)
		if err != nil {
			t.Fatalf("col.Get: %v", err)
		}
		decoded, err := storedDocumentToBSON(col, mat, stored)
		_ = mat.Close()
		if err != nil {
			t.Fatalf("storedDocumentToBSON(%s): %v", format, err)
		}
		if !bytes.Equal(decoded, res.docs[0]) {
			t.Fatalf("storedDocumentToBSON(%s) mismatch", format)
		}
	}

	// Verify missing-index parity for documentsForIndexedPredicate (returns empty, nil).
	bsonCol, err := servers[collections.DocumentFormatBSON].Collections.OpenCollection("app." + collName)
	if err != nil {
		t.Fatalf("OpenCollection(bson): %v", err)
	}
	missingView, err := bsonCol.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	missingPred, err := parseFindPredicates(mustMarshalWireDoc(t, bson.D{{Key: "ghost", Value: "abc"}}))
	if err != nil {
		_ = missingView.Close()
		t.Fatalf("parseFindPredicates: %v", err)
	}
	missingDec, err := newFindDocumentDecoder(bsonCol, collections.DocumentFormatBSON)
	if err != nil {
		_ = missingView.Close()
		t.Fatalf("newFindDocumentDecoder: %v", err)
	}
	missingDocs, err := documentsForIndexedPredicate(bsonCol, missingView, missingDec, missingPred[0], collections.IndexDefinition{
		Name:      "ghost_1",
		Field:     "ghost",
		ValueType: collections.IndexValueString,
	}, 10, 10)
	_ = missingDec.Close()
	_ = missingView.Close()
	if err != nil || len(missingDocs) != 0 {
		t.Fatalf("documentsForIndexedPredicate missing index got docs=%d err=%v, want 0, nil", len(missingDocs), err)
	}

	// Verify invalid hint still fails cleanly.
	badHintCmd := mustMarshalWireDoc(t, bson.D{
		{Key: "find", Value: collName},
		{Key: "$db", Value: "app"},
		{Key: "filter", Value: bson.D{{Key: "email", Value: "user-00012@example.com"}}},
		{Key: "hint", Value: "nonexistent_idx"},
	})
	resp := mustFindResponse(t, servers[collections.DocumentFormatBSON].Server, ctx, badHintCmd)
	if okVal, _ := bson.Raw(resp).Lookup("ok").AsInt64OK(); okVal != 0 {
		t.Fatalf("expected invalid hint error, got %s", bson.Raw(resp).String())
	}
}

func TestFindMetaCaptureSnapshotCoherenceConcurrentWrites(t *testing.T) {
	server, collName := openStandaloneCOWBSONFixture(t, collections.DocumentFormatBSON, false, 32)
	ctx := context.Background()

	// Warm cache before concurrent workload.
	warmCmd := mustMarshalWireDoc(t, bson.D{
		{Key: "find", Value: collName},
		{Key: "$db", Value: "app"},
		{Key: "filter", Value: bson.D{{Key: "email", Value: "user-00001@example.com"}}},
		{Key: "limit", Value: int32(1)},
		{Key: "singleBatch", Value: true},
	})
	_ = extractFirstBatchDocs(t, mustFindResponse(t, server.Server, ctx, warmCmd))

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	// Writer continuously rotates email and age on user-00000..user-00007.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 120; round++ {
			id := fmt.Sprintf("user-%05d", round%8)
			newEmail := fmt.Sprintf("rotated-%d-%d@example.com", round%8, round)
			newAge := int64(20 + (round % 10))
			updCmd := mustMarshalWireDoc(t, bson.D{
				{Key: "update", Value: collName},
				{Key: "$db", Value: "app"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: newEmail}, {Key: "age", Value: newAge}}}}},
				}}},
			})
			resp, err := server.Server.updateResponse(ctx, updCmd, nil)
			if err != nil {
				errCh <- fmt.Errorf("update err: %w", err)
				return
			}
			if okVal, _ := bson.Raw(resp).Lookup("ok").AsInt64OK(); okVal != 1 {
				errCh <- fmt.Errorf("update failed: %s", bson.Raw(resp).String())
				return
			}
		}
		stop.Store(true)
	}()

	// Readers continuously query by email and age while writes run.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; !stop.Load() || i < 40; i++ {
				targetAge := int64(20 + ((worker + i) % 10))
				ageCmd := mustMarshalWireDoc(t, bson.D{
					{Key: "find", Value: collName},
					{Key: "$db", Value: "app"},
					{Key: "filter", Value: bson.D{{Key: "age", Value: targetAge}}},
					{Key: "limit", Value: int32(16)},
					{Key: "singleBatch", Value: true},
				})
				resp, err := server.Server.findResponse(ctx, ageCmd, int64(worker+2))
				if err != nil {
					errCh <- fmt.Errorf("find age err: %w", err)
					return
				}
				arrVal := bson.Raw(resp).Lookup("cursor", "firstBatch")
				arr, ok := arrVal.ArrayOK()
				if !ok {
					errCh <- fmt.Errorf("find age bad response: %s", bson.Raw(resp).String())
					return
				}
				vals, err := arr.Values()
				if err != nil {
					errCh <- err
					return
				}
				for _, val := range vals {
					doc, ok := val.DocumentOK()
					if !ok {
						continue
					}
					if gotAge, ok := doc.Lookup("age").AsInt64OK(); !ok || gotAge != targetAge {
						errCh <- fmt.Errorf("torn age view: got=%d want=%d doc=%s", gotAge, targetAge, doc.String())
						return
					}
				}

				targetEmail := fmt.Sprintf("user-%05d@example.com", 8+(i%24))
				emailCmd := mustMarshalWireDoc(t, bson.D{
					{Key: "find", Value: collName},
					{Key: "$db", Value: "app"},
					{Key: "filter", Value: bson.D{{Key: "email", Value: targetEmail}}},
					{Key: "limit", Value: int32(1)},
					{Key: "singleBatch", Value: true},
				})
				resp, err = server.Server.findResponse(ctx, emailCmd, int64(worker+2))
				if err != nil {
					errCh <- fmt.Errorf("find email err: %w", err)
					return
				}
				arrVal = bson.Raw(resp).Lookup("cursor", "firstBatch")
				arr, ok = arrVal.ArrayOK()
				if !ok {
					errCh <- fmt.Errorf("find email bad response: %s", bson.Raw(resp).String())
					return
				}
				vals, err = arr.Values()
				if err != nil {
					errCh <- err
					return
				}
				if len(vals) != 1 {
					errCh <- fmt.Errorf("stable email %q returned %d docs want 1", targetEmail, len(vals))
					return
				}
				doc, _ := vals[0].DocumentOK()
				if gotEmail := doc.Lookup("email").StringValue(); gotEmail != targetEmail {
					errCh <- fmt.Errorf("torn email view: got=%q want=%q", gotEmail, targetEmail)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// After concurrent writes stop, repeated indexed reads must not leak COW leases/views/cuts.
	postWriteLeases := cowStatUint64(t, server, "treedb.cache.cow.external_leases")
	postWriteActiveCuts := cowStatUint64(t, server, "treedb.cache.cow.active_cuts")
	postWriteViews := cowStatUint64(t, server, "treedb.cache.cow.views")
	for i := 8; i < 32; i++ {
		emailCmd := mustMarshalWireDoc(t, bson.D{
			{Key: "find", Value: collName},
			{Key: "$db", Value: "app"},
			{Key: "filter", Value: bson.D{{Key: "email", Value: fmt.Sprintf("user-%05d@example.com", i)}}},
			{Key: "limit", Value: int32(1)},
			{Key: "singleBatch", Value: true},
		})
		if docs := extractFirstBatchDocs(t, mustFindResponse(t, server.Server, ctx, emailCmd)); len(docs) != 1 {
			t.Fatalf("post-write email find returned %d docs want 1", len(docs))
		}
	}
	afterLeases := cowStatUint64(t, server, "treedb.cache.cow.external_leases")
	afterActiveCuts := cowStatUint64(t, server, "treedb.cache.cow.active_cuts")
	afterViews := cowStatUint64(t, server, "treedb.cache.cow.views")
	if afterLeases > postWriteLeases || afterActiveCuts > postWriteActiveCuts || afterViews > postWriteViews {
		t.Fatalf("COW leak after concurrent workload reads: leases=%d(base %d) active_cuts=%d(base %d) views=%d(base %d)",
			afterLeases, postWriteLeases, afterActiveCuts, postWriteActiveCuts, afterViews, postWriteViews)
	}
}

func TestFindMetaCaptureHasIndexedRangePredicateEquivalence(t *testing.T) {
	indexes := []struct {
		name string
		idx  collections.IndexDefinition
	}{
		{name: "StringIdx", idx: collections.IndexDefinition{Name: "email_1", Field: "email", ValueType: collections.IndexValueString}},
		{name: "Int64Idx", idx: collections.IndexDefinition{Name: "age_1", Field: "age", ValueType: collections.IndexValueInt64}},
		{name: "DoubleIdx", idx: collections.IndexDefinition{Name: "score_1", Field: "score", ValueType: collections.IndexValueDouble}},
		{name: "BoolIdx", idx: collections.IndexDefinition{Name: "active_1", Field: "active", ValueType: collections.IndexValueBool}},
		{name: "BSONv2AscSingle", idx: collections.IndexDefinition{
			Name: "age_v2", Field: "age", ValueType: collections.IndexValueBSONOrderedV2,
			Components: []collections.IndexComponent{{Field: "age", Direction: collections.IndexDirectionAscending}},
		}},
		{name: "BSONv2DescSingle", idx: collections.IndexDefinition{
			Name: "age_v2_desc", Field: "age", ValueType: collections.IndexValueBSONOrderedV2,
			Components: []collections.IndexComponent{{Field: "age", Direction: collections.IndexDirectionDescending}},
		}},
		{name: "BSONv2Compound", idx: collections.IndexDefinition{
			Name: "age_email_v2", Field: "age", ValueType: collections.IndexValueBSONOrderedV2,
			Components: []collections.IndexComponent{
				{Field: "age", Direction: collections.IndexDirectionAscending},
				{Field: "email", Direction: collections.IndexDirectionAscending},
			},
		}},
	}

	filters := []struct {
		name   string
		filter bson.D
	}{
		{name: "Empty", filter: bson.D{}},
		{name: "EmailEq", filter: bson.D{{Key: "email", Value: "a@example.com"}}},
		{name: "EmailIn", filter: bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"a@example.com", "b@example.com"}}}}}},
		{name: "AgeGT", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(18)}}}}},
		{name: "AgeGTEAndLT", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(18)}, {Key: "$lt", Value: int64(65)}}}}},
		{name: "AgeLTE", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$lte", Value: int64(30)}}}}},
		{name: "AgeContradictory", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(50)}, {Key: "$lt", Value: int64(10)}}}}},
		{name: "AgeNaN", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: math.NaN()}}}}},
		{name: "AgeNullRange", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: nil}}}}},
		{name: "AgeNullEq", filter: bson.D{{Key: "age", Value: nil}}},
		{name: "AgeFractionalDouble", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 18.5}}}}},
		{name: "AgeNegatedGT", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: int64(18)}}}}}}},
		{name: "AgeWithOr", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(18)}}}, {Key: "$or", Value: bson.A{bson.D{{Key: "email", Value: "a@example.com"}}}}}},
		{name: "ScoreGT", filter: bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: 3.14}}}}},
		{name: "ActiveGT", filter: bson.D{{Key: "active", Value: bson.D{{Key: "$gte", Value: false}}}}},
	}

	baseCmd := mustMarshalWireDoc(t, bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}})
	for _, f := range filters {
		plan, err := parseFindPlan(baseCmd, mustMarshalWireDoc(t, f.filter))
		if err != nil {
			t.Fatalf("parseFindPlan(%s): %v", f.name, err)
		}
		for _, ix := range indexes {
			probes := findIndexProbesForIndex(plan, ix.idx)
			want := false
			for _, p := range probes {
				if p.stage == "secondary_range_lookup" {
					want = true
					break
				}
			}
			got := hasIndexedRangePredicateForIndex(plan, ix.idx)
			if got != want {
				t.Fatalf("filter=%s index=%s: hasIndexedRangePredicateForIndex=%v want %v (probes=%+v)", f.name, ix.name, got, want, probes)
			}
		}
	}

	// Verify zero allocations on equality queries.
	eqPlan, err := parseFindPlan(baseCmd, mustMarshalWireDoc(t, bson.D{{Key: "email", Value: "user-00001@example.com"}}))
	if err != nil {
		t.Fatalf("parseFindPlan(eq): %v", err)
	}
	emailIdx := indexes[0].idx
	allocs := testing.AllocsPerRun(1000, func() {
		if hasIndexedRangePredicateForIndex(eqPlan, emailIdx) {
			t.Fatal("unexpected range predicate on email eq")
		}
	})
	if allocs != 0 {
		t.Fatalf("hasIndexedRangePredicateForIndex allocated %.1f allocs/op on equality plan, want 0", allocs)
	}
}

func BenchmarkFindOneIndexedBSONStandaloneCOW(b *testing.B) {
	ctx := context.Background()

	runBench := func(b *testing.B, bsonV2EmailIndex bool, buildCmd func(coll string, i int) wire.Document) {
		const docCount = 1024
		server, collName := openStandaloneCOWBSONFixture(b, collections.DocumentFormatBSON, bsonV2EmailIndex, docCount)
		cmds := make([]wire.Document, docCount)
		for i := 0; i < docCount; i++ {
			cmds[i] = buildCmd(collName, i)
		}
		var dst []byte
		// Warm once before timer.
		var err error
		dst, err = server.Server.findMsgResponseInto(ctx, dst[:0], cmds[0], 1, 1, 1)
		if err != nil {
			b.Fatalf("warm findMsgResponseInto: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst, err = server.Server.findMsgResponseInto(ctx, dst[:0], cmds[i%docCount], int32(i+1), int32(i+1), 1)
			if err != nil {
				b.Fatalf("findMsgResponseInto: %v", err)
			}
		}
		b.StopTimer()
		_ = math.Abs(float64(len(dst)))
	}

	b.Run("ByEmail_StringIndex", func(b *testing.B) {
		runBench(b, false, func(coll string, i int) wire.Document {
			return mustMarshalWireDoc(b, bson.D{
				{Key: "find", Value: coll},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: fmt.Sprintf("user-%05d@example.com", i)}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			})
		})
	})

	b.Run("ByEmail_BSONOrderedV2Index", func(b *testing.B) {
		runBench(b, true, func(coll string, i int) wire.Document {
			return mustMarshalWireDoc(b, bson.D{
				{Key: "find", Value: coll},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "email", Value: fmt.Sprintf("user-%05d@example.com", i)}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			})
		})
	})

	b.Run("ByPrimaryID", func(b *testing.B) {
		runBench(b, false, func(coll string, i int) wire.Document {
			return mustMarshalWireDoc(b, bson.D{
				{Key: "find", Value: coll},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("user-%05d", i)}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			})
		})
	})

	b.Run("ByPrimaryID_Projected", func(b *testing.B) {
		runBench(b, false, func(coll string, i int) wire.Document {
			return mustMarshalWireDoc(b, bson.D{
				{Key: "find", Value: coll},
				{Key: "$db", Value: "app"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("user-%05d", i)}}},
				{Key: "projection", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "limit", Value: int32(1)},
				{Key: "singleBatch", Value: true},
			})
		})
	})
}
