package collections

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

const (
	overheadBenchDefaultBatchSize = 8000
	overheadBenchCities           = 64
)

var overheadBenchPayload = []byte(`{"name":"ada","city":"hnl","email":"ada@example.com","pad":"0123456789012345678901234567890123456789"}`)

var templateV1BenchmarkSink []byte

func BenchmarkTemplateV1EncodeDocument(b *testing.B) {
	fields := []string{"name", "email", "city", "pad"}
	values := []any{"ada", "ada@example.com", "hnl", "0123456789012345678901234567890123456789"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		doc, err := EncodeTemplateV1Document(fields, values)
		if err != nil {
			b.Fatalf("encode template-v1 document: %v", err)
		}
		templateV1BenchmarkSink = doc
	}
}

func BenchmarkTemplateV1EncoderEncodeDocumentRepeatedShape(b *testing.B) {
	fields := []string{"name", "email", "city", "pad"}
	values := []any{"ada", "ada@example.com", "hnl", "0123456789012345678901234567890123456789"}
	var encoder TemplateV1Encoder
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		doc, err := encoder.EncodeDocument(fields, values)
		if err != nil {
			b.Fatalf("encode template-v1 document: %v", err)
		}
		templateV1BenchmarkSink = doc
	}
}

func overheadBenchBatchSize(b *testing.B) int {
	b.Helper()

	raw := strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_BATCH_SIZE"))
	if raw == "" {
		return overheadBenchDefaultBatchSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		b.Fatalf("unsupported TREEDB_COLLECTION_BENCH_BATCH_SIZE=%q", raw)
	}
	return n
}

func overheadBenchDocumentID(n int) []byte {
	out := make([]byte, 0, len("u-")+9)
	out = append(out, "u-"...)
	return appendOverheadBenchZeroPaddedInt(out, n, 9)
}

func overheadBenchIndexedDocument(n int) []byte {
	out := make([]byte, 0, 112)
	out = append(out, `{"name":"user-`...)
	out = appendOverheadBenchZeroPaddedInt(out, n, 9)
	out = append(out, `","email":"user-`...)
	out = appendOverheadBenchZeroPaddedInt(out, n, 9)
	out = append(out, `@example.com","city":"city-`...)
	out = appendOverheadBenchZeroPaddedInt(out, n%overheadBenchCities, 2)
	out = append(out, `","pad":"01234567890123456789"}`...)
	return out
}

func overheadBenchTemplateIndexedDocument(b testing.TB, encoder *TemplateV1Encoder, n int) []byte {
	b.Helper()
	if encoder == nil {
		encoder = &TemplateV1Encoder{}
	}
	doc, err := encoder.EncodeDocument(
		[]string{"name", "email", "city", "pad"},
		[]any{
			fmt.Sprintf("user-%09d", n),
			fmt.Sprintf("user-%09d@example.com", n),
			fmt.Sprintf("city-%02d", n%overheadBenchCities),
			"01234567890123456789",
		},
	)
	if err != nil {
		b.Fatalf("encode template-v1 indexed document: %v", err)
	}
	return doc
}

func appendOverheadBenchZeroPaddedInt(dst []byte, n, width int) []byte {
	var scratch [20]byte
	pos := len(scratch)
	if n == 0 {
		pos--
		scratch[pos] = '0'
	} else {
		for n > 0 {
			pos--
			scratch[pos] = byte('0' + n%10)
			n /= 10
		}
	}
	for pad := width - (len(scratch) - pos); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, scratch[pos:]...)
}

func overheadBenchDocumentBatch(count int, indexed bool) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = overheadBenchDocumentID(i)
		if indexed {
			docs[i] = overheadBenchIndexedDocument(i)
		} else {
			docs[i] = overheadBenchPayload
		}
	}
	return ids, docs
}

func overheadBenchTemplateDocumentBatch(b testing.TB, count int, indexed bool) ([][]byte, [][]byte) {
	b.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	var templateEncoder TemplateV1Encoder
	for i := 0; i < count; i++ {
		ids[i] = overheadBenchDocumentID(i)
		if indexed {
			docs[i] = overheadBenchTemplateIndexedDocument(b, &templateEncoder, i)
		} else {
			doc, err := templateEncoder.EncodeDocument(
				[]string{"name", "city", "email", "pad"},
				[]any{"ada", "hnl", "ada@example.com", "0123456789012345678901234567890123456789"},
			)
			if err != nil {
				b.Fatalf("encode template-v1 payload: %v", err)
			}
			docs[i] = doc
		}
	}
	return ids, docs
}

func overheadBenchIndexedPlanner() insertBatchPlanner {
	return insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email_idx", field: "email", valueType: IndexValueString, unique: true},
			{name: "city_idx", field: "city", valueType: IndexValueString},
		},
	}
}

func overheadBenchTemplateIndexedPlanner() insertBatchPlanner {
	planner := overheadBenchIndexedPlanner()
	planner.options.documentFormat = DocumentFormatTemplateV1
	return planner
}

func requireOverheadBenchIndexStateValues(b *testing.B, state orderedDocumentIndexState, runtimes []indexRuntime) {
	b.Helper()
	if len(state) != len(runtimes) {
		b.Fatalf("index state len=%d want=%d", len(state), len(runtimes))
	}
	for runtimeIdx, runtime := range runtimes {
		values := state.valuesAt(runtimeIdx)
		if len(values) == 0 {
			b.Fatalf("index %q extracted no values", runtime.def.name)
		}
		for valueIdx, value := range values {
			if len(value) == 0 {
				b.Fatalf("index %q value %d is empty", runtime.def.name, valueIdx)
			}
		}
	}
}

func BenchmarkCollectionOverheadPlanNoIndex(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	ids, docs := overheadBenchDocumentBatch(batchSize, false)
	planner := insertBatchPlanner{collection: "users"}

	b.ReportAllocs()
	b.ReportMetric(float64(batchSize), "target_docs/batch")
	b.ResetTimer()
	for planned := 0; planned < b.N; {
		n := batchSize
		if remaining := b.N - planned; remaining < n {
			n = remaining
		}
		plan, err := planner.planInsertBatch(ids[:n], docs[:n])
		if err != nil {
			b.Fatalf("plan no-index batch: %v", err)
		}
		resetCollectionRunTables(plan.runs)
		planned += n
	}
}

func BenchmarkCollectionOverheadPlanIndexed(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	ids, docs := overheadBenchDocumentBatch(batchSize, true)
	planner := overheadBenchIndexedPlanner()

	b.ReportAllocs()
	b.ReportMetric(float64(batchSize), "target_docs/batch")
	b.ResetTimer()
	for planned := 0; planned < b.N; {
		n := batchSize
		if remaining := b.N - planned; remaining < n {
			n = remaining
		}
		plan, err := planner.planInsertBatch(ids[:n], docs[:n])
		if err != nil {
			b.Fatalf("plan indexed batch: %v", err)
		}
		resetCollectionRunTables(plan.runs)
		planned += n
	}
}

func BenchmarkCollectionOverheadPlanIndexedTemplateV1(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	ids, docs := overheadBenchTemplateDocumentBatch(b, batchSize, true)
	planner := overheadBenchTemplateIndexedPlanner()

	b.ReportAllocs()
	b.ReportMetric(float64(batchSize), "target_docs/batch")
	b.ResetTimer()
	for planned := 0; planned < b.N; {
		n := batchSize
		if remaining := b.N - planned; remaining < n {
			n = remaining
		}
		plan, err := planner.planInsertBatch(ids[:n], docs[:n])
		if err != nil {
			b.Fatalf("plan template-v1 indexed batch: %v", err)
		}
		resetCollectionRunTables(plan.runs)
		planned += n
	}
}

func BenchmarkCollectionOverheadPlanDirectBufferedIndexedSingle(b *testing.B) {
	ids, docs := overheadBenchDocumentBatch(1, true)
	planner := overheadBenchIndexedPlanner()
	planner.buildPrimaryVal = clonePrimaryDocument
	planner.directBufferedRuns = true

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		plan, err := planner.planInsertBatch(ids, docs)
		if err != nil {
			b.Fatalf("plan direct-buffered indexed batch: %v", err)
		}
		resetCollectionRunTables(plan.runs)
	}
}

func BenchmarkCollectionOverheadPlanDirectBufferedIndexedTemplateV1Single(b *testing.B) {
	ids, docs := overheadBenchTemplateDocumentBatch(b, 1, true)
	planner := overheadBenchTemplateIndexedPlanner()
	planner.buildPrimaryVal = clonePrimaryDocument
	planner.directBufferedRuns = true

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		plan, err := planner.planInsertBatch(ids, docs)
		if err != nil {
			b.Fatalf("plan direct-buffered template-v1 batch: %v", err)
		}
		resetCollectionRunTables(plan.runs)
	}
}

func BenchmarkCollectionOverheadIndexStateJSONExtraction(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	_, docs := overheadBenchDocumentBatch(batchSize, true)
	planner := overheadBenchIndexedPlanner()
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		b.Fatalf("index runtimes: %v", err)
	}
	for _, doc := range docs {
		state, err := orderedIndexStateForDocument(doc, runtimes, collectionOptions{})
		if err != nil {
			b.Fatalf("validate index extraction inputs: %v", err)
		}
		requireOverheadBenchIndexStateValues(b, state, runtimes)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(len(runtimes)), "indexes/doc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state, err := orderedIndexStateForDocument(docs[i%batchSize], runtimes, collectionOptions{})
		if err != nil {
			b.Fatalf("extract index state: %v", err)
		}
		_ = state
	}
}

func BenchmarkCollectionOverheadIndexStateTemplateV1Extraction(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	_, docs := overheadBenchTemplateDocumentBatch(b, batchSize, true)
	planner := overheadBenchTemplateIndexedPlanner()
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		b.Fatalf("index runtimes: %v", err)
	}
	storedDocs, _, _, resolver, err := prepareTemplateV1InsertDocuments(docs, nil, false, true)
	if err != nil {
		b.Fatalf("prepare template-v1 documents: %v", err)
	}
	opts := collectionOptions{documentFormat: DocumentFormatTemplateV1, templateResolver: resolver}
	for _, doc := range storedDocs {
		state, err := orderedIndexStateForDocument(doc, runtimes, opts)
		if err != nil {
			b.Fatalf("validate template-v1 index extraction inputs: %v", err)
		}
		requireOverheadBenchIndexStateValues(b, state, runtimes)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(len(runtimes)), "indexes/doc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state, err := orderedIndexStateForDocument(storedDocs[i%batchSize], runtimes, opts)
		if err != nil {
			b.Fatalf("extract template-v1 index state: %v", err)
		}
		_ = state
	}
}

func BenchmarkCollectionOverheadPlanIndexedPrecomputedState(b *testing.B) {
	batchSize := overheadBenchBatchSize(b)
	ids, docs := overheadBenchDocumentBatch(batchSize, true)
	planner := overheadBenchIndexedPlanner()
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		b.Fatalf("index runtimes: %v", err)
	}
	states := make([]orderedDocumentIndexState, len(docs))
	for i := range docs {
		state, err := orderedIndexStateForDocument(docs[i], runtimes, collectionOptions{})
		if err != nil {
			b.Fatalf("precompute index state: %v", err)
		}
		requireOverheadBenchIndexStateValues(b, state, runtimes)
		states[i] = state
	}

	b.ReportAllocs()
	b.ReportMetric(float64(batchSize), "target_docs/batch")
	b.ResetTimer()
	for planned := 0; planned < b.N; {
		n := batchSize
		if remaining := b.N - planned; remaining < n {
			n = remaining
		}
		if err := overheadBenchPlanIndexedPrecomputedState(planner, runtimes, ids[:n], docs[:n], states[:n]); err != nil {
			b.Fatalf("plan indexed precomputed batch: %v", err)
		}
		planned += n
	}
}

func overheadBenchPlanIndexedPrecomputedState(
	planner insertBatchPlanner,
	runtimes []indexRuntime,
	ids, documents [][]byte,
	states []orderedDocumentIndexState,
) error {
	if len(ids) != len(documents) || len(ids) != len(states) {
		return fmt.Errorf("collections: precomputed batch length mismatch")
	}
	if planner.primaryRoot == "" {
		planner.primaryRoot = planner.collection + "/primary"
	}
	if planner.indexStateRoot == "" {
		planner.indexStateRoot = planner.collection + "/index-state"
	}
	if planner.buildPrimaryVal == nil {
		planner.buildPrimaryVal = borrowPrimaryDocument
	}

	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return err
	}
	items := make([]insertBatchItem, len(documents))
	for i := range documents {
		id := resultIDs[i]
		items[i] = insertBatchItem{
			id:       id,
			document: documents[i],
			state:    states[i],
		}
	}

	primaryOrder := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	if err := rejectDuplicateDocumentIDs(items, primaryOrder); err != nil {
		return err
	}
	uniqueProbes := make([]uniqueProbeCandidate, 0, len(items))
	for i := range items {
		for runtimeIdx, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			for _, encoded := range items[i].state.valuesAt(runtimeIdx) {
				uniqueProbes = append(uniqueProbes, uniqueProbeCandidate{
					indexName:    runtime.def.name,
					encodedValue: encoded,
					documentID:   items[i].id,
				})
			}
		}
	}
	sortUniqueProbeCandidates(uniqueProbes)
	if err := rejectDuplicateUniqueProbeCandidates(uniqueProbes); err != nil {
		return err
	}

	plan := &insertBatchPlan{
		resultIDs: resultIDs,
	}
	if err := planner.emitPrimaryRun(plan, items, primaryOrder); err != nil {
		return err
	}
	if persistIndexStateForOptions(planner.options) {
		if err := planner.emitIndexStateRun(plan, items, runtimes); err != nil {
			return err
		}
	}
	if err := planner.emitSecondaryRuns(plan, items, runtimes, primaryOrder); err != nil {
		return err
	}
	resetCollectionRunTables(plan.runs)
	return nil
}
