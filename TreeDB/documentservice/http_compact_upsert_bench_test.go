package documentservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestHTTPCompactDeferredColumnGraphUsesValidatedFloat32Projection(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	svc.DiagnosticsHandler(nil)
	createBenchmarkColumnGraphIndex(t, svc, "compact_projection")
	handler := NewHandler(svc)
	var response UpsertDocumentsResponse
	postJSON(t, handler, "/v1/indexes/compact_projection/documents/upsert", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "compact", EmbeddingF32LEBase64: encodeFloat32LEBase64ForTest([]float32{1, 0})}},
		DeferVectorIndexRebuild: true,
	}, http.StatusOK, &response)
	if response.CompactEmbeddings != 1 || response.Inserted != 1 {
		t.Fatalf("response=%+v want one compact insert", response)
	}
	insertStats := svc.DiagnosticsSnapshot(nil).LastOpened
	if insertStats == nil || insertStats.Insert.ColumnPublishValidatedFloat32ProjectionRows != 1 || insertStats.Insert.ColumnPublishDocumentExtraction != 0 {
		t.Fatalf("compact insert stats=%+v want one validated projection row and zero document extraction", insertStats)
	}
}

func BenchmarkHTTPUpsertDecodePrepareNumericVsCompact(b *testing.B) {
	const (
		dimension = 768
		documents = 128
	)
	embedding := make([]float32, dimension)
	for i := range embedding {
		embedding[i] = float32(i%31+1) / 31
	}
	numericDocs := make([]Document, documents)
	compactDocs := make([]Document, documents)
	encoded := encodeFloat32LEBase64ForTest(embedding)
	for i := range numericDocs {
		id := fmt.Sprintf("doc-%03d", i)
		numericDocs[i] = Document{ID: id, Embedding: embedding}
		compactDocs[i] = Document{ID: id, EmbeddingF32LEBase64: encoded}
	}
	info := IndexInfo{Dimension: dimension, Metric: MetricCosine}
	for _, tc := range []struct {
		name string
		req  UpsertDocumentsRequest
	}{
		{name: "numeric_json", req: UpsertDocumentsRequest{Documents: numericDocs}},
		{name: "compact_f32le_base64", req: UpsertDocumentsRequest{Documents: compactDocs}},
	} {
		payload, err := json.Marshal(tc.req)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var req UpsertDocumentsRequest
				dec := json.NewDecoder(bytes.NewReader(payload))
				dec.UseNumber()
				dec.DisallowUnknownFields()
				if err := dec.Decode(&req); err != nil {
					b.Fatal(err)
				}
				if _, err := prepareDocumentsForWrite(req.Documents, info, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
