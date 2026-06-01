package nativewire

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func BenchmarkNativewireCollectionInsertBatch(b *testing.B) {
	const batchSize = 32
	b.Run("direct_collection", func(b *testing.B) {
		_, col, _, cleanup := benchmarkCollection(b)
		defer cleanup()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			b.StartTimer()
			if _, err := col.InsertBatch(ids, docs); err != nil {
				b.Fatalf("InsertBatch: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_inproc", func(b *testing.B) {
		mgr, _, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		ctx := context.Background()
		client, clientCleanup, err := NewInProcessClient(ctx, server)
		if err != nil {
			b.Fatalf("NewInProcessClient: %v", err)
		}
		defer func() { _ = clientCleanup() }()
		handle, err := client.OpenCollection(ctx, "bench")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			b.StartTimer()
			if _, err := client.InsertBatchHandle(ctx, handle, collections.DocumentFormatJSON, ids, docs, AckVisible); err != nil {
				b.Fatalf("InsertBatch native: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_inproc_no_result", func(b *testing.B) {
		mgr, _, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		ctx := context.Background()
		client, clientCleanup, err := NewInProcessClient(ctx, server)
		if err != nil {
			b.Fatalf("NewInProcessClient: %v", err)
		}
		defer func() { _ = clientCleanup() }()
		handle, err := client.OpenCollection(ctx, "bench")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			b.StartTimer()
			if err := client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatJSON, ids, docs, AckVisible); err != nil {
				b.Fatalf("InsertBatch native no-result: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_direct_dispatch", func(b *testing.B) {
		mgr, col, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(b, state, server, "bench", col)
		var sink benchmarkFrameSink
		var sectionBuf [4]iwire.Section
		requestBody := make([]byte, 0, 4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			guard := benchmarkMutationGuard(b, server, "insert_batch", i)
			b.StartTimer()
			var err error
			requestBody, err = appendInsertBatchRequestBodyRefFlags(requestBody[:0], "", handle, true, collections.DocumentFormatJSON, ids, docs, AckVisible, 0, guard)
			if err != nil {
				b.Fatalf("append insert request: %v", err)
			}
			response := benchmarkDispatchRequest(b, server, state, &sink, requestBody)
			sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, server.limits)
			if err != nil {
				b.Fatalf("decode insert response: %v", err)
			}
			rawIDs, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
			if err != nil {
				b.Fatalf("insert response document_ids: %v", err)
			}
			if !ok {
				b.Fatalf("insert response missing document_ids")
			}
			if _, err := decodeByteVectorBorrowed(rawIDs, server.limits); err != nil {
				b.Fatalf("decode insert ids: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_direct_dispatch_no_result", func(b *testing.B) {
		mgr, col, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(b, state, server, "bench", col)
		var sink benchmarkFrameSink
		requestBody := make([]byte, 0, 4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			guard := benchmarkMutationGuard(b, server, "insert_batch", i)
			b.StartTimer()
			var err error
			requestBody, err = appendInsertBatchRequestBodyRefFlags(requestBody[:0], "", handle, true, collections.DocumentFormatJSON, ids, docs, AckVisible, iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta, guard)
			if err != nil {
				b.Fatalf("append insert request: %v", err)
			}
			_ = benchmarkDispatchRequest(b, server, state, &sink, requestBody)
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
}

func BenchmarkNativewireCollectionGetMany(b *testing.B) {
	const (
		docs      = 4096
		batchSize = 64
	)
	b.Run("direct_collection", func(b *testing.B) {
		_, col, _, cleanup := benchmarkCollection(b)
		defer cleanup()
		seedBenchmarkCollection(b, col, docs)
		ids := benchmarkStoredIDs(0, batchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, id := range ids {
				if _, err := col.Get(id); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_inproc", func(b *testing.B) {
		mgr, col, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		seedBenchmarkCollection(b, col, docs)
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		ctx := context.Background()
		client, clientCleanup, err := NewInProcessClient(ctx, server)
		if err != nil {
			b.Fatalf("NewInProcessClient: %v", err)
		}
		defer func() { _ = clientCleanup() }()
		handle, err := client.OpenCollection(ctx, "bench")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		ids := benchmarkStoredIDs(0, batchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := client.GetManyHandle(ctx, handle, ids); err != nil {
				b.Fatalf("GetMany native: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_direct_dispatch", func(b *testing.B) {
		mgr, col, db, cleanup := benchmarkCollection(b)
		defer cleanup()
		seedBenchmarkCollection(b, col, docs)
		server := NewServer(ServerOptions{Collections: mgr, Backend: db})
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(b, state, server, "bench", col)
		ids := benchmarkStoredIDs(0, batchSize)
		var sink benchmarkFrameSink
		var sectionBuf [4]iwire.Section
		requestBody := make([]byte, 0, 1024)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var err error
			requestBody, err = appendGetManyRequestBodyRef(requestBody[:0], "", handle, true, ids)
			if err != nil {
				b.Fatalf("append get_many request: %v", err)
			}
			response := benchmarkDispatchRequest(b, server, state, &sink, requestBody)
			sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, server.limits)
			if err != nil {
				b.Fatalf("decode get_many response: %v", err)
			}
			rawDocs, ok, err := singletonSection(sections, iwire.SectionDocuments)
			if err != nil {
				b.Fatalf("get_many response documents: %v", err)
			}
			if !ok {
				b.Fatalf("get_many response missing documents")
			}
			docs, err := decodeByteVectorBorrowed(rawDocs, server.limits)
			if err != nil {
				b.Fatalf("decode get_many docs: %v", err)
			}
			rawPresence, ok, err := singletonSection(sections, iwire.SectionPresenceBitmap)
			if err != nil {
				b.Fatalf("get_many response presence: %v", err)
			}
			if !ok {
				b.Fatalf("get_many response missing presence bitmap")
			}
			if _, err := decodePresenceBitmap(rawPresence, len(docs)); err != nil {
				b.Fatalf("decode presence bitmap: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
}

func BenchmarkNativewireRejectDuplicateIDs(b *testing.B) {
	for _, count := range []int{32, 128, 512} {
		ids := benchmarkStoredIDs(0, count)
		b.Run(fmt.Sprintf("%d_ids", count), func(b *testing.B) {
			if err := rejectDuplicateIDs(ids); err != nil {
				b.Fatalf("warm rejectDuplicateIDs: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rejectDuplicateIDs(ids); err != nil {
					b.Fatalf("rejectDuplicateIDs: %v", err)
				}
			}
		})
	}
}

type benchmarkFrameSink struct {
	frame []byte
}

func benchmarkConnState() *connState {
	return &connState{id: 1, hello: true}
}

func (w *benchmarkFrameSink) Write(p []byte) (int, error) {
	w.frame = append(w.frame, p...)
	return len(p), nil
}

func benchmarkDispatchRequest(tb testing.TB, server *Server, state *connState, sink *benchmarkFrameSink, body []byte) []byte {
	tb.Helper()
	response, err := benchmarkDispatchRequestError(server, state, sink, body)
	if err != nil {
		tb.Fatal(err)
	}
	return response
}

func benchmarkDispatchRequestError(server *Server, state *connState, sink *benchmarkFrameSink, body []byte) ([]byte, error) {
	sink.frame = sink.frame[:0]
	header := iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0},
		Type:      iwire.FrameRequest,
		RequestID: 1,
	}
	if err := server.handleFrame(context.Background(), sink, state, header, body); err != nil {
		return nil, fmt.Errorf("handleFrame: %w", err)
	}
	if len(sink.frame) < int(iwire.FrameHeaderLenV1) {
		return nil, fmt.Errorf("response frame too short: %d", len(sink.frame))
	}
	responseHeader, err := iwire.DecodeHeader(sink.frame[:iwire.FrameHeaderLenV1], server.limits)
	if err != nil {
		return nil, fmt.Errorf("decode response header: %w", err)
	}
	if responseHeader.Type == iwire.FrameError {
		return nil, fmt.Errorf("server returned error frame: %w", decodeWireError(sink.frame[iwire.FrameHeaderLenV1:], server.limits))
	}
	if responseHeader.Type != iwire.FrameResponse {
		return nil, fmt.Errorf("response type=%d want %d", responseHeader.Type, iwire.FrameResponse)
	}
	response := sink.frame[iwire.FrameHeaderLenV1:]
	if uint64(len(response)) != responseHeader.BodyLen {
		return nil, fmt.Errorf("response body len=%d want %d", len(response), responseHeader.BodyLen)
	}
	return response, nil
}

func benchmarkAddCollectionHandle(tb testing.TB, state *connState, server *Server, name string, col *collections.Collection) CollectionHandle {
	tb.Helper()
	handle, err := state.addCollectionHandle(name, col, server.maxCollectionHandles, server.maxCachedCollections)
	if err != nil {
		tb.Fatalf("add collection handle: %v", err)
	}
	return handle
}

func benchmarkCollection(tb testing.TB) (*collections.CollectionManager, *collections.Collection, *backenddb.DB, func()) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "bench",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
	}); err != nil {
		_ = db.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bench")
	if err != nil {
		_ = db.Close()
		tb.Fatalf("open collection: %v", err)
	}
	return mgr, col, db, func() { _ = db.Close() }
}

func benchmarkMutationGuard(tb testing.TB, server *Server, command string, seq int) []iwire.Section {
	tb.Helper()
	version, err := server.currentCatalogVersion()
	if err != nil {
		tb.Fatalf("current catalog version: %v", err)
	}
	return []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(fmt.Sprintf("bench/%s/%d", command, seq))},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
	}
}

func seedBenchmarkCollection(tb testing.TB, col *collections.Collection, docs int) {
	tb.Helper()
	const batchSize = 256
	for start := 0; start < docs; start += batchSize {
		n := batchSize
		if docs-start < n {
			n = docs - start
		}
		ids, values := benchmarkStoredBatch(start, n)
		if _, err := col.InsertBatch(ids, values); err != nil {
			tb.Fatalf("seed InsertBatch: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("seed Flush: %v", err)
	}
}

func benchmarkStoredBatch(start, count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		id := benchmarkStoredID(start + i)
		ids[i] = []byte(id)
		docs[i] = []byte(fmt.Sprintf(`{"email":"%s@example.com","city":"hnl","age":%d}`, id, 18+((start+i)%67)))
	}
	return ids, docs
}

func benchmarkStoredIDs(start, count int) [][]byte {
	ids := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(benchmarkStoredID(start + i))
	}
	return ids
}

func benchmarkStoredID(n int) string {
	return fmt.Sprintf("doc-%08d", n)
}
