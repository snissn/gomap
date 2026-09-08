package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestDenseWorkGoldenStrictOwnershipAndAllocations(t *testing.T) {
	encoded, err := os.ReadFile("testdata/dense_work_v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	w, err := decodeDenseWork(raw)
	if err != nil || !w.Completed || w.Graph.Route != "typed_exact" || w.Graph.ExactBaseScored != 1 || w.Output.Fetched != 1 || w.Output.OutputBytes != 2 {
		t.Fatalf("golden=%+v err=%v", w, err)
	}
	got, err := appendDenseWork(nil, w)
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatalf("round trip: %x err=%v", got, err)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, err = decodeDenseWork(raw) }); err != nil || allocs != 0 {
		t.Fatalf("proof decode allocs=%g err=%v", allocs, err)
	}
	for size := range len(raw) {
		if _, err := decodeDenseWork(raw[:size]); err == nil {
			t.Fatalf("truncated proof accepted at %d", size)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(raw), 0), append([]byte{2}, raw[1:]...), append([]byte{0x81, 0}, raw[1:]...), append([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}, raw[1:]...)} {
		if _, err := decodeDenseWork(bad); err == nil {
			t.Fatal("malformed proof accepted")
		}
	}
	section := iwire.Section{ID: iwire.SectionDenseSearchWork, Bytes: raw}
	for _, sections := range [][]iwire.Section{nil, {section, section}, {section, {ID: 999, Flags: iwire.SectionFlagCritical}}} {
		if _, err := decodeDenseWorkSection(sections, true); err == nil {
			t.Fatal("missing/duplicate proof accepted")
		}
	}
	if _, err := decodeDenseWorkSection([]iwire.Section{section}, false); err == nil {
		t.Fatal("legacy version accepted typed proof")
	}
	before := w
	clear(raw)
	if w != before || w.Graph.Snapshot.BaseManifest.Format != "tcs1" {
		t.Fatal("proof borrowed input bytes")
	}
}

func TestDenseWorkNativeServiceAndEncodingErrors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("selected serving fixture requires Linux namespace authority and mmap")
	}
	server, _ := typedGetManyFixture(t)
	opts := collections.ColumnGraphServingOptions{
		Publication:     collections.ColumnGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 4096, OwnedBytes: 16 << 20, EncodedOutputBytes: 16 << 20},
		Owners:          collections.ColumnGraphReadOwnerLimits{Owners: 8, States: 8, StateBytes: 128 << 20, AssetBytes: 128 << 20, Cold: collections.ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 8 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1 << 30, AppenderAttempts: 4096},
		Maintenance:     collections.ColumnGraphMaintenanceLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 64 << 20, ManifestBytes: 8 << 20, RetainedBytes: 256 << 20, PagerPages: 32768},
		Filter:          collections.ColumnGraphFilterLimits{SourceIDs: 4096, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 100000, InspectedEntries: 4096}, FoldRows: 4096, SearchCandidates: 4096,
	}
	if _, err := server.documentService.OptimizeIndex(context.Background(), "batch", documentservice.OptimizeIndexRequest{ColumnGraphServing: &opts}); err != nil {
		t.Fatal(err)
	}
	query, err := appendDenseVectorSearchRequest(nil, DenseVectorSearchRequest{Index: "batch", Query: []float32{1, 0}, TopK: 1, ExpectedGeneration: 1}, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	sections := []iwire.Section{{ID: iwire.SectionDenseSearchRequest, Bytes: query}, {ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))}}
	body, err := server.handleVersionedDenseVectorSearch(context.Background(), &connState{}, 2, sections, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := iwire.DecodeSections(body, server.limits)
	work, err := decodeDenseWorkSection(decoded, true)
	if err != nil || !work.Completed || work.Graph.Route != "typed_hnsw" || work.Graph.BaseANNScored == 0 || work.Output.Fetched != 1 {
		t.Fatalf("real service work=%+v err=%v", work, err)
	}
	clientContext := denseSearchTestContext(t)
	client, cleanup, err := NewInProcessClient(clientContext, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := client.Hello(clientContext); err != nil {
		t.Fatal(err)
	}
	request := DenseVectorSearchRequest{Index: "batch", Query: []float32{1, 0}, TopK: 1, ExpectedGeneration: 1, TypedColumnGraph: true}
	first, err := client.DenseVectorSearch(clientContext, request)
	if err != nil {
		t.Fatal(err)
	}
	owned := first.DenseWork
	request.TopK = 2
	if _, err := client.DenseVectorSearch(clientContext, request); err != nil {
		t.Fatal(err)
	}
	if first.DenseWork != owned || first.DenseWork.Output.Fetched != 1 {
		t.Fatal("proof changed after next client round trip")
	}
	// Output limits reject only after the real service search/fetch completed.
	server.limits.MaxByteVectorBytes = 1
	partial, err := server.handleVersionedDenseVectorSearch(context.Background(), &connState{}, 2, sections, nil)
	var observed *denseWorkError
	if err == nil || len(partial) != 0 || !errors.As(err, &observed) || !observed.work.Completed || !observed.work.Graph.Completed || !observed.work.Output.Completed || observed.work.Output.Fetched != 1 {
		t.Fatalf("encoding failure lost service completion: %v", err)
	}
	var frame bytes.Buffer
	if err := server.writeError(&frame, iwire.Header{}, err); err != nil {
		t.Fatal(err)
	}
	_, payload, err := readFrame(&frame, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var remote *WireError
	if err := decodeWireError(payload, iwire.DefaultLimits(), true); !errors.As(err, &remote) || remote.DenseWork == nil || !remote.DenseWork.Completed {
		t.Fatalf("wire error lost owned details: %+v", err)
	}
	// The same error envelope carries an actual bounded-search prefix.
	t.Run("candidate_budget", func(t *testing.T) {
		bounded, _ := typedGetManyFixture(t)
		limited := opts
		limited.SearchCandidates = 1
		if _, err := bounded.documentService.OptimizeIndex(context.Background(), "batch", documentservice.OptimizeIndexRequest{ColumnGraphServing: &limited}); err != nil {
			t.Fatal(err)
		}
		partial, err := bounded.handleVersionedDenseVectorSearch(context.Background(), &connState{}, 2, sections, nil)
		if len(partial) != 0 || !errors.Is(err, collections.ErrColumnGraphSearchBudget) {
			t.Fatalf("expected candidate rejection: %v", err)
		}
		frame.Reset()
		if err := bounded.writeError(&frame, iwire.Header{}, err); err != nil {
			t.Fatal(err)
		}
		_, payload, err := readFrame(&frame, iwire.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		var remote *WireError
		if err := decodeWireError(payload, iwire.DefaultLimits(), true); !errors.As(err, &remote) || remote.DenseWork == nil {
			t.Fatalf("missing error proof: %v", err)
		}
		w := remote.DenseWork
		// Strict typed serving spends this single score on the upper entry;
		// no layer-zero candidate, edge or suffix score has run at rejection.
		if w.Completed || w.Graph.Completed || w.Graph.BaseANNScored != 1 || w.Graph.BaseCandidates != 0 || w.Graph.BaseEdges != 0 || w.Graph.DeltaScored != 0 || !w.Graph.Snapshot.Available || w.Output.Attempted {
			t.Fatalf("incorrect error prefix: %+v", w)
		}
		client, cleanup, err := NewInProcessClient(clientContext, bounded)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
		if err := client.Hello(clientContext); err != nil {
			t.Fatal(err)
		}
		_, err = client.DenseVectorSearch(clientContext, DenseVectorSearchRequest{Index: "batch", Query: []float32{1, 0}, TopK: 1, ExpectedGeneration: 1, TypedColumnGraph: true})
		if !errors.As(err, &remote) || remote.DenseWork == nil || *remote.DenseWork != *w {
			t.Fatalf("in-process typed error lost proof: %v", err)
		}
		genericBody, err := appendVersionedCommandRequestBody(nil, iwire.CommandDenseVectorSearch, 2, sections...)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = client.roundTrip(clientContext, iwire.FrameRequest, genericBody, iwire.FrameResponse)
		if errors.As(err, &remote) || nativeCodeOf(err) != iwire.ErrMalformedFrame {
			t.Fatalf("in-process generic call accepted dense proof: %v", err)
		}
	})
	// A v1 mismatch/error still has exactly the old error section.
	_, legacyErr := server.handleVersionedDenseVectorSearch(context.Background(), &connState{}, 1, sections, nil)
	frame.Reset()
	if err := server.writeError(&frame, iwire.Header{}, legacyErr); err != nil {
		t.Fatal(err)
	}
	_, payload, _ = readFrame(&frame, iwire.DefaultLimits())
	decoded, _ = iwire.DecodeSections(payload, iwire.DefaultLimits())
	if len(decoded) != 1 || decoded[0].ID != iwire.SectionError {
		t.Fatalf("legacy error gained proof: %+v", decoded)
	}
}

func TestDenseWorkErrorCallScope(t *testing.T) {
	encoded, err := os.ReadFile("testdata/dense_work_v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	proof, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	body, err := iwire.AppendSection(nil, iwire.Section{ID: iwire.SectionError, Bytes: appendErrorPayload(nil, iwire.ErrInternal, false, "original")})
	if err != nil {
		t.Fatal(err)
	}
	errorOnly := bytes.Clone(body)
	body, err = iwire.AppendSection(body, iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: proof})
	if err != nil {
		t.Fatal(err)
	}
	// Early typed errors retain the ordinary error; extensions remain optional.
	for _, raw := range [][]byte{errorOnly, body} {
		for _, flags := range []uint64{0, iwire.SectionFlagCritical} {
			extended, err := iwire.AppendSection(bytes.Clone(raw), iwire.Section{ID: 999, Flags: flags})
			if err != nil {
				t.Fatal(err)
			}
			got := decodeWireError(extended, iwire.DefaultLimits(), true)
			var remote *WireError
			if flags == 0 {
				if !errors.As(got, &remote) || remote.Code != iwire.ErrInternal || remote.Retryable || remote.Message != "original" || (remote.DenseWork != nil) != bytes.Equal(raw, body) {
					t.Fatalf("ordinary error changed: %v", got)
				}
			} else if nativeCodeOf(got) != iwire.ErrUnsupportedFeature {
				t.Fatalf("critical error extension accepted: %v", got)
			}
		}
	}
	for _, malformed := range []bool{false, true} {
		raw, content := bytes.Clone(body), proof
		if malformed {
			raw, content = bytes.Clone(errorOnly), []byte{1}
		}
		raw, err = iwire.AppendSection(raw, iwire.Section{ID: iwire.SectionDenseSearchWork, Bytes: content})
		if err != nil {
			t.Fatal(err)
		}
		var remote *WireError
		if got := decodeWireError(raw, iwire.DefaultLimits(), true); errors.As(got, &remote) || got == nil {
			t.Fatalf("bad proof accepted: %v", got)
		}
		if got := decodeWireError(raw, iwire.DefaultLimits(), false); nativeCodeOf(got) != iwire.ErrMalformedFrame {
			t.Fatalf("unrelated proof accepted: %v", got)
		}
	}
	for _, call := range []string{"hello", "get_many", "generic", "discard", "legacy_dense", "typed_dense"} {
		t.Run(call, func(t *testing.T) {
			left, right := net.Pipe()
			client := NewClient(left)
			defer client.Close()
			defer right.Close()
			client.denseTypedNegotiated = true
			done := make(chan error, 1)
			go func() {
				header, _, err := readFrame(right, iwire.DefaultLimits())
				if err == nil {
					err = writeFrame(right, iwire.Header{Type: iwire.FrameError, RequestID: header.RequestID}, body)
				}
				done <- err
			}()
			ctx := denseSearchTestContext(t)
			var got error
			switch call {
			case "hello":
				got = client.Hello(ctx)
			case "get_many":
				_, _, got = client.GetMany(ctx, "docs", [][]byte{[]byte("a")})
			case "generic":
				_, _, got = client.roundTrip(ctx, iwire.FrameRequest, nil, iwire.FrameResponse)
			case "discard":
				got = client.roundTripLockedDiscardResponse(ctx, iwire.FrameRequest, nil, iwire.FrameResponse)
			default:
				_, got = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1, TypedColumnGraph: call == "typed_dense"})
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			var remote *WireError
			if call == "typed_dense" {
				if !errors.As(got, &remote) || remote.DenseWork == nil || remote.Message != "original" {
					t.Fatalf("typed error=%v", got)
				}
			} else if errors.As(got, &remote) || nativeCodeOf(got) != iwire.ErrMalformedFrame {
				t.Fatalf("unrelated call accepted dense work: %v", got)
			}
		})
	}
}
