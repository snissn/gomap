package nativewire

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

var (
	vectorPartitionBenchmarkRequestSink  public.SearchRequestV1
	vectorPartitionBenchmarkResponseSink public.SearchResponseV1
	vectorPartitionBenchmarkEvidenceSink public.FastSearchEvidenceV1
)

func TestVectorPartitionWireV1RoundTrip(t *testing.T) {
	limits := iwire.DefaultLimits()
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7}, Query: []float32{1.25, -2.5},
		Metric: public.MetricCosineV1, TopK: 10, Probes: 2, EfSearch: 128, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 11, CandidateBytes: 22, ResponseBytes: 33, MergeEntries: 44}, Deadline: time.Unix(0, 123456789),
	}
	body, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	raw := vectorPartitionTestSectionV1(t, body, iwire.SectionVectorSearchRequest, limits)
	gotRequest, err := decodeVectorPartitionSearchRequestV1(raw, limits)
	if err != nil || !reflect.DeepEqual(gotRequest, request) {
		t.Fatalf("request round trip = %+v, err=%v", gotRequest, err)
	}
	queryScratch := make([]float32, len(request.Query))
	gotRequest, err = decodeVectorPartitionSearchRequestIntoV1(raw, limits, queryScratch[:0])
	if err != nil || &gotRequest.Query[0] != &queryScratch[0] {
		t.Fatalf("request decoder did not reuse query scratch: %v", err)
	}

	response := public.SearchResponseV1{
		Generation: request.Generation,
		Neighbors:  []public.NeighborV1{{ID: "doc-000001", Score: 0.25}, {ID: "doc-000002", Score: 0.5}},
		Counters: public.SearchCountersV1{
			SelectedPartitions: 1, SelectedGroups: 2, Requests: 3, RPCs: 4, Retries: 5, Redirects: 6, Candidates: 7, Edges: 8,
			SnapshotPins: 9, ReadProofs: 10, GenerationPins: 11, PartitionOpens: 12, QueryBytes: 13, RequestBytes: 14, CandidateBytes: 15, ResponseBytes: 16,
		},
		Timing: public.SearchTimingV1{
			Admission: 1, OperationsHealth: 2, ServiceAdapter: 3, PublicAdapter: 4, RouterOpen: 5, RouterSearch: 6, Placement: 7,
			CoordinatorLifecycle: 8, Dispatch: 9, Queue: 10, RPC: 11, Network: 12, ReadIndexApply: 13, GenerationOpen: 14,
			ShardSearch: 15, Response: 16, Dedupe: 17, Merge: 18, CoordinatorTotal: 19, Total: 20,
		},
	}
	body, err = appendVectorPartitionSearchResponseSectionV1(nil, response)
	if err != nil {
		t.Fatal(err)
	}
	raw = vectorPartitionTestSectionV1(t, body, iwire.SectionVectorSearchResponse, limits)
	gotResponse, err := decodeVectorPartitionSearchResponseV1(raw, limits)
	if err != nil || !reflect.DeepEqual(gotResponse, response) {
		t.Fatalf("response round trip = %+v, err=%v", gotResponse, err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if gotResponse.Neighbors[0].ID != "doc-000001" {
		t.Fatal("decoded response aliases the reusable frame buffer")
	}

	evidence := public.FastSearchEvidenceV1{
		Generation: request.Generation, IndexedThrough: 99, PublishedAt: time.Unix(0, 987654321), IndexAge: 3 * time.Millisecond,
		TopologyDigest: "topology", AuthorizationOverlayDigest: "authorization",
	}
	body, err = appendVectorPartitionFastEvidenceSectionV1(nil, evidence)
	if err != nil {
		t.Fatal(err)
	}
	gotEvidence, err := decodeVectorPartitionFastEvidenceV1(vectorPartitionTestSectionV1(t, body, iwire.SectionVectorFastEvidence, limits))
	if err != nil || !reflect.DeepEqual(gotEvidence, evidence) {
		t.Fatalf("evidence round trip = %+v, err=%v", gotEvidence, err)
	}
	raw = vectorPartitionTestSectionV1(t, body, iwire.SectionVectorFastEvidence, limits)
	gotEvidence, err = decodeVectorPartitionFastEvidenceV1(raw)
	for i := range raw {
		raw[i] = 0
	}
	if err != nil || !reflect.DeepEqual(gotEvidence, evidence) {
		t.Fatal("decoded evidence aliases the reusable frame buffer")
	}

	status := VectorPartitionStatusV1{NodeConfigSHA256: "config", Health: public.OperationsHealthV1{Ready: true, State: public.GenerationActiveV1, Generation: request.Generation, Reason: "ready"}}
	body, err = appendVectorPartitionStatusSectionV1(nil, status)
	if err != nil {
		t.Fatal(err)
	}
	gotStatus, err := decodeVectorPartitionStatusV1(vectorPartitionTestSectionV1(t, body, iwire.SectionVectorStatus, limits))
	if err != nil || !reflect.DeepEqual(gotStatus, status) {
		t.Fatalf("status round trip = %+v, err=%v", gotStatus, err)
	}
	raw = vectorPartitionTestSectionV1(t, body, iwire.SectionVectorStatus, limits)
	gotStatus, err = decodeVectorPartitionStatusV1(raw)
	for i := range raw {
		raw[i] = 0
	}
	if err != nil || !reflect.DeepEqual(gotStatus, status) {
		t.Fatal("decoded status aliases the reusable frame buffer")
	}
}

func TestValidateVectorPartitionFastEvidenceV1(t *testing.T) {
	generation := public.GenerationIDV1{Index: "fixture", Generation: 7}
	options := public.FastSearchOptionsV1{MaxIndexAge: time.Second, MinIndexedThrough: 9}
	valid := public.FastSearchEvidenceV1{
		Generation: generation, IndexedThrough: 9, PublishedAt: time.Now(), IndexAge: time.Millisecond,
		TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay",
	}
	if err := validateVectorPartitionFastEvidenceV1(valid, generation, options); err != nil {
		t.Fatalf("valid evidence: %v", err)
	}
	for name, test := range map[string]struct {
		mutate func(*public.FastSearchEvidenceV1)
		code   iwire.ErrorCode
	}{
		"generation":    {func(e *public.FastSearchEvidenceV1) { e.Generation.Generation++ }, iwire.ErrCatalogChanged},
		"watermark":     {func(e *public.FastSearchEvidenceV1) { e.IndexedThrough-- }, iwire.ErrConsistencyUnavailable},
		"publication":   {func(e *public.FastSearchEvidenceV1) { e.PublishedAt = time.Time{} }, iwire.ErrMalformedFrame},
		"age":           {func(e *public.FastSearchEvidenceV1) { e.IndexAge = 2 * time.Second }, iwire.ErrConsistencyUnavailable},
		"topology":      {func(e *public.FastSearchEvidenceV1) { e.TopologyDigest = "" }, iwire.ErrMalformedFrame},
		"authorization": {func(e *public.FastSearchEvidenceV1) { e.AuthorizationOverlayDigest = "" }, iwire.ErrMalformedFrame},
	} {
		t.Run(name, func(t *testing.T) {
			got := valid
			test.mutate(&got)
			if err := validateVectorPartitionFastEvidenceV1(got, generation, options); nativeCodeOf(err) != test.code {
				t.Fatalf("evidence error = %v, want code %d", err, test.code)
			}
		})
	}
}

func TestVectorPartitionWireV1RejectsMalformedAndOversized(t *testing.T) {
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 1}, Query: []float32{1, 2}, Metric: public.MetricCosineV1,
		TopK: 1, Probes: 1, EfSearch: 1, Consistency: public.ConsistencyGenerationSnapshotV1,
	}
	limits := iwire.DefaultLimits()
	limits.MaxByteVectorItems = 1
	if _, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits); err == nil {
		t.Fatal("oversized vector request encoded")
	}
	limits = iwire.DefaultLimits()
	body, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	raw := vectorPartitionTestSectionV1(t, body, iwire.SectionVectorSearchRequest, limits)
	if _, err := decodeVectorPartitionSearchRequestV1(raw[:len(raw)-1], limits); err == nil {
		t.Fatal("truncated vector request decoded")
	}
	if _, err := decodeVectorPartitionSearchRequestV1(append(append([]byte(nil), raw...), 0), limits); err == nil {
		t.Fatal("vector request with trailing bytes decoded")
	}
	_, versionBytes := binary.Uvarint(raw)
	overflowVersion := binary.AppendUvarint(nil, uint64(math.MaxUint32)+2)
	overflowVersion = append(overflowVersion, raw[versionBytes:]...)
	if _, err := decodeVectorPartitionSearchRequestV1(overflowVersion, limits); err == nil {
		t.Fatal("overflowing vector request version decoded")
	}
	unsupportedVersion := append([]byte(nil), raw...)
	unsupportedVersion[0] = 2
	if _, err := decodeVectorPartitionSearchRequestV1(unsupportedVersion, limits); nativeCodeOf(err) != iwire.ErrUnsupportedVersion {
		t.Fatalf("unsupported vector request version = %v", err)
	}
	empty := request
	empty.Query = nil
	if _, err := appendVectorPartitionSearchRequestSectionV1(nil, empty, limits); err == nil {
		t.Fatal("empty vector query encoded")
	}
	off := versionBytes
	indexLen, n := binary.Uvarint(raw[off:])
	off += n + int(indexLen)
	_, n = binary.Uvarint(raw[off:])
	off += n
	queryCount, n := binary.Uvarint(raw[off:])
	emptyRaw := append([]byte(nil), raw[:off]...)
	emptyRaw = append(emptyRaw, 0)
	emptyRaw = append(emptyRaw, raw[off+n+int(queryCount)*4:]...)
	if _, err := decodeVectorPartitionSearchRequestV1(emptyRaw, limits); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("empty vector query decoded: %v", err)
	}
	if _, err := decodeVectorPartitionFastOptionsV1([]byte{0, 0}); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("zero fast max age decoded: %v", err)
	}
	if _, err := decodeVectorPartitionPinOptionsV1([]byte{1, 0, 0}); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("zero pin session age decoded: %v", err)
	}
	badResponse := appendString(nil, "embedding")
	badResponse = binary.AppendUvarint(badResponse, 1)
	badResponse = binary.AppendUvarint(badResponse, 100)
	if _, err := decodeVectorPartitionSearchResponseV1(badResponse, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("impossible neighbor count decoded: %v", err)
	}
	if _, err := appendVectorPartitionSearchResponseSectionV1(nil, public.SearchResponseV1{Timing: public.SearchTimingV1{Total: -1}}); err == nil {
		t.Fatal("negative vector timing encoded")
	}
}

func TestVectorPartitionNativeWireErrorMappingV1(t *testing.T) {
	secret := errors.New("secret /srv/database path")
	err := vectorPartitionServerErrorV1(secret)
	if nativeCodeOf(err) != iwire.ErrInternal || errors.Is(err, secret) || err.Error() != "nativewire: error code 17: vector operation failed" {
		t.Fatalf("untyped server error leaked: %v", err)
	}
	var publicErr *public.ErrorV1
	if err := vectorPartitionClientErrorV1(&WireError{Code: iwire.ErrResourceExhausted, Message: "busy"}); !errors.As(err, &publicErr) || publicErr.Code != public.ErrorUnavailableV1 {
		t.Fatalf("resource exhaustion mapping = %v", err)
	}
	client := &Client{limits: iwire.DefaultLimits()}
	if _, err := client.VectorStatusV1(context.Background()); !errors.As(err, &publicErr) || publicErr.Code != public.ErrorInvalidRequestV1 {
		t.Fatalf("missing local deadline mapping = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := client.VectorSearchStrictV1(ctx, public.SearchRequestV1{}); !errors.As(err, &publicErr) || publicErr.Code != public.ErrorInvalidRequestV1 {
		t.Fatalf("local request validation mapping = %v", err)
	}
}

func TestVectorPartitionNativeWirePinCleanupV1(t *testing.T) {
	for _, test := range []struct {
		name     string
		evidence public.FastSearchEvidenceV1
		blocked  bool
	}{
		{name: "encode_failure", evidence: public.FastSearchEvidenceV1{Generation: public.GenerationIDV1{Index: "embedding", Generation: 1}, PublishedAt: time.Unix(-1, 0), IndexAge: time.Nanosecond, TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay"}},
		{name: "concurrent_close", evidence: public.FastSearchEvidenceV1{Generation: public.GenerationIDV1{Index: "embedding", Generation: 1}, PublishedAt: time.Now(), IndexAge: time.Nanosecond, TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay"}, blocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &vectorPartitionWirePinBackendV1{evidence: test.evidence, started: make(chan struct{}), release: make(chan struct{})}
			if !test.blocked {
				close(backend.release)
			}
			service, err := public.NewServiceV1(backend)
			if err != nil {
				t.Fatal(err)
			}
			config := public.ConservativeOperationsConfigV1()
			config.Enabled = true
			operations, err := public.NewOperationsV1(service, config, func(context.Context) (public.OperationsHealthV1, error) { return public.OperationsHealthV1{}, nil })
			if err != nil {
				t.Fatal(err)
			}
			server := NewServer(ServerOptions{VectorPartitionOperations: operations})
			client, _, err := NewInProcessClient(t.Context(), server)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			pinDone := make(chan error, 1)
			go func() {
				_, err := client.VectorPinSearchSnapshotV1(ctx, public.PinSearchSnapshotOptionsV1{FastSearchOptionsV1: public.FastSearchOptionsV1{MaxIndexAge: time.Second}, MaxSessionAge: time.Second})
				pinDone <- err
			}()
			<-backend.started
			if test.blocked {
				closeDone := make(chan error, 1)
				go func() { closeDone <- client.Close() }()
				select {
				case <-closeDone:
					t.Fatal("connection closed while pin publication was in flight")
				case <-time.After(10 * time.Millisecond):
				}
				close(backend.release)
				if err := <-closeDone; err != nil {
					t.Fatal(err)
				}
			}
			if err := <-pinDone; err == nil {
				t.Fatal("failed pin publication succeeded")
			}
			if backend.snapshot == nil || !backend.snapshot.closed {
				t.Fatal("failed pin publication retained the snapshot")
			}
		})
	}
}

type vectorPartitionWirePinBackendV1 struct {
	public.BackendV1
	evidence public.FastSearchEvidenceV1
	started  chan struct{}
	release  chan struct{}
	snapshot *vectorPartitionWirePinSnapshotV1
}

func (b *vectorPartitionWirePinBackendV1) PinVectorPartitionSearchSnapshotV1(context.Context, public.PinSearchSnapshotOptionsV1) (public.SearchSnapshotBackendV1, public.FastSearchEvidenceV1, error) {
	close(b.started)
	<-b.release
	b.snapshot = new(vectorPartitionWirePinSnapshotV1)
	return b.snapshot, b.evidence, nil
}

type vectorPartitionWirePinSnapshotV1 struct{ closed bool }

func (*vectorPartitionWirePinSnapshotV1) SearchVectorPartitionV1(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
	return public.SearchResponseV1{}, errors.New("unused")
}
func (s *vectorPartitionWirePinSnapshotV1) Close() error { s.closed = true; return nil }

func TestVectorPartitionNativeWirePropagatesRequestDeadlineV1(t *testing.T) {
	observed := make(chan error, 1)
	config := public.ConservativeOperationsConfigV1()
	config.Enabled = true
	operations, err := public.NewOperationsV1(new(public.ServiceV1), config, func(ctx context.Context) (public.OperationsHealthV1, error) {
		<-ctx.Done()
		observed <- ctx.Err()
		return public.OperationsHealthV1{}, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(ServerOptions{VectorPartitionOperations: operations})
	deadline := time.Now().Add(20 * time.Millisecond)
	missingDeadline, err := appendVectorPartitionCommandBodyV1(nil, iwire.CommandVectorStatus, nil, nil, nil, time.Time{}, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	missingSections, err := iwire.DecodeSections(missingDeadline, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.registry.ValidateRequestSections(missingSections); err == nil {
		t.Fatal("vector command without deadline was accepted")
	}
	zeroDeadline, err := appendCommandHeaderSection(nil, iwire.CommandVectorStatus)
	if err == nil {
		zeroDeadline, err = iwire.AppendSection(zeroDeadline, iwire.Section{ID: iwire.SectionDeadline, Bytes: []byte{0}})
	}
	if err != nil {
		t.Fatal(err)
	}
	zeroSections, err := iwire.DecodeSections(zeroDeadline, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	zeroCommand, err := server.registry.ValidateRequestSections(zeroSections)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.handleVectorPartitionCommandV1(context.Background(), nil, zeroCommand, nil); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("zero vector deadline = %v", err)
	}
	body, err := appendVectorPartitionCommandBodyV1(nil, iwire.CommandVectorStatus, nil, nil, nil, deadline, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	sections, err := iwire.DecodeSections(body, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	command, err := server.registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan error, 1)
	go func() {
		_, err := server.handleVectorPartitionCommandV1(context.Background(), nil, command, nil)
		handled <- err
	}()
	select {
	case err := <-observed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("server operation context = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server operation ignored the request deadline")
	}
	select {
	case err := <-handled:
		if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrTimeout {
			t.Fatalf("deadline result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server retained the timed-out request")
	}
}

func TestVectorPartitionNativeWireWriteDeadlineV1(t *testing.T) {
	server := NewServer(ServerOptions{ConnectionIdleTimeout: 20 * time.Millisecond})
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	done := make(chan error, 1)
	go func() {
		done <- server.writeSimpleFrame(left, iwire.Header{Type: iwire.FrameResponse}, make([]byte, 1<<20))
	}()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("blocked response write = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked response write ignored its deadline")
	}
}

func BenchmarkVectorPartitionPublicTransportV1(b *testing.B) {
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7}, Query: make([]float32, 128),
		Metric: public.MetricCosineV1, TopK: 10, Probes: 2, EfSearch: 128, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 1 << 30, ResponseBytes: 1 << 20, MergeEntries: 256},
	}
	for i := range request.Query {
		request.Query[i] = float32(i+1) / 128
	}
	options := public.FastSearchOptionsV1{MaxIndexAge: time.Second, MinIndexedThrough: 9}
	commandDeadline := time.Unix(0, 123456789)
	response := public.SearchResponseV1{Generation: request.Generation, Neighbors: []public.NeighborV1{
		{ID: "doc-000001", Score: .99}, {ID: "doc-000002", Score: .98}, {ID: "doc-000003", Score: .97}, {ID: "doc-000004", Score: .96}, {ID: "doc-000005", Score: .95},
		{ID: "doc-000006", Score: .94}, {ID: "doc-000007", Score: .93}, {ID: "doc-000008", Score: .92}, {ID: "doc-000009", Score: .91}, {ID: "doc-000010", Score: .90},
	}}
	evidence := public.FastSearchEvidenceV1{Generation: request.Generation, IndexedThrough: 9, PublishedAt: time.Unix(0, 123), IndexAge: time.Millisecond, TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay"}

	b.Run("native", func(b *testing.B) {
		limits := iwire.DefaultLimits()
		requestBody, responseBody := make([]byte, 0, 1024), make([]byte, 0, 512)
		requestSections, responseSections := make([]iwire.Section, 0, 3), make([]iwire.Section, 0, 2)
		query := make([]float32, 0, len(request.Query))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var err error
			requestBody, err = appendVectorPartitionCommandBodyV1(requestBody[:0], iwire.CommandVectorSearchFast, &request, &options, nil, commandDeadline, limits)
			if err != nil {
				b.Fatal(err)
			}
			requestSections, err = iwire.DecodeSectionsInto(requestSections[:0], requestBody, limits)
			if err != nil {
				b.Fatal(err)
			}
			rawRequest, ok, err := singletonSection(requestSections, iwire.SectionVectorSearchRequest)
			if err != nil || !ok {
				b.Fatal("native request section missing")
			}
			vectorPartitionBenchmarkRequestSink, err = decodeVectorPartitionSearchRequestIntoV1(rawRequest, limits, query[:0])
			if err != nil {
				b.Fatal(err)
			}
			query = vectorPartitionBenchmarkRequestSink.Query[:0]

			responseBody, err = appendVectorPartitionSearchResponseSectionV1(responseBody[:0], response)
			if err == nil {
				responseBody, err = appendVectorPartitionFastEvidenceSectionV1(responseBody, evidence)
			}
			if err != nil {
				b.Fatal(err)
			}
			responseSections, err = iwire.DecodeSectionsInto(responseSections[:0], responseBody, limits)
			if err != nil {
				b.Fatal(err)
			}
			rawResponse, ok, err := singletonSection(responseSections, iwire.SectionVectorSearchResponse)
			if err != nil || !ok {
				b.Fatal("native response section missing")
			}
			vectorPartitionBenchmarkResponseSink, err = decodeVectorPartitionSearchResponseV1(rawResponse, limits)
			if err != nil {
				b.Fatal(err)
			}
			rawEvidence, ok, err := singletonSection(responseSections, iwire.SectionVectorFastEvidence)
			if err != nil || !ok {
				b.Fatal("native evidence section missing")
			}
			vectorPartitionBenchmarkEvidenceSink, err = decodeVectorPartitionFastEvidenceV1(rawEvidence)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(requestBody)+len(responseBody)+2*int(iwire.FrameHeaderLenV1)), "wire-B/op")
	})

	b.Run("legacy_json", func(b *testing.B) {
		jsonRequest := vectorPartitionBenchmarkJSONRequestV1{SchemaVersion: 1, Operation: "search_fast", Search: request, FastOptions: &options}
		jsonResponse := vectorPartitionBenchmarkJSONResponseV1{SchemaVersion: 1, Search: &response, FastEvidence: &evidence}
		var rawRequest, rawResponse []byte
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var err error
			rawRequest, err = json.Marshal(jsonRequest)
			if err != nil {
				b.Fatal(err)
			}
			var gotRequest vectorPartitionBenchmarkJSONRequestV1
			if err := json.Unmarshal(rawRequest, &gotRequest); err != nil {
				b.Fatal(err)
			}
			vectorPartitionBenchmarkRequestSink = gotRequest.Search

			rawResponse, err = json.Marshal(jsonResponse)
			if err != nil {
				b.Fatal(err)
			}
			var gotResponse vectorPartitionBenchmarkJSONResponseV1
			if err := json.Unmarshal(rawResponse, &gotResponse); err != nil {
				b.Fatal(err)
			}
			vectorPartitionBenchmarkResponseSink = *gotResponse.Search
			vectorPartitionBenchmarkEvidenceSink = *gotResponse.FastEvidence
		}
		b.ReportMetric(float64(len(rawRequest)+len(rawResponse)+8), "wire-B/op")
	})
}

type vectorPartitionBenchmarkJSONRequestV1 struct {
	SchemaVersion int                         `json:"schema_version"`
	Operation     string                      `json:"operation"`
	Search        public.SearchRequestV1      `json:"search"`
	FastOptions   *public.FastSearchOptionsV1 `json:"fast_options"`
}

type vectorPartitionBenchmarkJSONResponseV1 struct {
	SchemaVersion int                          `json:"schema_version"`
	Search        *public.SearchResponseV1     `json:"search"`
	FastEvidence  *public.FastSearchEvidenceV1 `json:"fast_evidence"`
}

func vectorPartitionTestSectionV1(t *testing.T, body []byte, id iwire.SectionID, limits iwire.Limits) []byte {
	t.Helper()
	sections, err := iwire.DecodeSections(body, limits)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := singletonSection(sections, id)
	if err != nil || !ok {
		t.Fatalf("section %d = %v, %v", id, ok, err)
	}
	return raw
}
