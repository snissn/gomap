package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

var (
	vectorPartitionShardSearchTCPBenchmarkRawSinkV1   []byte
	vectorPartitionShardSearchTCPBenchmarkFrameSinkV1 vectorPartitionShardSearchTCPFrameV1
)

func TestVectorPartitionShardSearchTCPBinaryFrameRoundTripV1(t *testing.T) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1, 3})
	request.DeadlineUnixNano = 123
	request.StrictCapability = &vectorPartitionStrictSearchCapabilityV1{
		Version: 1, ServingIdentityDigest: "serving", CatalogEpoch: 2, CatalogDigest: "catalog",
		ProofNodeID: "node-a", ProofGroupID: "meta", ProofLeaderTerm: 3,
		CatalogAppliedIndex: 4, CatalogCommitIndex: 5, CatalogRaftAppliedIndex: 6,
		ValidThroughUnixNano: 7, TargetGroupID: "group-a", GroupAppliedIndex: 8, MAC: "mac",
	}
	response := VectorPartitionShardSearchResponseV1{
		Version: 1, RequestID: request.RequestID,
		Proof:      VectorPartitionShardSearchProofV1{Kind: "read_index", ServingNode: "node-a", LeaderNode: "node-b", GroupID: "group-a", ReadySetDigest: "ready", ServingIdentityDigest: "identity", ReadTerm: 1, ReadIndex: 2, AppliedTerm: 3, AppliedIndex: 4, CatalogAppliedIndex: 5, GroupAppliedIndex: 6, SourceGeneration: 7, SourceChecksum: 8, SourceSchemaHash: 9, SourceRowCount: 10, PartitionGeneration: 11, RouterGeneration: 12},
		Partials:   []VectorPartitionShardSearchPartialV1{{PartitionID: 3, Neighbors: []VectorPartitionShardSearchNeighborV1{{ID: "doc", Score: 0.5}}, Candidates: 13, Edges: 14, SearchRoute: "hnsw", PackBytes: 15, MappedBytes: 16, HeapBytes: 17, OpenNanos: 18}},
		Partitions: 1, ReadProofs: 2, GenerationPins: 3, PartitionOpens: 4, Candidates: 5, Edges: 6, ResponseBytes: 7,
		Timing: VectorPartitionShardSearchTimingV1{RouteOwnerNanos: 8, ReadIndexApplyNanos: 9, GenerationOpenNanos: 10, SearchNanos: 11, ResponseCopyNanos: 12, TotalNanos: 13},
	}
	stage := func(base uint64) VectorPartitionCatalogMetaLinearizableReadStageStatsV1 {
		return VectorPartitionCatalogMetaLinearizableReadStageStatsV1{
			Reads: base, Successes: base + 1, Failures: base + 2, VerifyLeaderCalls: base + 3,
			LogBarriers: base + 4, NoLogProofs: base + 5, TotalNanos: base + 6,
			AdmissionNanos: base + 7, VerifyLeaderNanos: base + 8, BarrierNanos: base + 9,
			CurrentTermNanos: base + 10, RaftApplyNanos: base + 11, AppliedReadNanos: base + 12,
		}
	}
	identity := VectorPartitionShardEndpointIdentityV1{
		Version: 1, GroupID: "group-a", InstanceIdentity: "instance",
		CatalogMetaReadStats: VectorPartitionCatalogMetaLinearizableReadStatsV1{
			Total: stage(1), OperationsHealth: stage(20), StrictSearch: stage(40), ServingRefresh: stage(60),
			CoordinatorLifecycle: stage(80), ShardLifecycle: stage(100), Unknown: stage(120),
			LastTerm: 140, LastCatalogApplied: 141, LastRaftApplied: 142, LastRaftLog: 143,
		},
		ProcessRuntimeStats: VectorPartitionProcessRuntimeStatsV1{
			SampleUnixNano: 150, CPUTimeNanos: 151, RunQueueDelayNanos: 152, Timeslices: 153,
			VoluntaryContextSwitches: 154, NonvoluntaryContextSwitches: 155,
			RSSBytes: 156, PeakRSSBytes: 157, HeapAllocBytes: 158, HeapObjects: 159,
			TotalAllocBytes: 160, Mallocs: 161, Frees: 162, NumGC: 163, PauseTotalNanos: 164,
			Goroutines: 165, LogicalCPUs: 4, GOMAXPROCS: 3, GoMemoryLimitBytes: 166, EffectiveCPUSet: "0-3",
		},
	}
	frames := []vectorPartitionShardSearchTCPFrameV1{
		{Request: &request}, {Response: &response}, {Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Message: "moved"}}, {Probe: &vectorPartitionShardEndpointProbeV1{Version: 1}}, {ProbeResponse: &identity},
	}
	wantBytes := []int{470, 368, 37, 10, 952}
	for i, frame := range frames {
		raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(raw)
		if err != nil || !reflect.DeepEqual(decoded, frame) {
			t.Fatalf("decoded=%+v want=%+v err=%v", decoded, frame, err)
		}
		if frame.Request != nil {
			requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(*frame.Request)
			if err != nil || requestBytes != uint64(len(raw)) {
				t.Fatalf("request bytes=%d wire=%d err=%v", requestBytes, len(raw), err)
			}
		}
		if len(raw) != wantBytes[i] {
			t.Fatalf("frame[%d] bytes=%d want=%d", i, len(raw), wantBytes[i])
		}
	}
	probe, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frames[3])
	if err != nil {
		t.Fatal(err)
	}
	wantProbe := []byte{1, 4, 0, 0, 0, 0, 1, 0, 0, 0}
	if !bytes.Equal(probe, wantProbe) {
		t.Fatalf("probe=%x want=%x", probe, wantProbe)
	}
}

func TestVectorPartitionShardSearchTCPBinaryFrameRejectsMalformedV1(t *testing.T) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
	if _, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{}); err == nil {
		t.Fatal("encoded empty frame")
	}
	invalid := request
	invalid.Metric = "invalid"
	if _, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Request: &invalid}); err == nil {
		t.Fatal("encoded invalid request enum")
	}
	invalid = request
	invalid.Query = []float32{float32(math.NaN())}
	if _, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Request: &invalid}); err == nil {
		t.Fatal("encoded nonfinite query")
	}
	invalidError := vectorPartitionShardSearchTCPErrorV1{}
	if _, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Error: &invalidError}); err == nil {
		t.Fatal("encoded invalid error code")
	}
	raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Request: &request})
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{nil, raw[:len(raw)-1], append(append([]byte(nil), raw...), 0), append([]byte{2}, raw[1:]...), append([]byte{1, 99, 0, 0, 0, 0}, raw[6:]...)} {
		if _, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(malformed); err == nil {
			t.Fatalf("accepted malformed body %x", malformed)
		}
	}
	badRequestVersion := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(badRequestVersion[6:10], 2)
	if _, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(badRequestVersion); err == nil {
		t.Fatal("accepted unsupported request version")
	}
	if _, err := decodeVectorPartitionShardSearchTCPFrameBodyV1([]byte{1, 4, 0, 0, 0, 0, 2, 0, 0, 0}); err == nil {
		t.Fatal("accepted unsupported probe version")
	}
	response := VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: "request"}
	responseRaw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Response: &response})
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(responseRaw[6:10], 2)
	if _, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(responseRaw); err == nil {
		t.Fatal("accepted unsupported response version")
	}
	w := vectorPartitionShardSearchTCPBodyWriterV1{}
	w.u8(vectorPartitionShardSearchTCPFrameVersionV1)
	w.u8(vectorPartitionShardSearchTCPFrameResponseV1)
	w.u32(0)
	w.u32(response.Version)
	w.string(response.RequestID)
	appendVectorPartitionShardSearchTCPProofV1(&w, response.Proof)
	w.u32(math.MaxUint32)
	if _, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(w.body); err == nil {
		t.Fatal("accepted response count that exceeds the remaining body")
	}
}

func BenchmarkVectorPartitionShardSearchTCPBinaryRequestCodecV1(b *testing.B) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1, 3})
	benchmarkVectorPartitionShardSearchTCPBinaryCodecV1(b, vectorPartitionShardSearchTCPFrameV1{Request: &request})
}

func TestVectorPartitionShardSearchTCPBinaryBenchmarkWireSizesV1(t *testing.T) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1, 3})
	partials := make([]VectorPartitionShardSearchPartialV1, 2)
	for i := range partials {
		partials[i] = VectorPartitionShardSearchPartialV1{PartitionID: uint32(i), SearchRoute: "hnsw"}
		for neighbor := range 10 {
			partials[i].Neighbors = append(partials[i].Neighbors, VectorPartitionShardSearchNeighborV1{ID: fmt.Sprintf("doc-%06d", i*10+neighbor), Score: float32(neighbor) / 10})
		}
	}
	response := VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: "benchmark", Partials: partials}
	want := []int{352, 739}
	for i, frame := range []vectorPartitionShardSearchTCPFrameV1{{Request: &request}, {Response: &response}} {
		raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != want[i] {
			t.Fatalf("frame[%d] bytes=%d want=%d", i, len(raw), want[i])
		}
	}
}

func BenchmarkVectorPartitionShardSearchTCPBinaryResponseCodecV1(b *testing.B) {
	partials := make([]VectorPartitionShardSearchPartialV1, 2)
	for i := range partials {
		partials[i] = VectorPartitionShardSearchPartialV1{PartitionID: uint32(i), SearchRoute: "hnsw"}
		for neighbor := range 10 {
			partials[i].Neighbors = append(partials[i].Neighbors, VectorPartitionShardSearchNeighborV1{ID: fmt.Sprintf("doc-%06d", i*10+neighbor), Score: float32(neighbor) / 10})
		}
	}
	response := VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: "benchmark", Partials: partials}
	benchmarkVectorPartitionShardSearchTCPBinaryCodecV1(b, vectorPartitionShardSearchTCPFrameV1{Response: &response})
}

func BenchmarkVectorPartitionShardSearchTCPBinaryControlCodecV1(b *testing.B) {
	errorFrame := vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Message: "moved"}
	identity := VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: "group-a", InstanceIdentity: "node-a"}
	for _, test := range []struct {
		name  string
		frame vectorPartitionShardSearchTCPFrameV1
	}{
		{name: "error", frame: vectorPartitionShardSearchTCPFrameV1{Error: &errorFrame}},
		{name: "probe", frame: vectorPartitionShardSearchTCPFrameV1{Probe: &vectorPartitionShardEndpointProbeV1{Version: 1}}},
		{name: "probe_response", frame: vectorPartitionShardSearchTCPFrameV1{ProbeResponse: &identity}},
	} {
		b.Run(test.name, func(b *testing.B) {
			benchmarkVectorPartitionShardSearchTCPBinaryCodecV1(b, test.frame)
		})
	}
}

func benchmarkVectorPartitionShardSearchTCPBinaryCodecV1(b *testing.B, frame vectorPartitionShardSearchTCPFrameV1) {
	raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		var encoded []byte
		b.ReportAllocs()
		b.ReportMetric(float64(len(raw)), "wire-B/op")
		for b.Loop() {
			encoded, err = appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
			if err != nil {
				b.Fatal(err)
			}
		}
		vectorPartitionShardSearchTCPBenchmarkRawSinkV1 = encoded
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(raw)), "wire-B/op")
		for b.Loop() {
			vectorPartitionShardSearchTCPBenchmarkFrameSinkV1, err = decodeVectorPartitionShardSearchTCPFrameBodyV1(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestVectorPartitionShardSearchTCPDispatcherReusesConnectionV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepts atomic.Uint64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go (VectorPartitionShardSearchTCPServerV1{Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
				return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
			})}).ServeConn(context.Background(), conn)
		}
	}()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	for _, id := range []string{"first", "second"} {
		if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if got := accepts.Load(); got != 1 {
		t.Fatalf("connections=%d want one reused connection", got)
	}
}

func TestVectorPartitionShardSearchTCPDerivesSeparateFrameBoundsV1(t *testing.T) {
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	dispatcher, err := newVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"}, nil, 1, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if got, want := dispatcher.maxRequestFrame, uint32(limits.MaxRequestBytes); got != want {
		t.Fatalf("request frame=%d want=%d", got, want)
	}
	if got, want := dispatcher.maxResponseFrame, uint32(limits.MaxResponseBytes); got != want {
		t.Fatalf("response frame=%d want=%d", got, want)
	}
	limits.MaxResponseBytes = 128 << 20
	dispatcher, err = newVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"}, nil, 1, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if got, want := dispatcher.maxResponseFrame, uint32(limits.MaxResponseBytes); got != want {
		t.Fatalf("custom response frame=%d want=%d", got, want)
	}
	limits.MaxResponseBytes = uint64(^uint32(0)) + 1
	if _, err := newVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"}, nil, 1, limits); err == nil {
		t.Fatal("unencodable response limit succeeded")
	}
}

func TestVectorPartitionShardSearchTCPDispatcherAcceptsNilContextV1(t *testing.T) {
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	_, err = dispatcher.DispatchVectorPartitionShardSearchV1(nil, VectorPartitionShardSearchRequestV1{TargetGroupID: "missing"})
	var searchErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &searchErr) || searchErr.Code != VectorPartitionShardSearchErrorUnknownOwnerV1 {
		t.Fatalf("error=%v want unknown owner", err)
	}
}

func TestVectorPartitionShardEndpointProbeBindsLiveInstanceV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	reads := uint64(1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			(VectorPartitionShardSearchTCPServerV1{
				EndpointIdentity: VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: "group-a", InstanceIdentity: "config-sha"},
				EndpointIdentityProvider: func() VectorPartitionShardEndpointIdentityV1 {
					return VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: "group-a", InstanceIdentity: "config-sha", CatalogMetaReadStats: VectorPartitionCatalogMetaLinearizableReadStatsV1{Total: VectorPartitionCatalogMetaLinearizableReadStageStatsV1{Reads: reads}}}
				},
			}).ServeConn(context.Background(), conn)
		}
	}()
	identity, err := ProbeVectorPartitionShardEndpointV1(t.Context(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if identity.GroupID != "group-a" || identity.InstanceIdentity != "config-sha" || identity.CatalogMetaReadStats.Total.Reads != 1 {
		t.Fatalf("endpoint identity = %+v", identity)
	}
}

func TestVectorPartitionM8ProductionTopologyOwnsDispatcherV1(t *testing.T) {
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	topology := &VectorPartitionM8ProductionMultiGroupV1{dispatcher: dispatcher}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	dispatcher.mu.Lock()
	closed := dispatcher.closed
	dispatcher.mu.Unlock()
	if !closed {
		t.Fatal("M8 topology left dispatcher open")
	}
}

func TestVectorPartitionShardSearchTCPServerBoundsRequestAndFailedResponseFramesV1(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		called := atomic.Bool{}
		go (VectorPartitionShardSearchTCPServerV1{MaxFrame: 64, MaxResponseFrame: 4096, Service: vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			called.Store(true)
			return VectorPartitionShardSearchResponseV1{}, nil
		})}).ServeConn(context.Background(), server)
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], 65)
		if _, err := client.Write(prefix[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("oversized request left connection open: %v", err)
		}
		if called.Load() {
			t.Fatal("oversized request reached shard service")
		}
	})
	t.Run("response", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		go (VectorPartitionShardSearchTCPServerV1{MaxFrame: 4096, MaxResponseFrame: 64, Service: vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			return VectorPartitionShardSearchResponseV1{RequestID: strings.Repeat("x", 4096)}, nil
		})}).ServeConn(context.Background(), server)
		if err := writeVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPFrameV1{Request: &VectorPartitionShardSearchRequestV1{}}, 4096); err != nil {
			t.Fatal(err)
		}
		if _, err := readVectorPartitionShardSearchTCPFrameV1(client, 4096); !errors.Is(err, io.EOF) {
			t.Fatalf("failed response write left connection open: %v", err)
		}
	})
	t.Run("default response", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		go (VectorPartitionShardSearchTCPServerV1{MaxFrame: 4096, Service: vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			return VectorPartitionShardSearchResponseV1{RequestID: strings.Repeat("x", 8192)}, nil
		})}).ServeConn(context.Background(), server)
		if err := writeVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPFrameV1{Request: &VectorPartitionShardSearchRequestV1{}}, 4096); err != nil {
			t.Fatal(err)
		}
		frame, err := readVectorPartitionShardSearchTCPFrameV1(client, 16384)
		if err != nil || frame.Response == nil || len(frame.Response.RequestID) != 8192 {
			t.Fatalf("default response frame=%+v err=%v", frame, err)
		}
	})
}

func TestVectorPartitionShardSearchTCPCancelWatcherStopsBeforeReuseV1(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	deadlineStarted := make(chan struct{})
	releaseDeadline := make(chan struct{})
	conn := &vectorPartitionShardSearchTCPBlockingDeadlineConnV1{Conn: left, started: deadlineStarted, release: releaseDeadline}
	ctx, cancel := context.WithCancel(context.Background())
	stop := vectorPartitionShardSearchTCPInterruptOnCancelV1(ctx, conn)
	cancel()
	<-deadlineStarted
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("cancel watcher stopped before its deadline write completed")
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseDeadline)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cancel watcher did not stop")
	}
	stop()
}

type vectorPartitionShardSearchTCPBlockingDeadlineConnV1 struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (c *vectorPartitionShardSearchTCPBlockingDeadlineConnV1) SetDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		close(c.started)
		<-c.release
	}
	return c.Conn.SetDeadline(deadline)
}

func TestVectorPartitionShardSearchTCPDispatcherReconnectsAfterIdleCloseAndFailsAfterCloseV1(t *testing.T) {
	const poolSize = 2
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	idleClosed := make(chan struct{}, poolSize)
	started := make(chan struct{}, poolSize)
	release := make(chan struct{})
	var accepts atomic.Uint64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted := accepts.Add(1)
			go func() {
				timeout := time.Second
				if accepted <= poolSize {
					timeout = 10 * time.Millisecond
				}
				(VectorPartitionShardSearchTCPServerV1{InitialTimeout: timeout, Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
					if r.RequestID != "reconnected" {
						started <- struct{}{}
						<-release
					}
					return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
				})}).ServeConn(context.Background(), conn)
				select {
				case idleClosed <- struct{}{}:
				default:
				}
			}()
		}
	}()
	dispatcher, err := newVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()}, nil, poolSize, VectorPartitionShardSearchLimitsV1{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, poolSize)
	for i := range poolSize {
		go func() {
			_, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: fmt.Sprintf("warm-%d", i)})
			done <- err
		}()
	}
	for range poolSize {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("pooled connection did not start")
		}
	}
	close(release)
	for range poolSize {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for range poolSize {
		select {
		case <-idleClosed:
		case <-time.After(time.Second):
			t.Fatal("server did not close pooled idle connection")
		}
	}
	if response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: "reconnected"}); err != nil || response.RequestID != "reconnected" {
		t.Fatalf("reconnect response=%+v err=%v", response, err)
	}
	if got := accepts.Load(); got != poolSize+1 {
		t.Fatalf("connections=%d want %d", got, poolSize+1)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"}); err == nil {
		t.Fatal("closed dispatcher accepted request")
	}
}

func TestVectorPartitionShardSearchTCPServerClosesAfterPeerInputDuringRequestV1(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		(VectorPartitionShardSearchTCPServerV1{Service: vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			close(started)
			<-ctx.Done()
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		})}).ServeConn(context.Background(), serverConn)
		close(done)
	}()
	if err := writeVectorPartitionShardSearchTCPFrameV1(clientConn, vectorPartitionShardSearchTCPFrameV1{Request: &VectorPartitionShardSearchRequestV1{}}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := clientConn.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := readVectorPartitionShardSearchTCPFrameV1(clientConn, vectorPartitionShardSearchTCPMaxFrameBytesV1); !errors.Is(err, io.EOF) {
		t.Fatalf("peer input left connection reusable: %v", err)
	}
	<-done
}

func TestVectorPartitionShardSearchTCPDispatcherCancellationWhilePoolIsFullV1(t *testing.T) {
	const poolSize = 2
	started := make(chan struct{}, poolSize)
	release := make(chan struct{})
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		started <- struct{}{}
		<-release
		return VectorPartitionShardSearchResponseV1{Version: 1}, nil
	}))
	dispatcher, err := newVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()}, nil, poolSize, VectorPartitionShardSearchLimitsV1{})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	done := make(chan error, poolSize)
	for range poolSize {
		go func() {
			_, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
			done <- err
		}()
	}
	for range poolSize {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("concurrent request did not acquire a distinct pooled connection")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
		secondDone <- err
	}()
	cancel()
	select {
	case err := <-secondDone:
		var shardErr *VectorPartitionShardSearchErrorV1
		if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorCanceledV1 || !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting cancellation error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request remained blocked behind the pooled connection")
	}
	close(release)
	for range poolSize {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestVectorPartitionShardSearchTCPDispatcherDoesNotRetryServiceUnavailableV1(t *testing.T) {
	var calls atomic.Uint64
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		calls.Add(1)
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: "group-a", Err: errors.New("lifecycle unavailable")}
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"}); err == nil {
		t.Fatal("service failure succeeded")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("service calls=%d want one", got)
	}
}

func TestVectorPartitionShardSearchTCPDispatcherUsesLeaderNodeEndpointV1(t *testing.T) {
	leader := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
	}))
	stale := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Err: errors.New("moved")}
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1(map[raftcluster.GroupID]string{"group-a": stale.Addr().String()}, map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-a": stale.Addr().String(), "node-b": leader.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", TargetNodeID: "node-a"}); err == nil {
		t.Fatal("stale leader endpoint succeeded")
	}
	if response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", TargetNodeID: "node-b", RequestID: "leader"}); err != nil || response.RequestID != "leader" {
		t.Fatalf("leader endpoint response=%+v err=%v", response, err)
	}
}

type vectorPartitionShardSearchHandlerFuncV1 func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)

func (f vectorPartitionShardSearchHandlerFuncV1) Search(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	return f(ctx, request)
}

func TestVectorPartitionShardSearchTCPServerInitialReadTimeoutV1(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		(VectorPartitionShardSearchTCPServerV1{InitialTimeout: 20 * time.Millisecond}).ServeConn(context.Background(), server)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slowloris initial frame kept M5 TCP server blocked")
	}
}

func TestVectorPartitionShardSearchTCPServerInterruptsPeerReadBeforeSuccessfulResponseV1(t *testing.T) {
	client, rawServer := net.Pipe()
	defer client.Close()
	server := &vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1{Conn: rawServer}
	serverDone := make(chan struct{})
	go func() {
		(VectorPartitionShardSearchTCPServerV1{
			InitialTimeout: time.Second,
			Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
				return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
			}),
		}).ServeConn(context.Background(), server)
		close(serverDone)
	}()
	if err := writeVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPFrameV1{Request: &VectorPartitionShardSearchRequestV1{RequestID: "immediate"}}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
		t.Fatal(err)
	}
	frame, err := readVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	if err != nil || frame.Response == nil || frame.Response.RequestID != "immediate" {
		t.Fatalf("response frame=%+v err=%v", frame, err)
	}
	// The production transport keeps a healthy connection for the next request.
	// Closing the peer must still release the server goroutine deterministically.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not return after peer close")
	}
	if !server.interruptedReadBeforeWrite() {
		t.Fatal("successful response was written without interrupting the peer-monitor read")
	}
}

// vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1 turns the
// successful-response assertion into a transport ordering check rather than a
// scheduler-sensitive wall-clock test. The monitor's stop path must set an
// already-expired read deadline before ServeConn writes its response.
type vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1 struct {
	net.Conn
	mu                        sync.Mutex
	interruptedReadDeadline   bool
	wroteAfterInterruptedRead bool
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		c.interruptedReadDeadline = true
	}
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) Write(raw []byte) (int, error) {
	c.mu.Lock()
	if c.interruptedReadDeadline {
		c.wroteAfterInterruptedRead = true
	}
	c.mu.Unlock()
	return c.Conn.Write(raw)
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) interruptedReadBeforeWrite() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wroteAfterInterruptedRead
}

func TestVectorPartitionShardSearchTCPDispatcherRoundTripV1(t *testing.T) {
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		if request.RequestID != "m8-round-trip" || request.TargetGroupID != "group-a" || request.TargetNodeID != "node-a" || len(request.PartitionIDs) != 1 || request.PartitionIDs[0] != 3 {
			t.Fatalf("unexpected serialized request: %+v", request)
		}
		return VectorPartitionShardSearchResponseV1{
			Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID,
			Proof:      VectorPartitionShardSearchProofV1{ServingNode: "node-a", LeaderNode: "node-a", GroupID: "group-a", ReadySetDigest: request.ReadySetDigest, ReadTerm: 2, ReadIndex: 9, AppliedTerm: 2, AppliedIndex: 9, SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum, SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount, PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration},
			Partitions: 1,
		}, nil
	}))
	defer listener.Close()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{
		Version: VectorPartitionShardSearchVersionV1, RequestID: "m8-round-trip", CancellationID: "cancel", Database: "default", Catalog: "default", Collection: "users", IndexName: "embedding", IndexDefinitionDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", ReadySetDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, RouterGeneration: 6, TargetGroupID: "group-a", TargetNodeID: "node-a", PartitionIDs: []uint32{3}, Query: []float32{1, 0}, Metric: VectorPartitionShardSearchMetricCosineV1, Mode: VectorPartitionShardSearchModeNoDocumentV1, Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 1, EfSearch: 1, RequestBytesLimit: 1024, CandidateBytesLimit: 1024, ResponseBytesLimit: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "m8-round-trip" || response.Proof.GroupID != "group-a" || response.Proof.AppliedIndex != 9 {
		t.Fatalf("response=%+v", response)
	}
}

func TestVectorPartitionShardSearchTCPDispatcherPreservesTypedServiceErrorV1(t *testing.T) {
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Err: errors.New("leader moved")}
	}))
	defer listener.Close()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
	if response.Version != 0 || response.RequestID != "" || len(response.Partials) != 0 || response.Partitions != 0 {
		t.Fatalf("error returned partial response: %+v", response)
	}
	var shardErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorNotLeaderV1 || shardErr.LeaderHint != "node-b" {
		t.Fatalf("error=%v does not preserve M5 class and redirect", err)
	}
}

func TestVectorPartitionShardSearchTCPDispatcherRejectsAmbiguousResponseFramesV1(t *testing.T) {
	for name, frame := range map[string]vectorPartitionShardSearchTCPFrameV1{
		"response_and_error":   {Response: &VectorPartitionShardSearchResponseV1{}, Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Message: "also error"}},
		"request_and_response": {Request: &VectorPartitionShardSearchRequestV1{RequestID: "unexpected"}, Response: &VectorPartitionShardSearchResponseV1{}},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			go func() {
				defer server.Close()
				_, _ = readVectorPartitionShardSearchTCPFrameV1(server, vectorPartitionShardSearchTCPMaxFrameBytesV1)
				_ = writeVectorPartitionShardSearchTCPFrameV1(server, frame, vectorPartitionShardSearchTCPMaxFrameBytesV1)
			}()
			dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": "pipe"})
			if err != nil {
				t.Fatal(err)
			}
			dispatcher.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"}); err == nil {
				t.Fatal("accepted ambiguous M5 response frame")
			}
		})
	}
}

func TestVectorPartitionShardSearchTCPDispatcherCancellationWithoutDeadlineV1(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan error, 1)
	defer close(release)
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		close(started)
		select {
		case <-ctx.Done():
			observed <- ctx.Err()
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		case <-release:
			return VectorPartitionShardSearchResponseV1{}, nil
		}
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		response VectorPartitionShardSearchResponseV1
		err      error
	}, 1)
	go func() {
		response, dispatchErr := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
		result <- struct {
			response VectorPartitionShardSearchResponseV1
			err      error
		}{response, dispatchErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server handler did not start")
	}
	cancel()
	select {
	case got := <-result:
		if got.response.Version != 0 || got.response.RequestID != "" || len(got.response.Partials) != 0 || got.response.Partitions != 0 {
			t.Fatalf("cancellation returned partial response: %+v", got.response)
		}
		var shardErr *VectorPartitionShardSearchErrorV1
		if !errors.As(got.err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorCanceledV1 || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("error=%v, want typed cancellation", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt TCP read")
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server handler context error=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe client disconnect cancellation")
	}
}

func TestVectorPartitionShardSearchTCPServerUsesRequestDeadlineV1(t *testing.T) {
	observed := make(chan error, 1)
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		<-ctx.Done()
		observed <- ctx.Err()
		return VectorPartitionShardSearchResponseV1{}, ctx.Err()
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Millisecond)
	_, err = dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", DeadlineUnixNano: deadline.UnixNano()})
	var shardErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorDeadlineV1 {
		t.Fatalf("error=%v, want typed deadline", err)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler context error=%v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not observe request deadline")
	}
}

func newVectorPartitionShardSearchTCPListenerV1(t *testing.T, handler VectorPartitionShardSearchHandlerV1) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go (VectorPartitionShardSearchTCPServerV1{Service: handler}).ServeConn(context.Background(), conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestVectorPartitionShardSearchTCPFrameRejectsOversizeV1(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	done := make(chan error, 1)
	go func() {
		var prefix [4]byte
		prefix[3] = 5
		_, err := right.Write(prefix[:])
		done <- err
	}()
	_, err := readVectorPartitionShardSearchTCPFrameV1(left, 4)
	if err == nil {
		t.Fatal("oversized frame was accepted")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer did not finish")
	}
}
