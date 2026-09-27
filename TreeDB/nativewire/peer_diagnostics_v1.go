package nativewire

import (
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// FixedPeerDiagnosticsV1 retains the existing process-runtime counter schema.
// Unavailable lists unsupported/failed observations instead of treating zero as
// measured. Status is observational; only Readiness performs quorum barriers.
type FixedPeerDiagnosticsV1 struct {
	Status          FixedPeerTCPStatusV1
	Process         VectorPartitionProcessRuntimeStatsV1
	FileDescriptors uint64
	Disk            []PeerDiskSpaceV1
	Network         []PeerNetworkBytesV1
	HostInterfaces  []PeerHostInterfaceV1
	Unavailable     []string
}
type PeerDiskSpaceV1 struct {
	Path                       string
	TotalBytes, AvailableBytes uint64
}
type PeerHostInterfaceV1 struct {
	Name                        string
	ReceiveBytes, TransmitBytes uint64
}

func (r *FixedPeerTCPRuntimeV1) DiagnosticsV1(ctx context.Context) (FixedPeerDiagnosticsV1, error) {
	if r == nil {
		return FixedPeerDiagnosticsV1{}, raftcluster.ErrAdmissionUnavailable
	}
	select {
	case r.diagnostics <- struct{}{}:
		defer func() { <-r.diagnostics }()
	default:
		return FixedPeerDiagnosticsV1{}, raftcluster.ErrAdmissionUnavailable
	}
	return r.diagnosticsV1(ctx)
}
func (r *FixedPeerTCPRuntimeV1) diagnosticsV1(ctx context.Context) (FixedPeerDiagnosticsV1, error) {
	var report FixedPeerDiagnosticsV1
	status, err := r.Status(ctx)
	report.Status = status
	report.Network = r.PeerTransportV1().NetworkStatsV1()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	report.Process = VectorPartitionProcessRuntimeStatsV1{
		SampleUnixNano: uint64(time.Now().UnixNano()), HeapAllocBytes: memory.HeapAlloc, HeapObjects: memory.HeapObjects,
		TotalAllocBytes: memory.TotalAlloc, Mallocs: memory.Mallocs, Frees: memory.Frees, NumGC: uint64(memory.NumGC), PauseTotalNanos: memory.PauseTotalNs,
		Goroutines: uint64(runtime.NumGoroutine()), LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoMemoryLimitBytes: debug.SetMemoryLimit(-1),
	}
	// No directory walks or retained inventory proportional to corpus size.
	peerOSDiagnosticsV1(&report, []string{r.config.DataRoot, r.config.RaftRoot, os.TempDir()})
	return report, err
}
func (c *FixedPeerTCPClientV1) DiagnosticsV1(ctx context.Context, node raftcluster.NodeID) (FixedPeerDiagnosticsV1, error) {
	reply, err := c.call(ctx, node, "diagnostics", fixedPeerRequestV1{}, false)
	if reply.Diagnostics == nil {
		return FixedPeerDiagnosticsV1{}, errors.Join(err, raftcluster.ErrAdmissionUnavailable)
	}
	return *reply.Diagnostics, err
}
