package nativewire

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// This entire test-only harness also compiles against the pre-sparse runtime.
// The paired workflow copies it unchanged into the exact base checkout.
type sparseCatalogProcessMetricsV1 struct {
	Sequence                                       uint64
	TotalAlloc, Mallocs, HeapAlloc, HeapInuse, Sys uint64
	NumGC                                          uint32
	PauseTotalNs                                   uint64
	Goroutines, FDs, Threads                       int
	RSSBytes, PeakRSSBytes                         uint64
}

func sparseCatalogReadProcessMetricsV1(sequence uint64) sparseCatalogProcessMetricsV1 {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	metrics := sparseCatalogProcessMetricsV1{
		Sequence: sequence, TotalAlloc: memory.TotalAlloc, Mallocs: memory.Mallocs,
		HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, Sys: memory.Sys,
		NumGC: memory.NumGC, PauseTotalNs: memory.PauseTotalNs, Goroutines: runtime.NumGoroutine(),
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		metrics.FDs = len(entries)
	}
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "VmRSS:":
				metrics.RSSBytes = value * 1024
			case "VmHWM:":
				metrics.PeakRSSBytes = value * 1024
			case "Threads:":
				metrics.Threads = int(value)
			}
		}
	}
	return metrics
}

func TestSparseCatalogBenchmarkProcessV1(t *testing.T) {
	raw := os.Getenv("GOMAP_SPARSE_CATALOG_BENCH_CONFIG")
	if raw == "" {
		return
	}
	var config FixedPeerTCPConfigV1
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		sequence, err := strconv.ParseUint(scanner.Text(), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		metrics, err := json.Marshal(sparseCatalogReadProcessMetricsV1(sequence))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("SPARSE_CATALOG_METRICS %s\n", metrics)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
}

func sparseCatalogBenchmarkProcessV1(t testing.TB, config FixedPeerTCPConfigV1) *fixedPeerTestProcessV1 {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.CreateTemp(t.TempDir(), "metrics-log-")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestSparseCatalogBenchmarkProcessV1$", "-test.v")
	command.Env = append(os.Environ(), "GOMAP_SPARSE_CATALOG_BENCH_CONFIG="+string(raw))
	command.Stdout, command.Stderr = log, log
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &fixedPeerTestProcessV1{command: command, input: input, log: log}
	t.Cleanup(func() { process.stop(t) })
	return process
}

func sparseCatalogBenchmarkMetricsV1(t testing.TB, process *fixedPeerTestProcessV1, sequence uint64) sparseCatalogProcessMetricsV1 {
	t.Helper()
	writer, ok := process.input.(interface{ Write([]byte) (int, error) })
	if !ok {
		t.Fatal("child metrics input is not writable")
	}
	if _, err := fmt.Fprintf(writer, "%d\n", sequence); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(process.log.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			const prefix = "SPARSE_CATALOG_METRICS "
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			var metrics sparseCatalogProcessMetricsV1
			if json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &metrics) == nil && metrics.Sequence == sequence {
				return metrics
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("child metrics deadline")
	return sparseCatalogProcessMetricsV1{}
}

func sparseCatalogBenchmarkConfigsV1(t testing.TB, sparse bool, inventory int) []FixedPeerTCPConfigV1 {
	t.Helper()
	configs := fixedPeerTestConfigsV1(t)
	used := make(map[string]bool)
	for _, config := range configs {
		used[config.ListenAddress] = true
		for _, address := range config.RaftListen {
			used[address] = true
		}
	}
	address := func() string {
		for attempt := 0; attempt < 32; attempt++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			result := listener.Addr().String()
			listener.Close()
			if !used[result] {
				used[result] = true
				return result
			}
		}
		t.Fatal("could not allocate distinct benchmark endpoint")
		return ""
	}
	extra := configs[0]
	extra.NodeID, extra.ListenAddress = "bench-ingress", address()
	extra.DataRoot, extra.RaftRoot = t.TempDir(), t.TempDir()
	extra.RaftListen = map[raftcluster.GroupID]string{}
	catalog := configs[0].Catalog
	catalog.Peers = append([]raftcluster.Peer(nil), catalog.Peers...)
	if !sparse {
		raftAddress := address()
		catalog.Peers = append(catalog.Peers, raftcluster.Peer{ID: extra.NodeID, Address: raftAddress, Capabilities: catalog.Features})
		extra.RaftListen[catalog.ID] = raftAddress
	}
	nodes := append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...)
	nodes = append(nodes, FixedPeerTCPNodeV1{ID: extra.NodeID, Address: extra.ListenAddress})
	for i := len(nodes); i < inventory; i++ {
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: raftcluster.NodeID(fmt.Sprintf("inventory-%04d", i)), Address: fmt.Sprintf("127.5.%d.%d:19000", i/256, i%256)})
	}
	configs = append(configs, extra)
	for i := range configs {
		configs[i].Nodes, configs[i].Catalog = nodes, catalog
	}
	return configs
}

func BenchmarkSparseCatalogRemoteOwnerCreateV1(b *testing.B) {
	benchmarkSparseCatalogRemoteOwnerCreateV1(b, nil)
}

func benchmarkSparseCatalogRemoteOwnerCreateV1(b *testing.B, configure func(testing.TB, []FixedPeerTCPConfigV1) int) {
	for _, variant := range []struct {
		name      string
		sparse    bool
		inventory int
	}{{"AllVoters", false, 4}, {"Sparse", true, 4}, {"Inventory40", true, 40}} {
		b.Run(variant.name, func(b *testing.B) {
			b.StopTimer()
			if b.N > 1000 {
				b.Skip("bounded conformance benchmark: use -benchtime=10x")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			configs := sparseCatalogBenchmarkConfigsV1(b, variant.sparse, variant.inventory)
			publisherIndex := 3
			if configure != nil {
				publisherIndex = configure(b, configs)
			}
			processes := make([]*fixedPeerTestProcessV1, len(configs))
			for i, config := range configs {
				processes[i] = sparseCatalogBenchmarkProcessV1(b, config)
			}
			client, err := NewFixedPeerTCPClientV1(configs[3])
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()
			publisher := client
			if publisherIndex != 3 {
				publisher, err = NewFixedPeerTCPClientV1(configs[publisherIndex])
				if err != nil {
					b.Fatal(err)
				}
				defer publisher.Close()
			}
			var leader raftcluster.NodeID
			fixedPeerWaitV1(b, ctx, func() bool {
				for _, peer := range configs[0].Catalog.Peers {
					status, err := client.Status(ctx, peer.ID)
					if err == nil && status.CatalogRaft.State == "Leader" {
						leader = peer.ID
						return true
					}
				}
				return false
			})
			catalog := raftplacement.CatalogV1{}
			for _, group := range configs[0].Groups {
				item := raftplacement.GroupV1{ID: group.ID}
				for _, peer := range group.Peers {
					item.Members = append(item.Members, peer.ID)
				}
				catalog.Groups = append(catalog.Groups, item)
			}
			names := make([]string, b.N)
			for i := range names {
				names[i] = fmt.Sprintf("bench-users-%d", i)
				catalog.Placements = append(catalog.Placements, raftplacement.CollectionPlacementV1{
					Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: names[i]}, GroupID: "group-b",
				})
			}
			record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
			if err != nil {
				b.Fatal(err)
			}
			command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := publisher.PublishCatalog(ctx, leader, command); err != nil {
				b.Fatal(err)
			}
			fixedPeerWaitV1(b, ctx, func() bool {
				for i, config := range configs {
					status, err := client.Status(ctx, config.NodeID)
					if err != nil || ((!variant.sparse || i < 3) && status.Catalog.Epoch != 1) {
						return false
					}
					for _, group := range status.Groups {
						if group.LeaderID == "" {
							return false
						}
					}
				}
				return true
			})
			metadata := make([]ClusterRequestMetadata, b.N)
			routeStart := time.Now()
			for i, name := range names {
				request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: name, Shape: ClusterRouteShapeCollection}
				route, err := client.Route(ctx, "bench-ingress", request)
				if err != nil {
					b.Fatal(err)
				}
				metadata[i].AckPolicy = iwire.AckRaftCommitted
				ApplyClusterRouteMetadata(&metadata[i], request, route)
			}
			routeElapsed := time.Since(routeStart)
			owner, err := client.Status(ctx, "owner-1")
			if err != nil {
				b.Fatal(err)
			}
			version := owner.Groups[0].CatalogVersion
			before := make([]sparseCatalogProcessMetricsV1, len(processes))
			for i, process := range processes {
				before[i] = sparseCatalogBenchmarkMetricsV1(b, process, 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i, name := range names {
				result, err := client.Submit(ctx, "bench-ingress", fixedPeerCreateEntryV1(b, name, version), metadata[i])
				if err != nil || !result.CommittedApplied || !result.CommittedRecoverable || !result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != "group-b" {
					b.Fatalf("lost production remote-owner durability: %+v, %v", result, err)
				}
				version = result.CatalogVersion
			}
			b.StopTimer()
			var totalAlloc, allocations, rss, peakRSS, heap, gcPause uint64
			var goroutines, fds, threads int
			for i, process := range processes {
				after := sparseCatalogBenchmarkMetricsV1(b, process, 2)
				totalAlloc += after.TotalAlloc - before[i].TotalAlloc
				allocations += after.Mallocs - before[i].Mallocs
				rss += after.RSSBytes
				peakRSS += after.PeakRSSBytes
				heap += after.HeapAlloc
				gcPause += after.PauseTotalNs - before[i].PauseTotalNs
				goroutines += after.Goroutines
				fds += after.FDs
				threads += after.Threads
				b.Logf("node=%s before=%+v after=%+v", configs[i].NodeID, before[i], after)
			}
			b.ReportMetric(float64(totalAlloc)/float64(b.N), "nodes_B/op")
			b.ReportMetric(float64(routeElapsed.Nanoseconds())/float64(b.N), "route_ns/op")
			b.ReportMetric(float64(allocations)/float64(b.N), "nodes_allocs/op")
			b.ReportMetric(float64(rss), "nodes_RSS_bytes")
			b.ReportMetric(float64(peakRSS), "sum_node_peak_RSS_bytes")
			b.ReportMetric(float64(heap), "nodes_heap_bytes")
			b.ReportMetric(float64(gcPause)/float64(b.N), "nodes_gc_pause_ns/op")
			b.ReportMetric(float64(goroutines), "node_goroutines")
			b.ReportMetric(float64(fds), "node_fds")
			b.ReportMetric(float64(threads), "node_threads")
			for _, process := range processes {
				process.stop(b)
			}
			var persistent int64
			for _, config := range configs {
				for _, root := range []string{config.DataRoot, config.RaftRoot} {
					entries, err := os.ReadDir(root)
					if err != nil && !os.IsNotExist(err) {
						b.Fatal(err)
					}
					if len(entries) == 0 {
						continue
					}
					// Reuse the existing recursive accounting for each present
					// root without requiring a storage-free ingress data root.
					one := config
					one.DataRoot, one.RaftRoot = root, b.TempDir()
					persistent += fixedPeerPersistentBytesV1(b, []FixedPeerTCPConfigV1{one})
				}
			}
			b.ReportMetric(float64(persistent), "persistent_bytes")
		})
	}
}

func BenchmarkSparseCatalogConfigV1(b *testing.B) {
	for _, nodes := range []int{4, 40, 1024} {
		for _, groups := range []int{2, 32, 128} {
			b.Run(fmt.Sprintf("Nodes%dGroups%d", nodes, groups), func(b *testing.B) {
				config := sparseCatalogBenchmarkConfigsV1(b, nodes > 4, nodes)[3]
				for i := len(config.Groups); i < groups; i++ {
					config.Groups = append(config.Groups, FixedPeerTCPGroupV1{
						ID: raftcluster.GroupID(fmt.Sprintf("config-group-%03d", i)), BootstrapNode: "owner-1",
						Peers: []raftcluster.Peer{{ID: "owner-1", Address: fmt.Sprintf("127.6.0.%d:19000", i)}},
					})
				}
				raw, err := json.Marshal(config)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					client, err := NewFixedPeerTCPClientV1(config)
					if err != nil {
						b.Fatal(err)
					}
					client.Close()
				}
				b.StopTimer()
				b.ReportMetric(float64(len(raw)), "config_bytes")
			})
		}
	}
}
