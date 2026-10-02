package nativewire

import (
	"net"
	"os"
	"runtime"
	"testing"
)

// Kept identical in the copied base/head audit. The candidate releases its
// retained fixture setup before measuring the public production opener.
var fixedPeerColdResourceReleaseV1 = func([]FixedPeerTCPConfigV1) {}

func TestFixedPeerColdRoleBootstrapResourceV1(t *testing.T) {
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0}, {id: "b", vector: []float32{0, 1}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, seed.collection.MetaView())
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}
	fixedPeerColdResourceReleaseV1(configs)
	config := configs[1]
	fdCount := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return -1
		}
		return len(entries)
	}
	runtime.GC()
	beforeFD, beforeG := fdCount(), runtime.NumGoroutine()
	var before, opened runtime.MemStats
	runtime.ReadMemStats(&before)
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&opened)
	openedFD, openedG := fdCount(), runtime.NumGoroutine()
	if node.vector == nil || node.vector.topology != nil {
		node.Close()
		t.Fatal("audit owner is not cold")
	}
	held := 0
	for _, peers := range config.Vector.ShardAddresses {
		address := peers[config.NodeID]
		if address == "" {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			held++
		} else {
			_ = listener.Close()
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var closed runtime.MemStats
	runtime.ReadMemStats(&closed)
	fdDelta := -1
	if beforeFD >= 0 && openedFD >= 0 {
		fdDelta = openedFD - beforeFD
	}
	t.Logf("cold_role_bootstrap dormant_roles=1 held_dormant_sockets=%d before_fds=%d opened_fds=%d added_fds=%d closed_fds=%d added_goroutines=%d total_alloc_bytes=%d total_allocations=%d opened_heap_bytes=%d closed_heap_bytes=%d", held, beforeFD, openedFD, fdDelta, fdCount(), openedG-beforeG, opened.TotalAlloc-before.TotalAlloc, opened.Mallocs-before.Mallocs, int64(opened.HeapAlloc)-int64(before.HeapAlloc), int64(closed.HeapAlloc)-int64(before.HeapAlloc))
}
