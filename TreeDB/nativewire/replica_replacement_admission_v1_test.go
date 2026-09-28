package nativewire

import (
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func replacementAdmissionForTestV1(t *testing.T) (*peerNodeAdmissionV1, raftcluster.GroupID) {
	t.Helper()
	config := fixedPeerTestConfigsV1(t)[0]
	config.RaftListen = nil // preauthorized spare hosts no Raft group yet
	config.ResourceLimits = &PeerNodeLimitsV1{InflightBytes: 40 << 20, GroupInflightBytes: 32 << 20}
	a, err := newPeerNodeAdmissionV1(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.close() })
	return a, config.Groups[0].ID
}

func TestReplacementRaftAdmissionRegistersKnownGroupV1(t *testing.T) {
	a, group := replacementAdmissionForTestV1(t)
	key := "raft:" + string(group)
	if _, err := a.acquire(key, peerConnectionsV1, 1); !errors.Is(err, raftcluster.ErrInvalidConfig) {
		t.Fatalf("unhosted group unexpectedly admitted: %v", err)
	}
	before := a.sharedLimits
	if err := a.admitReplacementRaftGroupV1("unknown"); !errors.Is(err, raftcluster.ErrInvalidConfig) {
		t.Fatalf("unknown group: %v", err)
	}
	if err := a.admitReplacementRaftGroupV1(group); err != nil {
		t.Fatal(err)
	}
	want := peerResourceAmountsV1{4, 1, 4 << 20}
	for kind, amount := range want {
		if a.sharedLimits[kind] != before[kind]-amount || a.scopes[key].reserved[kind] != amount {
			t.Fatalf("reservation kind %d: before=%v after=%v", kind, before, a.sharedLimits)
		}
	}
	after := a.sharedLimits
	if err := a.admitReplacementRaftGroupV1(group); err != nil || a.sharedLimits != after {
		t.Fatalf("retry reserved twice: %v", err)
	}
	lease, err := a.acquire(key, peerConnectionsV1, 1)
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	a.draining.Store(true)
	if err := a.admitReplacementRaftGroupV1(group); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("draining retry: %v", err)
	}
	a.draining.Store(false)
	if err := a.close(); err != nil {
		t.Fatal(err)
	}
	if err := a.admitReplacementRaftGroupV1(group); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("closed retry: %v", err)
	}
}

func TestReplacementRaftAdmissionSaturationIsAtomicV1(t *testing.T) {
	a, group := replacementAdmissionForTestV1(t)
	// Existing native traffic has already used capacity needed by the new
	// reserve. Connection/request feasibility passes before bytes refuse.
	lease, err := a.acquire("native", peerBytesV1, a.sharedLimits[peerBytesV1])
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	beforeLimits, beforeShared, beforeCurrent, beforeCount := a.sharedLimits, a.shared, a.stats.Current, len(a.scopes)
	if err := a.admitReplacementRaftGroupV1(group); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("stole capacity from live traffic: %v", err)
	}
	if a.sharedLimits != beforeLimits || a.shared != beforeShared || a.stats.Current != beforeCurrent || len(a.scopes) != beforeCount {
		t.Fatal("failed reserve partially changed ledger")
	}
	lease.release()
	if err := a.admitReplacementRaftGroupV1(group); err != nil {
		t.Fatalf("released capacity did not permit retry: %v", err)
	}
}

func TestReplacementRaftAdmissionConcurrentRetryV1(t *testing.T) {
	a, group := replacementAdmissionForTestV1(t)
	before := a.sharedLimits
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 16; j++ {
				if err := a.admitReplacementRaftGroupV1(group); err != nil {
					t.Error(err)
					return
				}
				lease, err := a.acquire("raft:"+string(group), peerConnectionsV1, 1)
				if err != nil {
					t.Error(err)
					return
				}
				a.mu.Lock()
				for kind := range a.shared {
					if a.shared[kind] > a.sharedLimits[kind] || a.stats.Current[kind] > a.limits[kind] {
						t.Error("concurrent reserve exceeded node capacity")
					}
				}
				a.mu.Unlock()
				lease.release()
			}
		}()
	}
	workers.Wait()
	for kind, amount := range (peerResourceAmountsV1{4, 1, 4 << 20}) {
		if a.sharedLimits[kind] != before[kind]-amount || a.stats.Current[kind] != 0 {
			t.Fatalf("concurrent reserve/release mismatch: %+v", a.stats)
		}
	}
}
