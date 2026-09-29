package nativewire

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestFixedPeerImmutableVectorNodesOnlyStandbyV1(t *testing.T) {
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{0, 1}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, seed.collection.MetaView())
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}

	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return listener.Addr().String()
	}
	const spareID raftcluster.NodeID = "standby"
	spare := configs[0]
	spare.NodeID, spare.ListenAddress = spareID, address()
	spare.Nodes = append(append([]FixedPeerTCPNodeV1(nil), spare.Nodes...), FixedPeerTCPNodeV1{ID: spareID, Address: spare.ListenAddress})
	vector := *spare.Vector
	vector.PublicAddresses = make(map[raftcluster.NodeID]string, len(spare.Vector.PublicAddresses)+1)
	for id, addr := range spare.Vector.PublicAddresses {
		vector.PublicAddresses[id] = addr
	}
	vector.PublicAddresses[spareID] = address()
	spare.Vector = &vector
	spare.RaftListen = map[raftcluster.GroupID]string{}
	spare.DataRoot, spare.RaftRoot = filepath.Join(t.TempDir(), "data"), filepath.Join(t.TempDir(), "raft")
	spare.ClusterID = "immutable-standby"
	ca := newPeerCAFixtureV1(t)
	spare.Credentials = ca.issue(t, spare.ClusterID, string(spareID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))

	if _, err := InspectFixedPeerTCPConfigV1(spare); err != nil {
		t.Fatalf("preauthorized standby rejected: %v", err)
	}
	client, err := NewFixedPeerTCPClientV1(spare)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		runtime, err := OpenFixedPeerTCPRuntimeV1(spare)
		if err != nil {
			t.Fatalf("standby open %d: %v", attempt, err)
		}
		status, err := client.Status(ctx, spareID)
		if err != nil || status.CatalogRole != "consumer" || len(status.Groups) != 0 || runtime.meta != nil || runtime.vector != nil {
			t.Fatalf("standby claimed serving authority: status=%+v err=%v", status, err)
		}
		if attempt == 1 && status.RecoveryState != "reopened" {
			t.Fatalf("standby did not reopen: %+v", status)
		}
		publicListener, err := net.Listen("tcp", vector.PublicAddresses[spareID])
		if err != nil {
			t.Fatalf("standby opened a public vector listener: %v", err)
		}
		_ = publicListener.Close()
		if _, err := runtime.searchVectorPartitionStrictV1(ctx, public.SearchRequestV1{}); err == nil {
			t.Fatal("standby served strict vector search")
		}
		if readiness, err := runtime.ReadinessV1(ctx); err == nil || readiness.Ready {
			t.Fatalf("standby claimed readiness: %+v, %v", readiness, err)
		}
		if _, err := runtime.validateReplacementBeginV1(raftplacement.ReplicaReplacementBeginV1{}); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
			t.Fatalf("standby admitted vector replacement BEGIN: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
