package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableVectorOwnerLeaderIgnoresStaleCatalogHintV1(t *testing.T) {
	var members []raftcluster.NodeID
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			_, _ = io.Copy(io.Discard, request.Body)
			if request.URL.Path != "/v1/status" {
				t.Errorf("leader discovery sent %s", request.URL.Path)
				return
			}
			_ = json.NewEncoder(w).Encode(fixedPeerReplyV1{
				NodeID: raftcluster.NodeID(request.Header.Get("X-TreeDB-Node")), ConfigDigest: request.Header.Get("X-TreeDB-Config"),
				Status: FixedPeerTCPStatusV1{Groups: []FixedPeerTCPGroupStatusV1{{RuntimeStatusV1: raftcluster.RuntimeStatusV1{GroupID: "group-b", LeaderID: members[1]}}}},
			})
		}))
		t.Cleanup(server.Close)
		servers[i] = server
	}
	config := fixedPeerTestConfigsV1(t)[0]
	for _, group := range config.Groups {
		if group.ID == "group-b" {
			for _, peer := range group.Peers {
				members = append(members, peer.ID)
			}
		}
	}
	if len(members) != 2 {
		t.Fatalf("owner fixture has %d members, want two", len(members))
	}
	for i := range config.Nodes {
		for memberIndex, member := range members {
			if config.Nodes[i].ID == member {
				config.Nodes[i].Address = strings.TrimPrefix(servers[memberIndex].URL, "http://")
			}
		}
	}
	client, err := NewFixedPeerTCPClientV1(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	runtime := &FixedPeerTCPRuntimeV1{config: config, client: client}
	resolved, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{
		ID: "group-b", Members: members, LeaderHint: members[0],
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	leader, err := runtime.immutableVectorOwnerLeaderV1(ctx, resolved, "group-b")
	if err != nil || leader != members[1] {
		t.Fatalf("stale catalog leader hint selected %q, err=%v, want %q", leader, err, members[1])
	}
	wrong, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{
		ID: "group-b", Members: []raftcluster.NodeID{members[0]}, LeaderHint: members[0],
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.immutableVectorOwnerLeaderV1(ctx, wrong, "group-b"); !errors.Is(err, ErrFixedPeerVectorWrongOwnerV1) {
		t.Fatalf("mismatched fixed/catalog membership accepted: %v", err)
	}
}

func TestImmutableVectorReadyRetryKeepsCommittedAppliedIndexV1(t *testing.T) {
	committed := []raftplacement.VectorPartitionLifecycleGroupReadyV1{{
		GroupID: "group-b", AppliedIndex: 17, AssetSetDigest: strings.Repeat("a", 64),
	}}
	advanced := raftplacement.VectorPartitionLifecycleGroupReadyV1{
		GroupID: "group-b", AppliedIndex: 21, AssetSetDigest: committed[0].AssetSetDigest,
	}
	alreadyReady, err := immutableVectorReadyCommittedV1(committed, advanced)
	if err != nil || !alreadyReady || committed[0].AppliedIndex != 17 {
		t.Fatalf("partial READY retry rewrote committed proof: already=%v, committed=%+v, err=%v", alreadyReady, committed, err)
	}
	advanced.GroupID = "group-c"
	if alreadyReady, err := immutableVectorReadyCommittedV1(committed, advanced); err != nil || alreadyReady {
		t.Fatalf("unready owner was skipped: already=%v, err=%v", alreadyReady, err)
	}
	advanced.GroupID = "group-b"
	advanced.AppliedIndex = 16
	if _, err := immutableVectorReadyCommittedV1(committed, advanced); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("stale applied proof accepted: %v", err)
	}
	advanced.AppliedIndex = 21
	advanced.AssetSetDigest = strings.Repeat("b", 64)
	if _, err := immutableVectorReadyCommittedV1(committed, advanced); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("changed assets accepted: %v", err)
	}
}
