package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type replacementGateTestTransportV1 struct {
	hraft.Transport
	inbox     chan hraft.RPC
	heartbeat func(hraft.RPC)
}

func (t *replacementGateTestTransportV1) Consumer() <-chan hraft.RPC            { return t.inbox }
func (t *replacementGateTestTransportV1) SetHeartbeatHandler(f func(hraft.RPC)) { t.heartbeat = f }

func replacementGateSeedForTestV1() (raftcluster.ReplacementSnapshotSeedV1, []byte, []byte) {
	payload := []byte("immutable native seed archive")
	configuration := hraft.EncodeConfiguration(hraft.Configuration{Servers: []hraft.Server{{ID: "old", Address: "127.0.0.1:19001", Suffrage: hraft.Voter}}})
	digest := func(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }
	seed := raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: "old", SnapshotID: "native-seed", Version: hraft.SnapshotVersionMax, Term: 4, Index: 9, ConfigurationIndex: 1, ConfigurationSHA256: digest(configuration), ArchiveSHA256: digest(payload), SizeBytes: int64(len(payload)), Manifest: raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: raftcluster.SnapshotManifestVersion1, NodeID: "old", GroupID: "group-a", LastIncludedTerm: 4, LastIncludedIndex: 9, AppliedCommandLSN: 3, LogicalDigestV1: strings.Repeat("a", 64), Scope: raftcluster.SnapshotScopeIdentityV1{ScopeRule: "single-group-v1", DatabaseScope: "database/default", CatalogScope: "catalog/default"}, CreatedAt: time.Unix(1700000000, 0).UTC()}}
	return seed, payload, configuration
}
func replacementGateRPCForTestV1(seed raftcluster.ReplacementSnapshotSeedV1, payload, configuration []byte) (hraft.RPC, <-chan hraft.RPCResponse) {
	responses := make(chan hraft.RPCResponse, 1)
	return hraft.RPC{Command: &hraft.InstallSnapshotRequest{SnapshotVersion: seed.Version, LastLogIndex: seed.Index, LastLogTerm: seed.Term, ConfigurationIndex: seed.ConfigurationIndex, Configuration: configuration, Size: seed.SizeBytes}, Reader: bytes.NewReader(payload), RespChan: responses}, responses
}
func replacementGateResultForTestV1(t *testing.T, responses <-chan hraft.RPCResponse) hraft.RPCResponse {
	t.Helper()
	select {
	case result := <-responses:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("gate response did not complete")
		return hraft.RPCResponse{}
	}
}
func TestReplacementPrejoinNativeCompletionAndHeartbeatFenceV1(t *testing.T) {
	seed, payload, configuration := replacementGateSeedForTestV1()
	inner := &replacementGateTestTransportV1{inbox: make(chan hraft.RPC, 8)}
	verifyEntered, verifyContinue := make(chan struct{}), make(chan struct{})
	var phase replacementReceiverPhaseV1
	gate, err := newReplacementPrejoinTransportV1(inner, seed, replacementReceiverPreparedV1, func(p replacementReceiverPhaseV1) error { phase = p; return nil }, func(raftcluster.ReplacementSnapshotSeedV1) error { close(verifyEntered); <-verifyContinue; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer gate.providerStopped()
	defer close(verifyContinue)
	var heartbeats atomic.Int32
	gate.SetHeartbeatHandler(func(rpc hraft.RPC) { heartbeats.Add(1); rpc.Respond(&hraft.AppendEntriesResponse{Success: true}, nil) })
	heartbeat := func() {
		rpcResponses := make(chan hraft.RPCResponse, 1)
		rpc := hraft.RPC{Command: &hraft.AppendEntriesRequest{}, RespChan: rpcResponses}
		inner.heartbeat(rpc)
		if result := replacementGateResultForTestV1(t, rpcResponses); !errors.Is(result.Error, raftcluster.ErrAdmissionUnavailable) {
			t.Fatalf("quarantine heartbeat=%v", result.Error)
		}
	}
	heartbeat()
	seedRPC, seedRPCResponses := replacementGateRPCForTestV1(seed, payload, configuration)
	inner.inbox <- seedRPC
	forwarded := <-gate.Consumer()
	if err := gate.allowEnrollment(); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("early add=%v", err)
	}
	heartbeat()
	// Native reading all bytes is still not completion or durable proof.
	if _, err := io.Copy(io.Discard, forwarded.Reader); err != nil {
		t.Fatal(err)
	}
	if gate.ordinaryAllowed() {
		t.Fatal("archive bytes opened replication")
	}
	forwarded.Respond(&hraft.InstallSnapshotResponse{Success: true}, nil)
	select {
	case <-verifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("native completion not verified")
	}
	heartbeat()
	if err := gate.allowEnrollment(); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("verification outstanding add=%v", err)
	}
	// No transport cancellation shortcut exists: verification owns the fence.
	verifyContinue <- struct{}{}
	if result := replacementGateResultForTestV1(t, seedRPCResponses); result.Error != nil {
		t.Fatal(result.Error)
	}
	if phase != replacementReceiverInstalledV1 || gate.ordinaryAllowed() {
		t.Fatal("install did not retain pre-enrollment quarantine")
	}
	if err := gate.allowEnrollment(); err != nil {
		t.Fatal(err)
	}
	delayed, delayedResponses := replacementGateRPCForTestV1(seed, payload, configuration)
	inner.inbox <- delayed
	if result := replacementGateResultForTestV1(t, delayedResponses); !errors.Is(result.Error, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("delayed seed=%v", result.Error)
	}
	ordinary, ordinaryResponses := replacementGateRPCForTestV1(seed, payload, configuration)
	ordinary.Command.(*hraft.InstallSnapshotRequest).LastLogIndex++
	inner.inbox <- ordinary
	got := <-gate.Consumer()
	if got.Reader != ordinary.Reader || got.RespChan != ordinary.RespChan {
		t.Fatal("ordinary native snapshot gained seed wrappers")
	}
	got.Respond(&hraft.InstallSnapshotResponse{Success: true}, nil)
	if result := replacementGateResultForTestV1(t, ordinaryResponses); result.Error != nil {
		t.Fatal(result.Error)
	}
	rpcResponses := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{Command: &hraft.AppendEntriesRequest{}, RespChan: rpcResponses}
	inner.heartbeat(rpc)
	if result := replacementGateResultForTestV1(t, rpcResponses); result.Error != nil || heartbeats.Load() != 1 {
		t.Fatalf("post-add heartbeat=%v", result.Error)
	}
}

func TestReplacementPrejoinCorruptSeedAndRestartRemainQuarantinedV1(t *testing.T) {
	for _, phase := range []replacementReceiverPhaseV1{replacementReceiverInstallingV1, replacementReceiverInstalledV1, replacementReceiverAddIntentV1} {
		t.Run(string(phase), func(t *testing.T) {
			seed, payload, configuration := replacementGateSeedForTestV1()
			inner := &replacementGateTestTransportV1{inbox: make(chan hraft.RPC, 1)}
			gate, err := newReplacementPrejoinTransportV1(inner, seed, phase, func(replacementReceiverPhaseV1) error { t.Fatal("replay changed phase"); return nil }, func(raftcluster.ReplacementSnapshotSeedV1) error {
				t.Fatal("replay called install verifier")
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer gate.providerStopped()
			rpc, rpcResponses := replacementGateRPCForTestV1(seed, payload, configuration)
			inner.inbox <- rpc
			if result := replacementGateResultForTestV1(t, rpcResponses); !errors.Is(result.Error, raftcluster.ErrAdmissionUnavailable) {
				t.Fatalf("replay=%v", result.Error)
			}
		})
	}
	seed, payload, configuration := replacementGateSeedForTestV1()
	payload = bytes.Clone(payload)
	payload[0] ^= 1
	inner := &replacementGateTestTransportV1{inbox: make(chan hraft.RPC, 1)}
	gate, err := newReplacementPrejoinTransportV1(inner, seed, replacementReceiverPreparedV1, func(replacementReceiverPhaseV1) error { return nil }, func(raftcluster.ReplacementSnapshotSeedV1) error { t.Fatal("corrupt archive verified"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer gate.providerStopped()
	rpc, rpcResponses := replacementGateRPCForTestV1(seed, payload, configuration)
	inner.inbox <- rpc
	forwarded := <-gate.Consumer()
	if _, err := io.Copy(io.Discard, forwarded.Reader); err != nil {
		t.Fatal(err)
	}
	forwarded.Respond(&hraft.InstallSnapshotResponse{Success: true}, nil)
	if result := replacementGateResultForTestV1(t, rpcResponses); !errors.Is(result.Error, raftcluster.ErrInvalidSnapshotManifest) {
		t.Fatalf("corrupt seed=%v", result.Error)
	}
	if err := gate.allowEnrollment(); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("corrupt seed enrollment=%v", err)
	}
}

func TestReplacementSnapshotSeedRefusesPathLikeNativeIDV1(t *testing.T) {
	seed, _, _ := replacementGateSeedForTestV1()
	for _, id := range []string{"", ".", "..", "../seed", "a/b", `a\b`, "seed\x00suffix"} {
		bad := seed
		bad.SnapshotID = id
		if err := bad.Validate(); !errors.Is(err, raftcluster.ErrInvalidSnapshotManifest) {
			t.Errorf("snapshot ID %q accepted: %v", id, err)
		}
	}
	if err := seed.Validate(); err != nil {
		t.Fatal(err)
	}
}
