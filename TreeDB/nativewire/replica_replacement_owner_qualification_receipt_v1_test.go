package nativewire

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableOwnerReplacementHistoricalQualificationReceiptV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateQualificationV1(t, true, true, true)
}

// Codec-valid semantic admission fixture; it executes no ANN or authority RPC.
func TestReplacementOwnerQualificationReceiptRequestAdmissionV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	digest := strings.Repeat("a", 64)
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index:  raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}, CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: digest, IndexEpoch: 1, CatalogEpoch: 1, CatalogDigest: digest},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2}, Generation: 7,
		Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: digest, PlacementDigest: digest},
	}
	command := raftplacement.ReplicaReplacementBeginV1{OperationID: "receipt-admission", ConfigDigest: digest, ExpectedEpoch: 1, CatalogDigest: digest, GroupID: config.Groups[0].ID, OldNodeID: "old-node", NewPeer: raftcluster.Peer{ID: config.NodeID, Address: "127.0.0.1:1"}, OwnerPreparation: &identity}
	config.Vector = &FixedPeerTCPVectorConfigV1{Identity: identity}
	client := &FixedPeerTCPClientV1{config: config, digest: digest, peerTransport: transport, security: transport.security}
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.TargetNodeID, request.TargetGroupID = command.NewPeer.ID, command.GroupID
	if _, err := raftplacement.EncodeReplicaReplacementBeginV1(command); err != nil {
		t.Fatal(err)
	}
	a := transport.admission
	held, err := a.acquire("control-write", peerBytesV1, a.scopes["control-write"].limits[peerBytesV1])
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()
	before := transport.ResourceStatsV1()
	if result, err := client.CommitReplicaReplacementOwnerQualificationV1(t.Context(), command, request); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || !reflect.DeepEqual(result, raftplacement.ReplicaReplacementStateV1{}) || transport.ResourceStatsV1().Current != before.Current || transport.ResourceStatsV1().WrittenBytes != before.WrittenBytes {
		t.Fatalf("caller byte refusal reached catalog or leaked work: %+v %v %+v", result, err, transport.ResourceStatsV1())
	}
	held.release()
	// No catalog addresses exist: later discovery refusal must release the
	// successful pre-encoding request reservation, without a wire send.
	if result, err := client.CommitReplicaReplacementOwnerQualificationV1(t.Context(), command, request); err == nil || !reflect.DeepEqual(result, raftplacement.ReplicaReplacementStateV1{}) || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) || transport.ResourceStatsV1().WrittenBytes != before.WrittenBytes {
		t.Fatalf("discovery refusal leaked caller request: %+v %v %+v", result, err, transport.ResourceStatsV1())
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.CommitReplicaReplacementOwnerQualificationV1(canceled, command, request); !errors.Is(err, context.Canceled) || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
		t.Fatalf("canceled caller leaked work: %v", err)
	}
}

func assertReplacementOwnerReceiptV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, target, catalog *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	request := replacementOwnerQualificationRequestV1(t, ctx, target, command)
	// Exhaust only the producer's retained result/hash scope. Fresh catalog
	// reads remain available; refusal must precede another actual ANN cache hit.
	a := catalog.client.peerTransport.admission
	scope := "shard:" + string(command.GroupID)
	held, err := a.acquire(scope, peerBytesV1, a.scopes[scope].limits[peerBytesV1])
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()
	slot := &target.localDataV1(command.GroupID).replacementWork
	slot.mu.Lock()
	source := slot.ownerSource
	slot.mu.Unlock()
	if source == nil {
		t.Fatal("receipt fixture has no retained private source")
	}
	cacheBefore := source.Stats()
	catalogBefore, _ := catalog.authority.Status()
	refused, refusalErr := client.CommitReplicaReplacementOwnerQualificationV1(ctx, command, request)
	catalogAfter, _ := catalog.authority.Status()
	if !errors.Is(refusalErr, raftcluster.ErrAdmissionUnavailable) || refused.OwnerQualification != nil || source.Stats() != cacheBefore || catalogAfter.AppliedIndex != catalogBefore.AppliedIndex {
		t.Fatalf("retained-result exhaustion reached ANN or commit: %+v %v cache=%+v", refused, refusalErr, source.Stats())
	}
	held.release()
	before, _ := catalog.authority.Status()
	var memoryBefore, memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	started := time.Now()
	committed, err := client.CommitReplicaReplacementOwnerQualificationV1(ctx, command, request)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&memoryAfter)
	if err != nil || committed.Phase != raftplacement.ReplicaReplacementAddIntentV1 || committed.OwnerQualification == nil {
		t.Fatalf("first historical receipt: %+v %v", committed, err)
	}
	after, _ := catalog.authority.Status()
	if after.AppliedIndex <= before.AppliedIndex {
		t.Fatal("receipt did not commit a catalog entry")
	}
	// The retained result/hash reservation ends after submission, before reply.
	a.mu.Lock()
	retainedAfter := a.scopes[scope].used[peerBytesV1]
	a.mu.Unlock()
	if retainedAfter != 0 {
		t.Fatalf("committed receipt retained result bytes: %d", retainedAfter)
	}
	t.Logf("historical_receipt sample=1 scope=caller+five_raft_nodes+background+TLS+private_ANN+fresh_authority+tail+commit ns=%d ops_per_second=%.2f global_bytes=%d global_allocs=%d", elapsed.Nanoseconds(), float64(time.Second)/float64(elapsed), memoryAfter.TotalAlloc-memoryBefore.TotalAlloc, memoryAfter.Mallocs-memoryBefore.Mallocs)
	owned, err := raftplacement.EncodeReplicaReplacementStateV1(committed)
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID += "-retry"
	request.DeadlineUnixNano = time.Now().Add(time.Minute).UnixNano()
	retry, err := client.CommitReplicaReplacementOwnerQualificationV1(ctx, command, request)
	if err != nil {
		t.Fatal(err)
	}
	retryBytes, err := raftplacement.EncodeReplicaReplacementStateV1(retry)
	status, _ := catalog.authority.Status()
	if err != nil || !bytes.Equal(owned, retryBytes) || status.AppliedIndex != after.AppliedIndex {
		t.Fatalf("exact historical retry changed owned bytes/index: %v", err)
	}
	changed := request
	changed.Query = append([]float32(nil), request.Query...)
	changed.Query[0] += 0.125
	if _, err := client.CommitReplicaReplacementOwnerQualificationV1(ctx, command, changed); !errors.Is(err, raftplacement.ErrCatalogMetaConflict) {
		t.Fatalf("different query receipt=%v", err)
	}
	for _, name := range []string{"unmarked", "different-BEGIN", "wrong-target"} {
		badCommand, badRequest := command, request
		switch name {
		case "unmarked":
			badCommand.OwnerPreparation = nil
		case "different-BEGIN":
			badCommand.OperationID += "-other"
		case "wrong-target":
			badRequest.TargetNodeID = command.OldNodeID
		}
		if state, err := client.CommitReplicaReplacementOwnerQualificationV1(ctx, badCommand, badRequest); err == nil || state.OwnerQualification != nil {
			t.Fatalf("%s acquired receipt: %+v %v", name, state, err)
		}
	}
	targetClient, err := NewFixedPeerTCPClientV1(target.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(targetClient.Close)
	if state, err := targetClient.CommitReplicaReplacementOwnerQualificationV1(ctx, command, request); !errors.Is(err, errPeerAuthenticationV1) || state.OwnerQualification != nil {
		t.Fatalf("noncatalog target published receipt: %+v %v", state, err)
	}
	injected, err := raftplacement.EncodeReplicaReplacementOwnerQualificationCommandV1(raftplacement.ReplicaReplacementOwnerQualificationCommandV1{State: committed})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.call(ctx, catalog.config.NodeID, "catalog-publish", fixedPeerRequestV1{Entry: injected}, true); err == nil {
		t.Fatal("generic external catalog publication accepted receipt bytes")
	}
	if _, err := client.call(ctx, catalog.config.NodeID, "replacement-advance", fixedPeerRequestV1{Entry: owned}, true); !errors.Is(err, raftplacement.ErrCatalogMetaConflict) {
		t.Fatalf("generic advance accepted receipt: %v", err)
	}
	final, _ := catalog.authority.Status()
	current, err := catalog.authority.ReplicaReplacementStateV1(command.GroupID)
	currentBytes, encodeErr := raftplacement.EncodeReplicaReplacementStateV1(current)
	if err != nil || encodeErr != nil || final.AppliedIndex != after.AppliedIndex || !bytes.Equal(currentBytes, owned) {
		t.Fatal("receipt refusals changed catalog authority")
	}
}
