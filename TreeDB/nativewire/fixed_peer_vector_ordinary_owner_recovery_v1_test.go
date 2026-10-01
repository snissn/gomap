package nativewire

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestImmutableOwnerOrdinaryServingRecoversCurrentFSMDBV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateQualificationRecoveryV1(t, true, true, false, true)
}

func TestImmutableOwnerWarmCatalogReadCloseV1(t *testing.T) {
	ctx, _, runtimes, _, _ := immutableOwnerReplacementFixtureV1(t)
	owner, leader := runtimes[1], runtimes[3]
	vector := owner.vector
	vector.initMu.Lock()
	cached := vector.topology != nil && vector.source != nil
	vector.initMu.Unlock()
	if !cached || owner.meta != nil {
		t.Fatal("fixture did not install a cached catalog-consumer owner")
	}
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/vector-catalog-read" {
			leader.server.Handler.ServeHTTP(w, request)
			return
		}
		if request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 {
			t.Error("catalog read was not authenticated")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if node, err := leader.client.security.identity(request.TLS.PeerCertificates[0]); err != nil || node != owner.config.NodeID {
			t.Errorf("catalog client identity: %s %v", node, err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			t.Error(err)
			return
		}
		close(entered)
		<-request.Context().Done()
		close(canceled)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{leader.client.security.certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: leader.client.security.roots}
	server.StartTLS()
	defer server.Close()
	defer server.CloseClientConnections()

	// The fixture is quiescent. Override only this owner's read client, retaining
	// its real credentialed transport/admission; restore it after all work joins.
	original := owner.client
	probe := *original
	probe.addresses = make(map[raftcluster.NodeID]string, len(original.addresses))
	for node, address := range original.addresses {
		probe.addresses[node] = address
	}
	address := server.Listener.Addr().String()
	probe.addresses[leader.config.NodeID] = address
	transport := original.readHTTP.Transport.(*http.Transport).Clone()
	originalDial := transport.DialTLSContext
	transport.DialTLSContext = func(ctx context.Context, network, destination string) (net.Conn, error) {
		if destination == address {
			return original.peerTransport.dialScope(ctx, destination, leader.config.NodeID, "control")
		}
		return originalDial(ctx, network, destination)
	}
	defer transport.CloseIdleConnections()
	readClient := *original.readHTTP
	readClient.Transport = transport
	probe.readHTTP = &readClient
	before := original.peerTransport.ResourceStatsV1()
	owner.client = &probe
	warmCtx, cancel := context.WithCancel(ctx)
	warmDone := make(chan error, 1)
	go func() { _, err := vector.warmImmutableTopologyV1(warmCtx); warmDone <- err }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-warmDone
		}
		owner.client = original
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	vector.initMu.Lock()
	tracked := vector.initDone != nil && vector.initCancel != nil
	vector.initMu.Unlock()
	if !tracked {
		t.Fatal("initial catalog read escaped Close tracker")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- vector.Close() }()
	select {
	case err := <-warmDone:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close did not cancel initial catalog read: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close during initial catalog read: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	vector.initMu.Lock()
	late := vector.topology != nil || vector.source != nil || vector.initDone != nil || vector.initCancel != nil
	vector.initMu.Unlock()
	if late {
		t.Fatal("catalog Warm retained state after Close")
	}
	after := original.peerTransport.ResourceStatsV1()
	if after.Current[peerRequestsV1] != before.Current[peerRequestsV1] || after.Current[peerBytesV1] != before.Current[peerBytesV1] {
		t.Fatalf("catalog Warm leaked admission: before=%v after=%v", before.Current, after.Current)
	}
}

func assertImmutableOwnerServingBindingLifetimeV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, target, owner *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	vector, data := owner.vector, owner.localDataV1(command.GroupID)
	vector.initMu.Lock()
	oldTopology, oldSource, oldGuard := vector.topology, vector.source, vector.servingGuard
	vector.initMu.Unlock()
	if oldTopology == nil || oldSource == nil || oldGuard == nil {
		t.Fatal("Warm did not install captured owner binding")
	}

	// Export actual installed native bytes; restore replaces the FSM DB even
	// though the immutable generation and semantic command history stay exact.
	snapshotBytes := func() []byte {
		snapshot, err := data.fsm.ExportRaftSnapshotV1()
		if err != nil {
			t.Fatal(err)
		}
		archive, err := snapshot.OpenArchive()
		if err != nil {
			_ = snapshot.Release()
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(archive)
		if err := errors.Join(readErr, archive.Close(), snapshot.Release()); err != nil {
			t.Fatal(err)
		}
		return payload
	}
	payload := snapshotBytes()
	request := replacementOwnerQualificationRequestV1(t, ctx, target, command)
	request.TargetNodeID = command.OldNodeID
	service := oldTopology.services[command.GroupID]
	entered, resume := make(chan struct{}), make(chan struct{})
	service.postSearchGuard = func() error {
		close(entered)
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
		return oldGuard()
	}
	result := make(chan struct {
		response VectorPartitionShardSearchResponseV1
		err      error
	}, 1)
	go func() {
		response, err := service.Search(ctx, request)
		result <- struct {
			response VectorPartitionShardSearchResponseV1
			err      error
		}{response, err}
	}()
	select {
	case <-entered:
	case got := <-result:
		t.Fatalf("old ordinary request did not reach final guard: %v", got.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Always release the paused response if a subsequent assertion fails.
	released := false
	defer func() {
		if !released {
			close(resume)
		}
	}()
	if err := data.fsm.InstallRaftSnapshotV1(bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if err := oldGuard(); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("old DB witness survived second native swap: %v", err)
	}
	backend := &fixedPeerVectorBackendV1{runtime: vector}
	if health, err := backend.OperationsHealthV1(ctx); health.Ready || !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("health silently rebound second swap: %+v %v", health, err)
	}
	id := public.GenerationIDV1{Index: command.OwnerPreparation.Index.IndexName, Generation: command.OwnerPreparation.Generation}
	if _, err := backend.GenerationStatusV1(ctx, id); err == nil {
		t.Fatal("status admitted stale serving binding")
	}
	if _, err := vector.ensureImmutableTopologyV1(ctx); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("ensure silently rebound second swap: %v", err)
	}
	var assetPath string
	for _, asset := range owner.config.Vector.Manifest.Assets {
		for _, placement := range owner.config.Vector.Manifest.Placements {
			if placement.PartitionID == asset.PartitionID && placement.GroupID == string(command.GroupID) {
				assetPath = filepath.Join(backenddb.ColumnAssetRootDirPath(data.db.Dir()), filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
				break
			}
		}
		if assetPath != "" {
			break
		}
	}
	if assetPath == "" {
		t.Fatal("no ordinary hosted asset")
	}
	hidden := assetPath + ".ordinary-warm-test"
	if err := os.Rename(assetPath, hidden); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := os.Stat(hidden); err == nil {
			_ = os.Rename(hidden, assetPath)
		}
	}()
	if _, err := vector.warmImmutableTopologyV1(ctx); err == nil {
		t.Fatal("explicit Warm admitted missing hosted asset")
	}
	if err := os.Rename(hidden, assetPath); err != nil {
		t.Fatal(err)
	}
	if _, err := vector.warmImmutableTopologyV1(ctx); err != nil {
		t.Fatalf("second explicit ordinary Warm: %v", err)
	}
	close(resume)
	released = true
	select {
	case got := <-result:
		if got.err == nil || len(got.response.Partials) != 0 {
			t.Fatalf("old in-flight response survived second Warm: %+v %v", got.response, got.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := oldGuard(); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("old guard consulted future serving DB: %v", err)
	}
	if pin, err := oldSource.PinVectorPartitionGenerationV1(ctx, id.Index, id.Generation); err == nil {
		_ = pin.Close()
		t.Fatal("old source reopened after second Warm")
	}
	assertReplacementOwnerQualificationParityV1(t, ctx, client, target, owner, command, VectorPartitionShardSearchResponseV1{})
	if health, err := backend.OperationsHealthV1(ctx); err != nil || !health.Ready {
		t.Fatalf("warmed recovered owner health: %+v %v", health, err)
	}
	if _, err := backend.GenerationStatusV1(ctx, id); err != nil {
		t.Fatalf("warmed recovered owner status: %v", err)
	}
	// Startup-only preparation remains stale despite successful serving Warm.
	if _, err := owner.stageImmutableVectorLocalV1(ctx); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("serving Warm rebound staging manager: %v", err)
	}

	// Hold the actual root barrier while construction is registered. Cancellation
	// and Close must finish without releasing it or installing serving state;
	// registration alone does not prove entry into the barrier wait.
	payload = snapshotBytes()
	if err := data.fsm.InstallRaftSnapshotV1(bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	barrierEntered, barrierResume, barrierDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		barrierDone <- collections.WithVectorPartitionStorageBarrierWithContextV1(ctx, data.db.Dir(), func() error {
			close(barrierEntered)
			select {
			case <-barrierResume:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-barrierEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	barrierReleased := false
	defer func() {
		if !barrierReleased {
			close(barrierResume)
		}
	}()
	vector.initMu.Lock()
	beforeCanceledTopology, beforeCanceledSource := vector.topology, vector.source
	vector.initMu.Unlock()
	canceledCtx, cancelWarm := context.WithCancel(ctx)
	canceledDone := make(chan error, 1)
	go func() { _, err := vector.warmImmutableTopologyV1(canceledCtx); canceledDone <- err }()
	fixedPeerWaitV1(t, ctx, func() bool { vector.initMu.Lock(); defer vector.initMu.Unlock(); return vector.initDone != nil })
	cancelWarm()
	select {
	case err := <-canceledDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Warm cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	vector.initMu.Lock()
	// Cancellation can precede detaching the unchanged stale binding, while the
	// first catalog read is pending. It must never publish a new binding.
	canceledInstall := (vector.topology != nil && vector.topology != beforeCanceledTopology) ||
		(vector.source != nil && vector.source != beforeCanceledSource) || vector.initDone != nil || vector.initCancel != nil
	vector.initMu.Unlock()
	if canceledInstall {
		t.Fatal("canceled Warm installed serving state")
	}
	warmDone := make(chan error, 1)
	go func() { _, err := vector.warmImmutableTopologyV1(ctx); warmDone <- err }()
	fixedPeerWaitV1(t, ctx, func() bool { vector.initMu.Lock(); defer vector.initMu.Unlock(); return vector.initDone != nil })
	closeDone := make(chan error, 1)
	go func() { closeDone <- vector.Close() }()
	select {
	case err := <-warmDone:
		if err == nil {
			t.Fatal("Close admitted blocked Warm")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close while Warm blocked: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	vector.initMu.Lock()
	late := vector.topology != nil || vector.source != nil || vector.initDone != nil
	vector.initMu.Unlock()
	if late {
		t.Fatal("Warm installed topology after Close")
	}
	if _, err := vector.warmImmutableTopologyV1(ctx); err == nil {
		t.Fatal("closed runtime admitted new Warm")
	}
	close(barrierResume)
	barrierReleased = true
	select {
	case err := <-barrierDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
