package nativewire

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableVectorBackendAuthorityReadCloseV1(t *testing.T) {
	ctx, _, runtimes, _, _ := immutableOwnerReplacementFixtureV1(t)
	router, leader := runtimes[0], runtimes[3]
	router.vector.initMu.Lock()
	cached := router.vector.topology != nil && router.vector.backend != nil
	// Keep the real installed topology. Only its borrowing backend is cold,
	// forcing the subsequent ACTIVE read after the initializer's first fence.
	router.vector.backend = nil
	router.vector.initMu.Unlock()
	if !cached {
		t.Fatal("fixture did not install the ordinary router backend")
	}
	assertVectorBackendAuthorityReadCloseV1(t, ctx, router, leader, "/v1/catalog-read", 1)
}

func TestMutableVectorBackendLifecycleReadCloseV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	seed := fixedPeerVectorSeedV1(t)
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	ca := newPeerCAFixtureV1(t)
	for i := range configs {
		configs[i].ClusterID = "mutable-backend-close"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		group := "group-b"
		if configs[i].NodeID == "ingress" {
			group = "group-a"
		}
		dir := filepath.Join(configs[i].DataRoot, group)
		if err := os.CopyFS(dir, os.DirFS(seed.dir)); err != nil {
			t.Fatal(err)
		}
		if err := backenddb.RebindDurableRootSnapshotV1(dir); err != nil {
			t.Fatal(err)
		}
		bootstrapFixedPeerVectorTrustedGenesisV1(t, configs[i], seed.appliedCommandLSN)
	}
	runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	t.Cleanup(func() {
		for _, runtime := range runtimes {
			if runtime != nil {
				if err := runtime.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i, config := range configs {
		var err error
		runtimes[i], err = OpenFixedPeerTCPRuntimeV1(config)
		if err != nil {
			t.Fatal(err)
		}
	}
	owner, leader := runtimes[1], runtimes[2]
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, runtime := range runtimes {
			status, err := owner.client.Status(ctx, runtime.config.NodeID)
			if err != nil || status.CatalogRaft.LeaderID != leader.config.NodeID || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, seed.catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.client.PublishCatalog(ctx, leader.config.NodeID, command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		status, ok := owner.authority.Status()
		return ok && status.Epoch == record.Epoch && status.Digest == record.Digest
	})
	if _, exists := owner.authority.VectorPartitionLifecycleRecordV1(owner.config.Vector.Identity); exists {
		t.Fatal("fixture activated lifecycle before lazy backend construction")
	}
	assertVectorBackendAuthorityReadCloseV1(t, ctx, owner, leader, "/v1/vector-lifecycle", 0)
}

// Done is observed only when beginInitialization reaches its occupied-slot
// select, so cancellation below exercises a real waiter rather than preflight.
type vectorInitializationWaitContextV1 struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *vectorInitializationWaitContextV1) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func assertVectorBackendAuthorityReadCloseV1(t *testing.T, ctx context.Context, node, leader *FixedPeerTCPRuntimeV1, operation string, forward int32) {
	t.Helper()
	entered, canceled := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != operation {
			leader.server.Handler.ServeHTTP(w, request)
			return
		}
		if request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 {
			t.Error("backend authority request was not authenticated")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if identity, err := leader.client.security.identity(request.TLS.PeerCertificates[0]); err != nil || identity != node.config.NodeID {
			t.Errorf("backend authority identity: %s %v", identity, err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if calls.Add(1) <= forward {
			leader.server.Handler.ServeHTTP(w, request)
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
	original := node.client
	probe := *original
	probe.addresses = make(map[raftcluster.NodeID]string, len(original.addresses))
	for id, address := range original.addresses {
		probe.addresses[id] = address
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
	readClient, writeClient := *original.readHTTP, *original.http
	readClient.Transport, writeClient.Transport = transport, transport
	probe.readHTTP, probe.http = &readClient, &writeClient
	before := original.peerTransport.ResourceStatsV1()
	node.client = &probe
	workCtx, cancel := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { _, err := node.vector.ensureBackendV1(workCtx); workDone <- err }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-workDone
		}
		node.client = original
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A real blocked authority request must not hold Close's snapshot mutex.
	if !node.vector.initMu.TryLock() {
		t.Fatal("backend authority I/O holds initMu")
	}
	done := node.vector.initDone
	tracked := done != nil && node.vector.initCancel != nil
	node.vector.initMu.Unlock()
	if !tracked {
		t.Fatal("backend authority read escaped Close tracker")
	}
	waitCtx, waitCancel := context.WithCancel(ctx)
	waiting := &vectorInitializationWaitContextV1{Context: waitCtx, waiting: make(chan struct{})}
	waitDone := make(chan error, 1)
	go func() { _, err := node.vector.ensureBackendV1(waiting); waitDone <- err }()
	select {
	case <-waiting.waiting:
	case <-ctx.Done():
		waitCancel()
		t.Fatal(ctx.Err())
	}
	waitCancel()
	if err := <-waitDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("occupied-slot waiter cancellation: %v", err)
	}
	node.vector.initMu.Lock()
	unchanged := node.vector.initDone == done && node.vector.initCancel != nil
	node.vector.initMu.Unlock()
	if !unchanged {
		t.Fatal("canceled waiter cleared builder ownership")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- node.vector.Close() }()
	select {
	case err := <-workDone:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close did not cancel backend authority I/O: %v", err)
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
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	node.vector.initMu.Lock()
	late := node.vector.backend != nil || node.vector.topology != nil || node.vector.source != nil || node.vector.initDone != nil || node.vector.initCancel != nil
	node.vector.initMu.Unlock()
	if late {
		t.Fatal("backend construction installed state after Close")
	}
	after := original.peerTransport.ResourceStatsV1()
	if after.Current[peerRequestsV1] != before.Current[peerRequestsV1] || after.Current[peerBytesV1] != before.Current[peerBytesV1] {
		t.Fatalf("backend initialization leaked admission: before=%v after=%v", before.Current, after.Current)
	}
}
