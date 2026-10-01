package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func splitInsertRequestV1(f fixedPeerVectorReadyFixtureV1, n int) public.InsertRequestV1 {
	return public.InsertRequestV1{Version: 1, Generation: f.Generation, IdempotencyKey: []byte(fmt.Sprintf("split-attempt-%03d", n)),
		ID: []byte(fmt.Sprintf("split-document-%03d", n)), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"canonical-source"}`), Deadline: time.Now().Add(30 * time.Second)}
}

// The historical first RED used legacy unauthenticated peers. This GREEN
// deliberately uses the existing CA helper because cross-group projection is
// a new authenticated production mutation surface.
func TestAuthenticatedSplitSourceInsertCanonicalCapacityAndVisibilityV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
	defer cancel()
	f := fixedPeerVectorReadyWithSourcePlacementAuthV1(t, ctx, "group-a", raftplacement.PlacementModeCollectionV1, true)
	client, err := DialContext(ctx, "tcp", f.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first := splitInsertRequestV1(f, 1)
	result, err := client.VectorInsertV1(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	token, err := commitlog.DecodeSplitVectorInsertPayloadV1(result.VisibilityToken)
	if err != nil || token.Operation != "clear" || token.SourceIndex == 0 || token.TargetIndex != result.CommitIndex || result.OwnerGroup != "group-b" {
		t.Fatalf("durable receipt/token=%+v response=%+v err=%v", token, result, err)
	}
	retry, err := client.VectorInsertV1(ctx, first)
	if err != nil || !bytes.Equal(retry.VisibilityToken, result.VisibilityToken) || retry.LiveRevision != result.LiveRevision || retry.Counters.Commits != 1 {
		t.Fatalf("completed duplicate=%+v err=%v", retry, err)
	}
	conflict := first
	conflict.Vector = []float32{1, 0}
	conflict.Document = []byte(`{"embedding":[1,0],"kind":"conflicting"}`)
	if _, err := client.VectorInsertV1(ctx, conflict); err == nil {
		t.Fatal("semantic idempotency conflict accepted")
	}
	search := f.SearchRequest(first.Vector, 1)
	search.VisibilityToken = result.VisibilityToken
	if _, err := client.VectorSearchStrictV1(ctx, search); err != nil {
		t.Fatalf("real watermark search refused: %v", err)
	}
	malformed := search
	malformed.VisibilityToken = []byte("not-a-receipt")
	if _, err := client.VectorSearchStrictV1(ctx, malformed); err == nil {
		t.Fatal("malformed session watermark accepted")
	}
	for n := 2; n <= commitlog.SplitVectorInsertMaxAttemptsV1; n++ {
		if _, err := client.VectorInsertV1(ctx, splitInsertRequestV1(f, n)); err != nil {
			t.Fatalf("insert %d: %v", n, err)
		}
	}
	rejected := splitInsertRequestV1(f, commitlog.SplitVectorInsertMaxAttemptsV1+1)
	if _, err := client.VectorInsertV1(ctx, rejected); err == nil {
		t.Fatal("fixed generation completed capacity exceeded")
	}
	// Restart the actual source leader and prove old completed outcome survives
	// the lifetime ceiling, while the refused new canonical ID remains absent.
	var sourceConfig FixedPeerTCPConfigV1
	for i, c := range f.configs {
		if c.NodeID == "ingress" {
			sourceConfig = c
			f.processes[i].stop(t)
		}
	}
	source, err := OpenFixedPeerTCPRuntimeV1(sourceConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		s, e := source.Status(ctx)
		return e == nil && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
	})
	reopened, err := DialContext(ctx, "tcp", f.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	first.Deadline = time.Now().Add(30 * time.Second)
	duplicate, err := reopened.VectorInsertV1(ctx, first)
	if err != nil || !bytes.Equal(duplicate.VisibilityToken, result.VisibilityToken) || duplicate.LiveRevision != result.LiveRevision {
		t.Fatalf("reopened duplicate=%+v err=%v", duplicate, err)
	}
	if doc, err := source.vector.collection.Get(rejected.ID); err != nil || doc != nil {
		t.Fatalf("capacity refusal mutated canonical row=%q err=%v", doc, err)
	}
	if doc, err := source.vector.collection.Get(first.ID); err != nil || !bytes.Equal(doc, first.Document) {
		t.Fatalf("source canonical row=%q err=%v", doc, err)
	}
	// Lose both the catalog and target majorities while source and target
	// leaders remain alive. Warm completed receipts cannot waive fresh quorum.
	for i, c := range f.configs {
		if c.NodeID == "owner-2" || c.NodeID == "owner-3" {
			f.processes[i].stop(t)
		}
	}
	first.Deadline = time.Now().Add(30 * time.Second)
	if denied, err := reopened.VectorInsertV1(ctx, first); err == nil || len(denied.VisibilityToken) != 0 {
		t.Fatalf("completed receipt survived quorum loss: %+v err=%v", denied, err)
	}
	search.Deadline = time.Now().Add(30 * time.Second)
	if denied, err := reopened.VectorSearchStrictV1(ctx, search); err == nil || len(denied.Neighbors) != 0 {
		t.Fatalf("watermark search survived quorum loss: %+v err=%v", denied, err)
	}
	// A target graph receipt is not permission to create canonical rows there.
	for i, c := range f.configs {
		if c.NodeID != "owner-1" {
			continue
		}
		f.processes[i].stop(t)
		db, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(c.DataRoot, "group-b"), CommandWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		col, err := collections.NewCollectionManager(db).OpenCollection("docs")
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		row, e := col.Get(first.ID)
		closeErr := db.Close()
		if e != nil || closeErr != nil || row != nil {
			t.Fatalf("target canonical duplicate=%q err=%v close=%v", row, e, closeErr)
		}
	}
}

type splitInsertRoundTripperV1 func(*http.Request) (*http.Response, error)

func (f splitInsertRoundTripperV1) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// This is deterministic graceful source leader loss/reopen, not SIGKILL or
// power-loss certification. The intercepted real authenticated HTTP request
// can only be constructed after source Raft apply published row plus intent.
func TestAuthenticatedSplitSourceInsertCommitBeforeProjectionReopenV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	f := fixedPeerVectorReadyWithSourcePlacementAuthV1(t, ctx, "group-a", raftplacement.PlacementModeCollectionV1, true)
	var config FixedPeerTCPConfigV1
	for i, c := range f.configs {
		if c.NodeID == "ingress" {
			config = c
			f.processes[i].stop(t)
		}
	}
	source, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	fixedPeerWaitV1(t, ctx, func() bool {
		s, e := source.Status(ctx)
		return e == nil && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
	})
	base := source.client.http.Transport
	baseRead := source.client.readHTTP.Transport
	arrived := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProjection := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseProjection() // release held work even when the old blocking retry fails
	var once sync.Once
	var outbound atomic.Uint64
	// Install only while no source intent/client mutation exists. The owned
	// retry loop's empty-slot path cannot touch either shared HTTP transport.
	source.client.http.Transport = splitInsertRoundTripperV1(func(request *http.Request) (*http.Response, error) {
		outbound.Add(1)
		if request.URL.Path == "/v1/vector-split-project" {
			once.Do(func() { close(arrived) })
			select {
			case <-request.Context().Done():
				return nil, request.Context().Err()
			case <-release:
				return nil, context.Canceled
			}
		}
		return base.RoundTrip(request)
	})
	source.client.readHTTP.Transport = splitInsertRoundTripperV1(func(request *http.Request) (*http.Response, error) {
		outbound.Add(1)
		return baseRead.RoundTrip(request)
	})
	before := outbound.Load()
	for i := 0; i < 3; i++ {
		if err := source.retrySplitVectorPendingV1(ctx); err != nil {
			t.Fatalf("empty retry slot: %v", err)
		}
	}
	if outbound.Load() != before {
		t.Fatal("idle source retry issued authority/control RPC")
	}
	client, err := DialContext(ctx, "tcp", f.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := splitInsertRequestV1(f, 1)
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	insertDone := make(chan error, 1)
	go func() { _, e := client.VectorInsertV1(callCtx, request); insertDone <- e }()
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	identity := commitlog.SplitVectorInsertV1{Collection: config.Vector.Collection.Collection, Index: config.Vector.Manifest.IndexName, Generation: config.Vector.Identity.Generation,
		SourceGroup: "group-a", TargetGroup: "group-b", CatalogEpoch: config.Vector.Identity.Index.CatalogEpoch, CatalogDigest: config.Vector.Identity.Index.CatalogDigest}
	pending, err := source.vector.collection.PendingVectorPartitionSplitInsertV1(identity)
	if err != nil || pending == nil || pending.SourceTerm == 0 || pending.SourceIndex == 0 || !bytes.Equal(pending.Document, request.Document) {
		t.Fatalf("source intent not durably published before outbound projection: %+v err=%v", pending, err)
	}
	if row, err := source.vector.collection.Get(request.ID); err != nil || !bytes.Equal(row, request.Document) {
		t.Fatalf("source row before projection=%q err=%v", row, err)
	}
	// The foreground producer owns mutationMu until the intercepted projection
	// returns. A background tick must skip that busy lock, not wait behind it.
	if source.vector.mutationMu.TryLock() {
		source.vector.mutationMu.Unlock()
		t.Fatal("held foreground projection did not own mutation lock")
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, 5*time.Second)
	defer cancelProbe()
	retryDone := make(chan error, 1)
	go func() { retryDone <- source.retrySplitVectorPendingV1(probeCtx) }()
	select {
	case err := <-retryDone:
		if err != nil {
			t.Fatalf("busy background retry refused instead of skipping: %v", err)
		}
	case <-probeCtx.Done():
		releaseProjection()
		cancelCall()
		// Release the foreground owner before joining the old blocking retry.
		// This keeps its regression failure from stranding a goroutine at Cleanup.
		<-retryDone
		t.Fatal("background retry waited behind foreground mutation lock")
	}
	select {
	case err := <-insertDone:
		t.Fatalf("background probe released held foreground work: %v", err)
	default:
	}
	// A real target-node certificate cannot act as the source-group producer.
	projection := *pending
	projection.Operation, projection.Document = "project", nil
	rawProjection, err := commitlog.EncodeSplitVectorInsertPayloadV1(projection)
	if err != nil {
		t.Fatal(err)
	}
	var targetConfig FixedPeerTCPConfigV1
	for _, c := range f.configs {
		if c.NodeID == "owner-1" {
			targetConfig = c
		}
	}
	wrongCaller, err := NewFixedPeerTCPClientV1(targetConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer wrongCaller.Close()
	beforeTarget, err := wrongCaller.Status(ctx, "owner-1")
	if err != nil || len(beforeTarget.Groups) != 1 {
		t.Fatalf("target pre-refusal status=%+v err=%v", beforeTarget, err)
	}
	denied, err := wrongCaller.call(ctx, "owner-1", "vector-split-project", fixedPeerRequestV1{Entry: rawProjection}, true)
	if !errors.Is(err, errPeerAuthenticationV1) || denied.VectorInsert != nil || denied.ReadProof != nil {
		t.Fatalf("wrong authenticated producer accepted: %+v err=%v", denied, err)
	}
	afterTarget, err := wrongCaller.Status(ctx, "owner-1")
	if err != nil || len(afterTarget.Groups) != 1 || afterTarget.Groups[0].Applied.Index != beforeTarget.Groups[0].Applied.Index {
		t.Fatalf("wrong producer advanced target: before=%+v after=%+v err=%v", beforeTarget, afterTarget, err)
	}
	closing := make(chan error, 1)
	go func() { closing <- source.Close() }()
	fixedPeerWaitV1(t, ctx, source.draining.Load)
	releaseProjection() // the intercepted real transport returns cancellation; no forged target response
	cancelCall()
	select {
	case err := <-closing:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close failed to drain intercepted/canceled projection")
	}
	select {
	case err := <-insertDone:
		if err == nil {
			t.Fatal("lost projection acknowledgement returned success")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Close never changes the durable intent. Successful native reopen must
	// own one retry worker and finish without a new client mutation or ANN query.
	reopened, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		p, e := reopened.vector.collection.PendingVectorPartitionSplitInsertV1(identity)
		return e == nil && p == nil
	})
	complete := *pending
	complete.Operation = "project"
	complete.Document = nil
	p, receipt, known, err := reopened.vector.collection.VectorPartitionSplitInsertStateV1(complete)
	if err != nil || p != nil || !known || receipt.SourceTerm != pending.SourceTerm || receipt.SourceIndex != pending.SourceIndex {
		t.Fatalf("durable retry/retirement state pending=%+v receipt=%+v known=%v err=%v", p, receipt, known, err)
	}
	complete.Operation = "clear"
	complete.TargetTerm = receipt.TargetTerm
	complete.TargetIndex = receipt.TargetIndex
	complete.LiveRevision = receipt.LiveRevision
	proof, err := reopened.callSplitVectorGroupV1(ctx, "group-b", "vector-split-receipt", complete)
	if err != nil || proof.VectorInsert == nil || proof.ReadProof == nil {
		t.Fatalf("independent target receipt fence=%+v err=%v", proof, err)
	}
	if err := (raftcluster.ReadIndexBarrier{GroupID: "group-b"}).Check(*proof.ReadProof); err != nil {
		t.Fatal(err)
	}
	token, err := commitlog.DecodeSplitVectorInsertPayloadV1(proof.VectorInsert.VisibilityToken)
	if err != nil || token.SourceIndex != pending.SourceIndex || token.TargetIndex != receipt.TargetIndex {
		t.Fatalf("receipt token=%+v err=%v", token, err)
	}
	// An explicit retry returns exactly that durable outcome after native reopen.
	retryClient, err := DialContext(ctx, "tcp", f.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer retryClient.Close()
	request.Deadline = time.Now().Add(30 * time.Second)
	result, err := retryClient.VectorInsertV1(ctx, request)
	if err != nil || !bytes.Equal(result.VisibilityToken, proof.VectorInsert.VisibilityToken) {
		t.Fatalf("reopened exact duplicate=%+v err=%v", result, err)
	}
	if row, e := reopened.vector.collection.Get(request.ID); e != nil || !bytes.Equal(row, request.Document) {
		t.Fatalf("reopened canonical row=%q err=%v", row, e)
	}
	search := f.SearchRequest(request.Vector, 1)
	search.VisibilityToken = result.VisibilityToken
	if _, err := retryClient.VectorSearchStrictV1(ctx, search); err != nil {
		t.Fatal(err)
	}
}
