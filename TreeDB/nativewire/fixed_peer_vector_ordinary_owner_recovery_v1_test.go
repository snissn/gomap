package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestImmutableOwnerOrdinaryServingRecoversCurrentFSMDBV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateQualificationRecoveryV1(t, true, true, false, true)
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

	// Make a new construction necessary, then hold the actual root barrier. Close
	// cancels the real blocked accessor and waits for cleanup without holding it.
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
	canceledInstall := vector.topology != nil || vector.source != nil || vector.initDone != nil
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
