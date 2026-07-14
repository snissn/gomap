package collections

import (
	"errors"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type columnVectorStablePrepareResult struct {
	closure *ColumnVectorGraphStablePreparedClosure
	err     error
}

func TestPrepareVectorIndexStableClosureCloseAdmission(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0}},
	}

	t.Run("close-wins-after-snapshot", func(t *testing.T) {
		_, d, collection, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
		registry := d.StableResourceIdentityPinRegistry()
		// Remove collection-manager callbacks from the race so Close reaches the
		// DB teardown gate while preparation is paused after releasing its snapshot.
		if err := d.RunCloseHooks(); err != nil {
			_ = d.Close()
			t.Fatalf("RunCloseHooks: %v", err)
		}

		paused := make(chan struct{})
		resume := make(chan struct{})
		var pauseOnce sync.Once
		restore := setColumnVectorGraphStableBeforeCaptureAdmissionTestHook(func() {
			pauseOnce.Do(func() { close(paused) })
			<-resume
		})
		defer restore()

		prepared := make(chan columnVectorStablePrepareResult, 1)
		go func() {
			closure, err := collection.PrepareVectorIndexStableClosure(def.Name)
			prepared <- columnVectorStablePrepareResult{closure: closure, err: err}
		}()
		awaitColumnVectorStablePrepareSignal(t, paused, "pre-admission pause")

		closed := make(chan error, 1)
		go func() { closed <- d.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				close(resume)
				t.Fatalf("Close: %v", err)
			}
		case <-time.After(10 * time.Second):
			close(resume)
			t.Fatal("Close did not complete while preparation was paused before admission")
		}
		close(resume)

		result := awaitColumnVectorStablePrepareResult(t, prepared)
		if result.closure != nil {
			result.closure.Release()
			t.Fatal("pre-admission Close race returned a closure")
		}
		if !errors.Is(result.err, backenddb.ErrClosed) {
			t.Fatalf("pre-admission Close race error=%v want ErrClosed", result.err)
		}
		assertStableColumnVectorGraphRegistryZero(t, registry, "close-wins after snapshot")
	})

	t.Run("admitted-prepare-drains", func(t *testing.T) {
		_, d, collection, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
		registry := d.StableResourceIdentityPinRegistry()
		if err := d.RunCloseHooks(); err != nil {
			_ = d.Close()
			t.Fatalf("RunCloseHooks: %v", err)
		}

		preparedAuthority := make(chan struct{})
		resume := make(chan struct{})
		var pauseOnce sync.Once
		restore := setColumnVectorGraphStableAuthorityTestHook(func(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
			if resources == nil || len(assets) == 0 {
				return errors.New("stable vector preparation reached hook without authority")
			}
			pauseOnce.Do(func() { close(preparedAuthority) })
			<-resume
			return nil
		})
		defer restore()

		prepared := make(chan columnVectorStablePrepareResult, 1)
		go func() {
			closure, err := collection.PrepareVectorIndexStableClosure(def.Name)
			prepared <- columnVectorStablePrepareResult{closure: closure, err: err}
		}()
		awaitColumnVectorStablePrepareSignal(t, preparedAuthority, "stable authority capture")
		stats := registry.Stats()
		if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks == 0 {
			close(resume)
			t.Fatalf("admitted stable capture stats=%+v want live pins, identities, and namespace proofs", stats)
		}

		closed := make(chan error, 1)
		go func() { closed <- d.Close() }()
		deadline := time.Now().Add(10 * time.Second)
		for !d.IsClosing() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !d.IsClosing() {
			close(resume)
			t.Fatal("Close did not reach teardown admission while capture lease was held")
		}
		select {
		case err := <-closed:
			close(resume)
			t.Fatalf("Close returned before admitted stable capture drained: %v", err)
		default:
		}
		close(resume)

		result := awaitColumnVectorStablePrepareResult(t, prepared)
		if result.err != nil || result.closure == nil {
			t.Fatalf("admitted stable preparation closure=%v err=%v", result.closure, result.err)
		}
		select {
		case err := <-closed:
			if err != nil {
				result.closure.Release()
				t.Fatalf("Close after stable capture drain: %v", err)
			}
		case <-time.After(10 * time.Second):
			result.closure.Release()
			t.Fatal("Close did not complete after admitted stable capture drained")
		}
		postClose := registry.Stats()
		if postClose.ActivePins == 0 || postClose.ActiveIdentities == 0 || postClose.ActiveStableNamespaceLinks != 0 {
			result.closure.Release()
			t.Fatalf("post-Close retained closure stats=%+v want retained identities but cleared DB-lifetime namespace proofs", postClose)
		}
		result.closure.Release()
		assertStableColumnVectorGraphRegistryZero(t, registry, "admitted closure release after DB close")

		lateClosure, err := collection.PrepareVectorIndexStableClosure(def.Name)
		if lateClosure != nil {
			lateClosure.Release()
			t.Fatal("post-Close stable preparation returned a closure")
		}
		if !errors.Is(err, backenddb.ErrClosed) {
			t.Fatalf("post-Close stable preparation error=%v want ErrClosed", err)
		}
		assertStableColumnVectorGraphRegistryZero(t, registry, "post-Close rejected capture")
	})
}

func awaitColumnVectorStablePrepareSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitColumnVectorStablePrepareResult(t *testing.T, result <-chan columnVectorStablePrepareResult) columnVectorStablePrepareResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for stable preparation result")
		return columnVectorStablePrepareResult{}
	}
}
