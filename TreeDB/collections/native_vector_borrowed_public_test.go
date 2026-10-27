package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// The buffered public route must search and materialize the same native cut.
// Freeze this fixture before the adapter; old source refuses c.db == nil.
func TestNativeVectorBorrowedPublicLifecycle(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			owner, manager, c := openNativeTextPublic(t, dir, profile, true)
			if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`), []byte(`{"tenant":"b","content":"alpha","embedding":[0,1]}`)}); err != nil {
				t.Fatal(err)
			}
			opts := VectorIndexSearchOptions{Context: context.Background(), IndexName: "embedding", Query: []float32{1, 0}, TopK: 2, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "a"}}
			var buffer VectorIndexSearchBuffer
			defer buffer.Reset()
			phase := 0
			check := func() {
				phase++
				t.Logf("lifecycle phase %d", phase)
				response, view, err := c.SearchVectorIndexWithBufferReadView(opts, &buffer)
				if err != nil {
					t.Fatal(err)
				}
				if response.Strategy != VectorIndexStrategyNativeRuntime || response.Stats.SearchRouteNativeRuntime != 1 || response.Stats.DocumentsFetched != 0 || len(response.Results) != 1 || string(response.Results[0].ID) != "a" {
					t.Fatalf("route/results: %+v", response)
				}
				if !response.Diagnostics().NoDocumentGuardrailsOK || response.Diagnostics().LiveANN.ExactFallbacks != 0 {
					t.Fatalf("fallback: %+v", response.Diagnostics())
				}
				fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
				if err != nil || len(fetched.Results) != 1 || bytes.Contains(fetched.Results[0].Document, []byte("embedding")) {
					t.Fatalf("materialization: %+v %v", fetched, err)
				}
				id := bytes.Clone(response.Results[0].ID)
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(response.Results[0].ID, id) {
					t.Fatal("view Close changed buffer-owned IDs")
				}
				buffer.Reset()
				rangeFilter := HybridScalarFilter{IndexName: "tenant", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "a", Inclusive: true}, Upper: IndexRangeBound{Value: "b", Inclusive: true}}}
				for _, filtered := range []struct {
					filter HybridScalarFilter
					want   int
				}{{rangeFilter, 2}, {HybridScalarFilter{And: []HybridScalarFilter{rangeFilter, {IndexName: "tenant", Value: "a"}}}, 1}} {
					rangeOpts := opts
					rangeOpts.DeclaredScalarFilter = &filtered.filter
					response, err := c.SearchVectorIndexWithBuffer(rangeOpts, &buffer)
					if err != nil || len(response.Results) != filtered.want || response.Stats.DocumentsFetched != 0 || response.Stats.SearchRouteNativeRuntime != 1 {
						t.Fatalf("range/conjunction: %+v %v", response, err)
					}
					buffer.Reset()
				}
			}
			check()
			hybrid := nativeHybridRequest()
			hybrid.Vector = &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}, CandidateLimit: 2}
			response := requireNativeHybridResult(t, c, hybrid, "a")
			if response.Stats.VectorCandidatesReturned != 1 {
				t.Fatalf("vector work: %+v", response.Stats)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check()
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			owner, manager, c = openNativeTextPublic(t, dir, profile, false)
			check()
			requireNativeHybridResult(t, c, hybrid, "a")
		})
	}
}

// A missing scalar column is harmless only when its plane has no rows.
func TestNativeVectorBorrowedScalarEmptyPlane(t *testing.T) {
	plan := &nativeScalarFilterExecution{clauses: []nativeScalarClause{{indexName: "tenant"}}}
	if _, err := bindNativeScalarMatcher(plan, nil, 0, "base"); err != nil {
		t.Fatal(err)
	}
	for _, columns := range []map[string]vectorIndexScalarColumn{nil, {"tenant": newVectorIndexScalarColumn(IndexValueString)}} {
		if _, err := bindNativeScalarMatcher(plan, columns, 1, "base"); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
			t.Fatalf("nonempty invalid plane accepted: %v", err)
		}
	}
}

func TestNativeVectorBorrowedPublicReceipt(t *testing.T) {
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, []byte("a"))
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	defer buffer.Reset()
	response, view, err := c.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil || len(response.Results) != 1 {
		t.Fatalf("warm: %+v %v", response, err)
	}
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	buffer.Reset()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	before := budget.Stats()
	view, err = c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := newHybridNativeScratch(view)
	if err != nil {
		t.Fatal(err)
	}
	view.catalog, err = newNativeTextCatalog(view.nativeRead, scratch.admitFn)
	if err != nil {
		t.Fatal(err)
	}
	during := budget.Stats()
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	after := budget.Stats()
	if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || during.ReservedBytes <= before.ReservedBytes {
		t.Fatalf("view ownership before=%+v during=%+v after=%+v", before, during, after)
	}
	viewBytes := during.ReservedBytes - before.ReservedBytes
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < viewBytes {
		t.Fatal("setup exceeds native view boundary")
	}
	if err = blocker.Resize(available - viewBytes); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		before = budget.Stats()
		response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "buffered vector query workspace") || len(response.Results) != 0 || buffer.nativeLease != nil {
			t.Fatalf("attempt %d: response=%+v err=%v", attempt, response, err)
		}
		after = budget.Stats()
		if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
			t.Fatalf("refused query retained admission: before=%+v after=%+v", before, after)
		}
	}
	blocker.Close()
	before = budget.Stats()
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || len(response.Results) != 1 || buffer.nativeLease == nil {
		t.Fatalf("after release: %+v %v", response, err)
	}
	during = budget.Stats()
	if during.ReservedBytes <= before.ReservedBytes || during.ExternalLeases <= before.ExternalLeases {
		t.Fatal("returned native buffer has no retained receipt")
	}
	buffer.Reset()
	after = budget.Stats()
	if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || buffer.results != nil || buffer.nativeSearchScratch.visitedEpochs != nil {
		t.Fatalf("Reset retained native backing: before=%+v after=%+v", before, after)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts.Context = ctx
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if !errors.Is(err, context.Canceled) || len(response.Results) != 0 {
		t.Fatalf("cancel: %+v %v", response, err)
	}
	after = budget.Stats()
	if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
		t.Fatalf("cancel retained admission: before=%+v after=%+v", before, after)
	}
}

func TestNativeVectorBorrowedPublicHeldView(t *testing.T) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	opts := VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	defer buffer.Reset()
	response, view, err := c.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"tenant":"a","content":"delta","embedding":[1,0]}`), true, nil
	}); err != nil {
		t.Fatal(err)
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
	if err != nil || len(fetched.Results) != 1 || !strings.Contains(string(fetched.Results[0].Document), "alpha") {
		t.Fatalf("held fetch: %+v %v", fetched, err)
	}
	if fresh, err := c.OpenCollectionReadViewForVectorIndexSearch(response); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		if fresh != nil {
			fresh.Close()
		}
		t.Fatalf("changed publication accepted: %v", err)
	}
	document := bytes.Clone(fetched.Results[0].Document)
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	buffer.Reset()
	if !bytes.Equal(document, fetched.Results[0].Document) {
		t.Fatal("closing/reset changed independent document output")
	}
	response, view, err = c.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	fetched, err = view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 1 || !strings.Contains(string(fetched.Results[0].Document), "delta") {
		t.Fatalf("current fetch: %+v %v", fetched, err)
	}
}

func TestNativeVectorBorrowedHybridFence(t *testing.T) {
	for _, change := range []string{"checkpoint", "unrelated", "document", "cancel"} {
		t.Run(change, func(t *testing.T) {
			owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := nativeHybridRequest()
			opts.Context = ctx
			opts.Vector = &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}, CandidateLimit: 2}
			setNativeHybridHook(t, func(got *Collection, _ *CollectionReadView) {
				if got != c {
					return
				}
				var err error
				switch change {
				case "checkpoint":
					err = owner.Checkpoint()
				case "unrelated":
					_, err = manager.CreateCollection(&CollectionMeta{Name: "other"})
				case "document":
					_, err = c.Insert([]byte("b"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`))
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			response, err := c.SearchHybrid(opts)
			if change == "checkpoint" {
				if err != nil || len(response.Results) != 1 {
					t.Fatalf("checkpoint: %+v %v", response, err)
				}
				return
			}
			if change == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrHybridSearchStaleIndex) {
				t.Fatalf("logical fence: %v", err)
			}
			if len(response.Results) != 0 || len(response.Candidates) != 0 || response.Stats.FailClosed != 1 {
				t.Fatalf("not fail closed: %+v", response)
			}
		})
	}
}

// The second public query must use the runtime installed while it waited for
// the existing mutation gate, rather than interpreting an unrun loader's zero
// status as a failed load.
func TestNativeVectorBorrowedConcurrentColdLoad(t *testing.T) {
	dir := t.TempDir()
	owner, manager, c := openNativeTextPublic(t, dir, treedb.ProfileNoWALFast, true)
	if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, c = openNativeTextPublic(t, dir, treedb.ProfileNoWALFast, false)
	if c.registeredVectorIndex("embedding") != nil {
		t.Fatal("fixture is not cold")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var installOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	restore := setNativeVectorIndexBeforeInstallHookForTest(func(string) { installOnce.Do(func() { close(entered); <-release }) })
	defer restore()
	done := make(chan error, 2)
	query := func() {
		var buffer VectorIndexSearchBuffer
		defer buffer.Reset()
		response, err := c.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "a"}}, &buffer)
		if err == nil && (len(response.Results) != 1 || string(response.Results[0].ID) != "a") {
			err = fmt.Errorf("cold results: %+v", response)
		}
		done <- err
	}
	go query()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first cold loader did not reach install")
	}
	go query()
	// The established install hook holds the first loader before registration.
	// Observe the sibling actually parked at the mutation gate, with no sleeps
	// or assumption about scheduler timing, before allowing publication.
	deadline := time.After(5 * time.Second)
	stacks := make([]byte, 256<<10)
	for {
		n := runtime.Stack(stacks, true)
		waiting := false
		for _, stack := range strings.Split(string(stacks[:n]), "\n\n") {
			if strings.Contains(stack, "lockVectorIndexMutation") && strings.Contains(stack, "searchNativeHNSWAtReadView") {
				waiting = true
				break
			}
		}
		if waiting {
			break
		}
		select {
		case <-deadline:
			unblock()
			t.Fatal("sibling did not wait at cold-load mutation gate")
		default:
			runtime.Gosched()
		}
	}
	unblock()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent cold query did not finish")
		}
	}
}
