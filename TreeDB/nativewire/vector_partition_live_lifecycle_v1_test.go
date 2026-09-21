package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// Component evidence only: the real standalone live binding and TCP codecs,
// with the existing fixture's test read proof, not replicated live serving.
type liveLifecycleIdentityV1 struct{ revision, coverage uint64 }
type liveLifecycleTruthV1 struct {
	scores map[string]float32
	top    map[string]bool
}
type liveLifecycleAttemptV1 struct {
	id                int
	query             int
	started, finished time.Time
	response          VectorPartitionCoordinatorResponseV1
	err               error
}
type liveLifecycleWriteV1 struct {
	started, finished time.Time
	identity          liveLifecycleIdentityV1
	err               error
}

// Optional raw component receipts, not a new campaign format. CreateTemp never
// overwrites an earlier failure or repetition. The enclosing go-test command,
// source tree, environment and exit status must be retained by the caller.
func retainLiveLifecycleV1(t *testing.T, phase string, manifest collections.VectorPartitionManifestV1, queries [][]float32, attempts []liveLifecycleAttemptV1, writes []liveLifecycleWriteV1, truth map[liveLifecycleIdentityV1][]liveLifecycleTruthV1) {
	t.Helper()
	dir := os.Getenv("GOMAP_SELECTED_LIVE_RECEIPTS")
	if dir == "" {
		return
	}
	type attempt struct {
		ID, Query         int
		Started, Finished time.Time
		Outcome           string
		Response          VectorPartitionCoordinatorResponseV1
	}
	type write struct {
		Started, Finished  time.Time
		Revision, Coverage uint64
		Outcome            string
	}
	type truthCell struct {
		Revision, Coverage uint64
		Query              int
		Scores             map[string]float32
		Top                map[string]bool
	}
	receipt := struct {
		Scope, Phase, Fixture string
		Manifest              collections.VectorPartitionManifestV1
		Queries               [][]float32
		Attempts              []attempt
		Writes                []write
		Truth                 []truthCell
	}{Scope: "standalone_components_not_qualification", Phase: phase, Fixture: "procedural_512x768_v1", Manifest: manifest, Queries: queries}
	for _, a := range attempts {
		outcome := "response_returned"
		if a.started.IsZero() {
			outcome = "not_dispatched"
		} else if a.err != nil {
			outcome = a.err.Error()
		}
		receipt.Attempts = append(receipt.Attempts, attempt{a.id, a.query, a.started, a.finished, outcome, a.response})
	}
	for _, w := range writes {
		outcome := "ack_synced"
		if w.started.IsZero() {
			outcome = "not_dispatched"
		} else if w.err != nil {
			outcome = w.err.Error()
		}
		receipt.Writes = append(receipt.Writes, write{w.started, w.finished, w.identity.revision, w.identity.coverage, outcome})
	}
	for key, cells := range truth {
		for q, cell := range cells {
			receipt.Truth = append(receipt.Truth, truthCell{key.revision, key.coverage, q, cell.scores, cell.top})
		}
	}
	sort.Slice(receipt.Truth, func(i, j int) bool {
		a, b := receipt.Truth[i], receipt.Truth[j]
		if a.Revision != b.Revision {
			return a.Revision < b.Revision
		}
		if a.Coverage != b.Coverage {
			return a.Coverage < b.Coverage
		}
		return a.Query < b.Query
	})
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(dir, phase+"-*.json")
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(raw)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	t.Logf("raw_receipt=%s bytes=%d sha256=%x", filepath.Clean(file.Name()), len(raw), sha256.Sum256(raw))
}

func liveLifecycleTruthForV1(t *testing.T, vectors map[string][]float32, queries [][]float32, k int) []liveLifecycleTruthV1 {
	t.Helper()
	truth := make([]liveLifecycleTruthV1, len(queries))
	for q, query := range queries {
		scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
		if err != nil {
			t.Fatal(err)
		}
		cell := liveLifecycleTruthV1{scores: make(map[string]float32, len(vectors)), top: make(map[string]bool, k)}
		ids := make([]string, 0, len(vectors))
		for id, vector := range vectors {
			score, err := scorer.ScoreV1(vector)
			if err != nil {
				t.Fatal(err)
			}
			cell.scores[id] = score
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			if cell.scores[ids[i]] != cell.scores[ids[j]] {
				return cell.scores[ids[i]] > cell.scores[ids[j]]
			}
			return ids[i] < ids[j]
		})
		for _, id := range ids[:k] {
			cell.top[id] = true
		}
		truth[q] = cell
	}
	return truth
}

// Failed/absent attempts never disappear from the denominator. Exact truth is
// computed outside search windows and joined to BOTH revision and coverage.
func validateLiveLifecycleAttemptsV1(attempts []liveLifecycleAttemptV1, expected, k int, generation uint64, truth map[liveLifecycleIdentityV1][]liveLifecycleTruthV1) (int, error) {
	if len(attempts) != expected || expected < 1 || k < 1 {
		return 0, errors.New("incomplete attempt population")
	}
	seen := make(map[int]bool, expected)
	hits := 0
	for _, a := range attempts {
		if a.id < 0 || a.id >= expected || seen[a.id] || a.started.IsZero() || !a.finished.After(a.started) {
			return hits, errors.New("invalid attempt identity or interval")
		}
		seen[a.id] = true
		if a.err != nil {
			return hits, fmt.Errorf("attempt %d failed: %w", a.id, a.err)
		}
		r := a.response
		cells, ok := truth[liveLifecycleIdentityV1{r.LiveRevision, r.LiveCoverage}]
		if !ok || a.query < 0 || a.query >= len(cells) || r.PartitionGeneration != generation {
			return hits, errors.New("unknown searched revision/coverage/generation")
		}
		if len(r.Neighbors) != k || r.Counters.ExactScanPartitions != 0 || r.Counters.RequestPathFullRebuilds != 0 || r.Counters.Failures != 0 ||
			r.Counters.SelectedDomains != 1 || r.Counters.LiveDomainsSearched != 1 || r.Counters.SelectedPacks < 1 || r.Counters.SelectedPacks > 2 ||
			r.Counters.SelectedPartitions != r.Counters.SelectedPacks || r.Counters.HNSWServedPartitions != r.Counters.SelectedPacks {
			return hits, errors.New("incomplete native route or result")
		}
		// This fixture deliberately gives domain 0 two packs in two groups and
		// domain 1 one pack. A vague 1..2 pack count cannot prove full routing.
		wantPacks := []uint32{0, 1}
		if a.query == 1 {
			wantPacks = []uint32{2}
		}
		if !slices.Equal(r.ProbedDomains, []uint32{uint32(a.query)}) || !slices.Equal(r.ProbedPacks, wantPacks) ||
			!slices.Equal(r.ProbedPartitions, wantPacks) || r.Counters.SelectedPacks != uint64(len(wantPacks)) {
			return hits, errors.New("missing, duplicated, or misassigned pack")
		}
		cell := cells[a.query]
		ids := make(map[string]bool, k)
		for i, n := range r.Neighbors {
			score, present := cell.scores[n.ID]
			if !present || ids[n.ID] || math.Float32bits(score) != math.Float32bits(n.Score) {
				return hits, errors.New("stale, duplicate, or noncanonical result")
			}
			if i > 0 && (r.Neighbors[i-1].Score < n.Score || r.Neighbors[i-1].Score == n.Score && r.Neighbors[i-1].ID > n.ID) {
				return hits, errors.New("noncanonical ordering")
			}
			ids[n.ID] = true
			if cell.top[n.ID] {
				hits++
			}
		}
	}
	if hits*100 < expected*k*95 {
		return hits, fmt.Errorf("recall below 95%%: %d/%d", hits, expected*k)
	}
	return hits, nil
}

func validateLiveLifecycleFreshnessV1(attempts []liveLifecycleAttemptV1, current liveLifecycleIdentityV1, writes []liveLifecycleWriteV1) error {
	for _, a := range attempts {
		key := liveLifecycleIdentityV1{a.response.LiveRevision, a.response.LiveCoverage}
		if len(writes) == 0 {
			if key != current {
				return errors.New("quiescent phase served an earlier live identity")
			}
			continue
		}
		minimum := current
		for _, w := range writes {
			if w.err == nil && !w.finished.After(a.started) {
				minimum = w.identity
			}
		}
		if key.revision < minimum.revision || key.coverage < minimum.coverage {
			return errors.New("query missed a previously acknowledged write")
		}
	}
	return nil
}

func liveLifecycleStatusV1(collection *collections.Collection, manifest collections.VectorPartitionManifestV1) (collections.VectorIndexPartitionLiveStatusV1, error) {
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		return collections.VectorIndexPartitionLiveStatusV1{}, err
	}
	defer pin.Release()
	return pin.StatusV1(), nil
}

func liveLifecycleNativeClientV1(t *testing.T, database *backenddb.DB) (*Client, func()) {
	t.Helper()
	server := NewServer(ServerOptions{Collections: collections.NewCollectionManager(database), Backend: database})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), listener) }()
	client, err := DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	var once sync.Once
	closeClient := func() { once.Do(func() { _ = client.Close(); _ = server.Close(); <-done }) }
	t.Cleanup(closeClient)
	return client, closeClient
}

func liveLifecycleTCPReaderV1(t *testing.T, fixture vectorPartitionLiveProductionFixtureV1) (*VectorPartitionCoordinatorV1, func()) {
	t.Helper()
	services, sources := newVectorPartitionLiveProductionServicesV1(t, fixture)
	endpoints := make(map[raftcluster.GroupID]string, len(services))
	for group, service := range services {
		endpoints[group] = newVectorPartitionShardSearchTCPListenerV1(t, service).Addr().String()
	}
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewVectorPartitionCoordinatorForTopologyV1(vectorPartitionLiveCoordinatorTopologyV1(fixture), CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: fixture.collection}, dispatcher, VectorPartitionCoordinatorLimitsV1{})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	closeReader := func() {
		once.Do(func() {
			_ = coordinator.Close()
			_ = dispatcher.Close()
			for _, source := range sources {
				_ = source.Close()
			}
		})
	}
	t.Cleanup(closeReader)
	return coordinator, closeReader
}

func liveLifecycleReplaceV1(ctx context.Context, client *Client, vector []float32, marker int, ack AckPolicy) error {
	doc, err := json.Marshal(map[string]any{"embedding": vector, "marker": marker})
	if err != nil {
		return err
	}
	matched, modified, err := client.ReplaceBatch(ctx, "docs", collections.DocumentFormatJSON, [][]byte{[]byte("a")}, [][]byte{doc}, ack)
	if err == nil && (matched != 1 || modified != 1) {
		return fmt.Errorf("replace matched=%d modified=%d", matched, modified)
	}
	return err
}

func TestVectorPartitionLiveSelectedLifecycleV1(t *testing.T) {
	const dimensions, rows, topK, workers, perWorker, mutations = 768, 512, 10, 4, 1024, 32
	queries := [][]float32{make([]float32, dimensions), make([]float32, dimensions)}
	queries[0][0], queries[1][1] = 1, 1
	if dir := os.Getenv("GOMAP_SELECTED_LIVE_CRASH_DIR"); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_SELECTED_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		col, err := collections.NewCollectionManager(db).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := col.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := col.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatal(err)
		}
		client, _ := liveLifecycleNativeClientV1(t, db)
		if err := liveLifecycleReplaceV1(t.Context(), client, queries[0], 999, AckSynced); err != nil {
			t.Fatal(err)
		}
		// AckSynced is checkpoint-backed. No additional checkpoint or clean
		// DB/server close follows its acknowledgment; this is not a WAL-only test.
		os.Exit(23)
	}

	documents := make([]vectorPartitionLiveDocumentV1, rows)
	vectors := make(map[string][]float32, rows)
	for i := range documents {
		v := make([]float32, dimensions)
		domain := i % 2
		v[domain] = 1
		for d := 2; d < dimensions; d++ {
			v[d] = float32((i*37+d*13)%101-50) * .0001
		}
		home := uint32(2)
		if domain == 0 {
			home = uint32((i / 2) % 2)
		}
		id := fmt.Sprintf("doc-%04d", i)
		if i == 0 {
			id = "a"
		}
		documents[i] = vectorPartitionLiveDocumentV1{id: id, vector: v, home: home, overlap: i == 0}
		vectors[id] = v
	}
	fixture := newVectorPartitionLiveNativewireDocumentsV1(t, documents)
	t.Cleanup(func() {
		if fixture.database != nil {
			_ = fixture.database.Close()
		}
	})
	for _, asset := range fixture.manifest.Assets {
		if asset.GraphVariant != string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1) {
			t.Fatalf("wrong selected base graph: %s", asset.GraphVariant)
		}
	}
	client, closeClient := liveLifecycleNativeClientV1(t, fixture.database)
	coordinator, closeReader := liveLifecycleTCPReaderV1(t, fixture)
	request := VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, Database: "default", Catalog: "default", Collection: "docs", IndexName: fixture.definition.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(fixture.definition), Metric: VectorPartitionShardSearchMetricCosineV1,
		RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterScoreBudget: 256, PartitionProbes: 1,
		Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
		TopK: topK, EfSearch: 96, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 3 * topK,
	}
	search := func(id, query int) liveLifecycleAttemptV1 {
		r := request
		r.RequestID, r.CancellationID = fmt.Sprintf("lifecycle-%d", id), fmt.Sprintf("cancel-%d", id)
		r.Query = queries[query]
		a := liveLifecycleAttemptV1{id: id, query: query, started: time.Now()}
		a.response, a.err = coordinator.Search(t.Context(), r)
		a.finished = time.Now()
		return a
	}
	truth := make(map[liveLifecycleIdentityV1][]liveLifecycleTruthV1)
	var current liveLifecycleIdentityV1
	remember := func(cells []liveLifecycleTruthV1) collections.VectorIndexPartitionLiveStatusV1 {
		s, err := liveLifecycleStatusV1(fixture.collection, fixture.manifest)
		if err != nil {
			t.Fatal(err)
		}
		key := liveLifecycleIdentityV1{s.Revision, s.Coverage}
		if previous, ok := truth[key]; ok && !reflect.DeepEqual(previous, cells) {
			t.Fatal("different truth for the same live identity")
		}
		truth[key] = cells
		current = key
		return s
	}
	check := func(phase string, attempts []liveLifecycleAttemptV1, writes ...liveLifecycleWriteV1) {
		t.Helper()
		retainLiveLifecycleV1(t, phase, fixture.manifest, queries, attempts, writes, truth)
		for i, w := range writes {
			if w.err != nil || !w.finished.After(w.started) {
				t.Fatalf("%s write %d: %+v", phase, i, w)
			}
		}
		hits, err := validateLiveLifecycleAttemptsV1(attempts, len(attempts), topK, fixture.manifest.Generation, truth)
		var elapsed time.Duration
		for _, a := range attempts {
			elapsed += a.finished.Sub(a.started)
		}
		t.Logf("phase=%s attempts=%d truth_hits=%d truth_slots=%d sum_query_ns=%d", phase, len(attempts), hits, len(attempts)*topK, elapsed)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if err := validateLiveLifecycleFreshnessV1(attempts, current, writes); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
	}
	// The first search installs the normal standalone binding; never create a
	// benchmark-only delta or substitute a direct local-search result.
	initial := []liveLifecycleAttemptV1{search(0, 0), search(1, 1)}
	remember(liveLifecycleTruthForV1(t, vectors, queries, topK))
	check("initial", initial)

	insert, _ := json.Marshal(map[string]any{"embedding": queries[0]})
	if _, err := client.InsertBatch(t.Context(), "docs", collections.DocumentFormatJSON, [][]byte{[]byte("live")}, [][]byte{insert}, AckSynced); err != nil {
		t.Fatal(err)
	}
	vectors["live"] = queries[0]
	if deleted, err := client.DeleteBatch(t.Context(), "docs", [][]byte{[]byte("doc-0001")}, AckSynced); err != nil || deleted != 1 {
		t.Fatalf("delete=%d err=%v", deleted, err)
	}
	delete(vectors, "doc-0001")
	if err := liveLifecycleReplaceV1(t.Context(), client, queries[1], 1, AckSynced); err != nil {
		t.Fatal(err)
	}
	vectors["a"] = queries[1]
	beforeMeta := remember(liveLifecycleTruthForV1(t, vectors, queries, topK))
	check("insert-delete-move", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})
	if err := liveLifecycleReplaceV1(t.Context(), client, queries[1], 2, AckSynced); err != nil {
		t.Fatal(err)
	}
	afterMeta := remember(liveLifecycleTruthForV1(t, vectors, queries, topK))
	if beforeMeta.MutatedIDs != afterMeta.MutatedIDs || beforeMeta.LiveIDs != afterMeta.LiveIDs || beforeMeta.Cutovers != afterMeta.Cutovers {
		t.Fatalf("metadata grew live state: before=%+v after=%+v", beforeMeta, afterMeta)
	}
	docs, present, err := client.GetMany(t.Context(), "docs", [][]byte{[]byte("a"), []byte("doc-0001"), []byte("live")})
	if err != nil || !reflect.DeepEqual(present, []bool{true, false, true}) {
		t.Fatalf("document visibility=%v err=%v", present, err)
	}
	var metadata struct {
		Marker int `json:"marker"`
	}
	if err := json.Unmarshal(docs[0], &metadata); err != nil || metadata.Marker != 2 {
		t.Fatalf("metadata=%+v err=%v", metadata, err)
	}
	check("metadata-only", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})

	// Precompute two independently scored corpus states; only one writer may
	// publish them. Status capture is untimed writer bookkeeping, not a search
	// pin held across writes and not an inferred revision increment.
	states := make([][]liveLifecycleTruthV1, 2)
	for state := range states {
		vectors["a"] = queries[state]
		states[state] = liveLifecycleTruthForV1(t, vectors, queries, topK)
	}
	startReaders := func(attempts []liveLifecycleAttemptV1, start <-chan struct{}, wg *sync.WaitGroup) {
		for worker := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := range perWorker {
					id := worker*perWorker + i
					attempts[id] = search(id, i%2)
				}
			}()
		}
	}
	queryOnly := make([]liveLifecycleAttemptV1, workers*perWorker)
	baselineStart := make(chan struct{})
	var baselineWG sync.WaitGroup
	startReaders(queryOnly, baselineStart, &baselineWG)
	close(baselineStart)
	baselineWG.Wait()
	check("query-only", queryOnly)
	queryOnly = nil
	attempts := make([]liveLifecycleAttemptV1, workers*perWorker)
	writes := make([]liveLifecycleWriteV1, mutations)
	start := make(chan struct{})
	var wg sync.WaitGroup
	startReaders(attempts, start, &wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := range writes {
			w := liveLifecycleWriteV1{started: time.Now()}
			w.err = liveLifecycleReplaceV1(t.Context(), client, queries[i%2], i+3, AckSynced)
			w.finished = time.Now()
			if w.err == nil {
				var s collections.VectorIndexPartitionLiveStatusV1
				s, w.err = liveLifecycleStatusV1(fixture.collection, fixture.manifest)
				w.identity = liveLifecycleIdentityV1{s.Revision, s.Coverage}
			}
			writes[i] = w
		}
	}()
	windowStart := time.Now()
	close(start)
	wg.Wait()
	window := time.Since(windowStart)
	var writeNanos time.Duration
	var writeIdentityError error
	for i, w := range writes {
		if _, exists := truth[w.identity]; exists && w.err == nil {
			writeIdentityError = fmt.Errorf("write %d reused revision/coverage %+v", i, w.identity)
		} else if w.err == nil {
			truth[w.identity] = states[i%2]
		}
		writeNanos += w.finished.Sub(w.started)
	}
	overlaps, revisions, progressed := 0, make(map[liveLifecycleIdentityV1]bool), false
	for _, a := range attempts {
		revisions[liveLifecycleIdentityV1{a.response.LiveRevision, a.response.LiveCoverage}] = true
		for _, w := range writes {
			if a.started.Before(w.finished) && w.started.Before(a.finished) {
				overlaps++
				break
			}
		}
		if a.started.After(writes[0].finished) {
			progressed = true
		}
	}
	check("concurrent-write", attempts, writes...)
	if writeIdentityError != nil {
		t.Fatal(writeIdentityError)
	}
	if overlaps == 0 || len(revisions) < 2 || !progressed {
		t.Fatalf("no concurrent progress: overlapping_queries=%d revisions=%d progressed=%v", overlaps, len(revisions), progressed)
	}
	t.Logf("scope=standalone-components fixture=procedural rows=%d dims=%d domains=2 packs=3 reads=%d writes=%d overlapping_queries=%d revisions=%d window_ns=%d sum_write_ns=%d", rows, dimensions, len(attempts), len(writes), overlaps, len(revisions), window, writeNanos)
	current = writes[len(writes)-1].identity
	check("after-write", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})
	closeReader()
	coordinator, closeReader = liveLifecycleTCPReaderV1(t, fixture)
	check("cold-source", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})
	if err := client.CheckpointWithAck(t.Context(), AckSynced); err != nil {
		t.Fatal(err)
	}
	closeReader()
	closeClient()
	if err := fixture.database.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.database = nil
	reopen := func() {
		var err error
		fixture.database, err = backenddb.Open(backenddb.Options{Dir: fixture.dir, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		fixture.collection, err = collections.NewCollectionManager(fixture.database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		coordinator, closeReader = liveLifecycleTCPReaderV1(t, fixture)
	}
	reopen()
	check("checkpoint-reopen", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})
	checkDocuments := func(marker int, vector []float32) {
		t.Helper()
		reader, closeNative := liveLifecycleNativeClientV1(t, fixture.database)
		defer closeNative()
		docs, present, err := reader.GetMany(t.Context(), "docs", [][]byte{[]byte("a"), []byte("doc-0001"), []byte("live")})
		if err != nil || !reflect.DeepEqual(present, []bool{true, false, true}) {
			t.Fatalf("recovered document visibility=%v err=%v", present, err)
		}
		var a struct {
			Embedding []float32 `json:"embedding"`
			Marker    int       `json:"marker"`
		}
		var live struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(docs[0], &a); err != nil || a.Marker != marker || !slices.Equal(a.Embedding, vector) {
			t.Fatalf("recovered a marker=%d want=%d err=%v", a.Marker, marker, err)
		}
		if err := json.Unmarshal(docs[2], &live); err != nil || !slices.Equal(live.Embedding, queries[0]) {
			t.Fatalf("recovered live embedding: %v", err)
		}
	}
	checkDocuments(mutations+2, queries[(mutations-1)%2])
	closeReader()
	if _, err := fixture.collection.ColumnAssetGC(t.Context(), collections.ColumnAssetGCOptions{}); err != nil {
		t.Fatal(err)
	}
	coordinator, closeReader = liveLifecycleTCPReaderV1(t, fixture)
	check("active-generation-after-gc", []liveLifecycleAttemptV1{search(0, 0), search(1, 1)})
	checkDocuments(mutations+2, queries[(mutations-1)%2])
	closeReader()
	if err := fixture.database.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.database = nil
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorPartitionLiveSelectedLifecycleV1$")
	cmd.Env = append(os.Environ(), "GOMAP_SELECTED_LIVE_CRASH_DIR="+fixture.dir, "GOMAP_SELECTED_LIVE_GENERATION="+strconv.FormatUint(fixture.manifest.Generation, 10))
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("durable-ack crash helper: %v\n%s", err, output)
	}
	reopen()
	recovered := []liveLifecycleAttemptV1{search(0, 0), search(1, 1)}
	vectors["a"] = queries[0]
	remember(liveLifecycleTruthForV1(t, vectors, queries, topK))
	check("durable-ack-crash-reopen", recovered)
	checkDocuments(999, queries[0])
}

func TestVectorPartitionLiveLifecycleReceiptRejectsV1(t *testing.T) {
	key := liveLifecycleIdentityV1{1, 2}
	truth := map[liveLifecycleIdentityV1][]liveLifecycleTruthV1{key: {{scores: map[string]float32{"a": 1}, top: map[string]bool{"a": true}}}}
	base := liveLifecycleAttemptV1{id: 0, started: time.Now(), finished: time.Now().Add(time.Second), response: VectorPartitionCoordinatorResponseV1{
		PartitionGeneration: 3, LiveRevision: 1, LiveCoverage: 2, Neighbors: []VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: 1}},
		ProbedDomains: []uint32{0}, ProbedPacks: []uint32{0, 1}, ProbedPartitions: []uint32{0, 1},
		Counters: VectorPartitionCoordinatorCountersV1{SelectedDomains: 1, SelectedPacks: 2, SelectedPartitions: 2, HNSWServedPartitions: 2, LiveDomainsSearched: 1},
	}}
	if hits, err := validateLiveLifecycleAttemptsV1([]liveLifecycleAttemptV1{base}, 1, 1, 3, truth); err != nil || hits != 1 {
		t.Fatalf("valid hits=%d err=%v", hits, err)
	}
	for _, corrupt := range []struct {
		name   string
		change func(*liveLifecycleAttemptV1)
	}{
		{"failure", func(a *liveLifecycleAttemptV1) { a.err = context.DeadlineExceeded }},
		{"coverage", func(a *liveLifecycleAttemptV1) { a.response.LiveCoverage++ }},
		{"revision", func(a *liveLifecycleAttemptV1) { a.response.LiveRevision++ }},
		{"generation", func(a *liveLifecycleAttemptV1) { a.response.PartitionGeneration++ }},
		{"unattempted", func(a *liveLifecycleAttemptV1) { a.started = time.Time{} }},
		{"partial", func(a *liveLifecycleAttemptV1) { a.response.Neighbors = nil }},
		{"stale", func(a *liveLifecycleAttemptV1) {
			a.response.Neighbors = []VectorPartitionCoordinatorNeighborV1{{ID: "deleted", Score: 1}}
		}},
		{"score", func(a *liveLifecycleAttemptV1) {
			a.response.Neighbors = []VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: .9}}
		}},
		{"fallback", func(a *liveLifecycleAttemptV1) { a.response.Counters.ExactScanPartitions = 1 }},
		{"missing-pack", func(a *liveLifecycleAttemptV1) { a.response.ProbedPacks = []uint32{0} }},
		{"duplicate-pack", func(a *liveLifecycleAttemptV1) { a.response.ProbedPacks = []uint32{0, 0} }},
		{"wrong-domain", func(a *liveLifecycleAttemptV1) { a.response.ProbedDomains = []uint32{1} }},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			a := base
			corrupt.change(&a)
			if _, err := validateLiveLifecycleAttemptsV1([]liveLifecycleAttemptV1{a}, 1, 1, 3, truth); err == nil {
				t.Fatal("accepted corrupt receipt")
			}
		})
	}
	if _, err := validateLiveLifecycleAttemptsV1([]liveLifecycleAttemptV1{base}, 2, 1, 3, truth); err == nil {
		t.Fatal("accepted missing failure")
	}
	if _, err := validateLiveLifecycleAttemptsV1([]liveLifecycleAttemptV1{base, base}, 2, 1, 3, truth); err == nil {
		t.Fatal("accepted duplicate attempt")
	}
	newer := liveLifecycleIdentityV1{2, 3}
	if err := validateLiveLifecycleFreshnessV1([]liveLifecycleAttemptV1{base}, newer, nil); err == nil {
		t.Fatal("accepted known-old truth in quiescent phase")
	}
	w := liveLifecycleWriteV1{finished: base.started.Add(-time.Nanosecond), identity: newer}
	if err := validateLiveLifecycleFreshnessV1([]liveLifecycleAttemptV1{base}, key, []liveLifecycleWriteV1{w}); err == nil {
		t.Fatal("accepted known-old truth after write ack")
	}
	w.finished = base.started.Add(time.Nanosecond)
	if err := validateLiveLifecycleFreshnessV1([]liveLifecycleAttemptV1{base}, key, []liveLifecycleWriteV1{w}); err != nil {
		t.Fatalf("rejected legitimate in-flight old pin: %v", err)
	}
}
