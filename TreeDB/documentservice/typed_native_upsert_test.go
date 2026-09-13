package documentservice

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestServiceTypedNativeConcurrentInsertsShareAdmissionAndPublication(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	s := New(collections.NewCollectionManager(db))
	defer db.Close()
	defer s.Close()
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "typed-group", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := s.manager.OpenCollection(info.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.primeBenchmarkSearchCache(info.Name, cached, info); err != nil {
		t.Fatal(err)
	}
	request := func(id string, vector []float32) TypedDocumentsRequest {
		return TypedDocumentsRequest{
			ExpectedGeneration: info.Generation,
			IDs:                [][]byte{[]byte(id)},
			Retained:           [][]byte{[]byte(`{"id":"` + id + `"}`)},
			Columns: []collections.TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{vector}},
				{Name: "content", Strings: []string{"grouped"}},
			},
		}
	}
	var hookMu sync.Mutex
	arrived := 0
	var admitted []*collections.Collection
	release := make(chan struct{})
	s.typedUpsertBeforeGroup = func(col *collections.Collection) {
		hookMu.Lock()
		admitted = append(admitted, col)
		arrived++
		if arrived == 2 {
			close(release)
		}
		hookMu.Unlock()
		<-release
	}
	defer func() { s.typedUpsertBeforeGroup = nil }()
	beforeState := db.State()
	beforeSyncs, err := strconv.ParseUint(db.Stats()["treedb.command_wal.file_sync.calls_total"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// Holding another shared admission lease makes an accidental exclusive
	// route (or exclusive fallback) block deterministically.
	s.writeMu.RLock()
	sharedHeld := true
	defer func() {
		if sharedHeld {
			s.writeMu.RUnlock()
		}
	}()
	type result struct {
		out UpsertDocumentsResponse
		err error
	}
	done := make(chan result, 2)
	for _, input := range []TypedDocumentsRequest{request("group-a", []float32{1, 0}), request("group-b", []float32{0, 1})} {
		input := input
		go func() {
			out, err := s.UpsertTypedDocuments(ctx, info.Name, input)
			done <- result{out: out, err: err}
		}()
	}
	for range 2 {
		select {
		case got := <-done:
			if got.err != nil || got.out.Inserted != 1 || got.out.Updated != 0 {
				t.Fatalf("grouped typed service result=%+v err=%v", got.out, got.err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("concurrent typed service call did not complete under shared admission")
		}
	}
	hookMu.Lock()
	gotAdmitted := append([]*collections.Collection(nil), admitted...)
	hookMu.Unlock()
	if len(gotAdmitted) != 2 {
		t.Fatalf("shared write handle count=%d want=2", len(gotAdmitted))
	}
	if gotAdmitted[0] == cached || gotAdmitted[1] == cached || gotAdmitted[0] == gotAdmitted[1] {
		t.Fatalf("shared write handles=%p,%p cached=%p want independent", gotAdmitted[0], gotAdmitted[1], cached)
	}
	s.writeMu.RUnlock()
	sharedHeld = false
	afterState := db.State()
	if afterState.CommitSeq != beforeState.CommitSeq+1 {
		t.Fatalf("commit seq=%d want one grouped publication after %d", afterState.CommitSeq, beforeState.CommitSeq)
	}
	afterSyncs, err := strconv.ParseUint(db.Stats()["treedb.command_wal.file_sync.calls_total"], 10, 64)
	if err != nil || afterSyncs != beforeSyncs+1 {
		t.Fatalf("command WAL file syncs=%d want=%d err=%v", afterSyncs, beforeSyncs+1, err)
	}
	fetched, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, [][]byte{[]byte("group-a"), []byte("group-b")})
	if err != nil || len(fetched.Results) != 2 || !fetched.Results[0].Found || !fetched.Results[1].Found {
		t.Fatalf("grouped typed service fetch=%+v err=%v", fetched, err)
	}
}

func TestServiceTypedNativeCancelledPreQueueUpdateDoesNotFallback(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	s := New(collections.NewCollectionManager(db))
	defer db.Close()
	defer s.Close()
	info, err := s.CreateIndex(t.Context(), CreateIndexRequest{Name: "typed-cancel", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	request := func(content string) TypedDocumentsRequest {
		return TypedDocumentsRequest{
			ExpectedGeneration: info.Generation,
			IDs:                [][]byte{[]byte("existing")},
			Retained:           [][]byte{[]byte(`{"id":"existing"}`)},
			Columns: []collections.TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}},
				{Name: "content", Strings: []string{content}},
			},
		}
	}
	if out, err := s.UpsertTypedDocuments(t.Context(), info.Name, request("before")); err != nil || out.Inserted != 1 {
		t.Fatalf("seed typed insert=%+v err=%v", out, err)
	}
	beforeState := db.State()
	beforeNextLSN := db.CommandWALNextLSN()
	beforeSyncs, err := strconv.ParseUint(db.Stats()["treedb.command_wal.file_sync.calls_total"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// The update is ineligible before group admission. Hold an existing reader
	// until its exclusive fallback is waiting, then cancel before that fallback
	// acquires service admission.
	s.writeMu.RLock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := s.UpsertTypedDocuments(ctx, info.Name, request("after"))
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for s.writeMu.TryRLock() {
		s.writeMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("typed update did not reach exclusive fallback")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	s.writeMu.RUnlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled typed update error=%v want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled typed update did not return")
	}
	if got := db.CommandWALNextLSN(); got != beforeNextLSN {
		t.Fatalf("cancelled fallback next LSN=%d want unchanged %d", got, beforeNextLSN)
	}
	if got := db.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("cancelled fallback commit seq=%d want unchanged %d", got, beforeState.CommitSeq)
	}
	afterSyncs, err := strconv.ParseUint(db.Stats()["treedb.command_wal.file_sync.calls_total"], 10, 64)
	if err != nil || afterSyncs != beforeSyncs {
		t.Fatalf("cancelled fallback file syncs=%d want=%d err=%v", afterSyncs, beforeSyncs, err)
	}
	fetched, err := s.FetchTypedDocuments(t.Context(), info.Name, info.Generation, [][]byte{[]byte("existing")})
	if err != nil || len(fetched.Results) != 1 || !fetched.Results[0].Found || !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"before"`)) {
		t.Fatalf("cancelled fallback fetch=%+v err=%v", fetched, err)
	}
}

func TestServiceTypedNativeUpsertContract(t *testing.T) {
	s, db := newTestService(t)
	defer db.Close()
	defer s.Close()
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "binary", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	req := TypedDocumentsRequest{ExpectedGeneration: info.Generation, IDs: [][]byte{[]byte("a")}, Retained: [][]byte{[]byte(`{"id":"a","meta":{"extra":"owned"}}`)}, Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "meta.user_id", Strings: []string{"u"}}}}
	out, err := s.UpsertTypedDocuments(ctx, info.Name, req)
	if err != nil || out.Inserted != 1 {
		t.Fatalf("insert=%+v %v", out, err)
	}
	out, err = s.UpsertTypedDocuments(ctx, info.Name, req)
	if err != nil || out.Updated != 1 || out.Inserted != 0 {
		t.Fatalf("noop=%+v %v", out, err)
	}
	for _, name := range []string{"generation", "missing", "duplicate", "unknown", "dimension", "residual"} {
		t.Run(name, func(t *testing.T) {
			bad := req
			bad.Columns = append([]collections.TypedColumnBatch(nil), req.Columns...)
			switch name {
			case "generation":
				bad.ExpectedGeneration++
			case "missing":
				bad.Columns = bad.Columns[:2]
			case "duplicate":
				bad.Columns[2].Name = "content"
			case "unknown":
				bad.Columns[2].Name = "meta.other"
			case "dimension":
				bad.Columns[0].Float32Vectors = [][]float32{{1}}
			case "residual":
				bad.Retained = [][]byte{[]byte(`{"id":"a","content":"sneaked"}`)}
			}
			if _, err := s.UpsertTypedDocuments(ctx, info.Name, bad); err == nil {
				t.Fatal("invalid typed request accepted")
			}
		})
	}
}

func TestServiceTypedNativeUpsertPublishesLastCompletedInsertStats(t *testing.T) {
	s, db := newTestService(t)
	defer db.Close()
	defer s.Close()
	s.DiagnosticsHandler(nil)
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "typed-stats", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	request := func(id string) TypedDocumentsRequest {
		return TypedDocumentsRequest{
			ExpectedGeneration: info.Generation,
			IDs:                [][]byte{[]byte(id)},
			Retained:           [][]byte{[]byte(`{"id":"` + id + `"}`)},
			Columns: []collections.TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}},
				{Name: "content", Strings: []string{"text"}},
			},
		}
	}
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, request("first")); err != nil {
		t.Fatal(err)
	}
	first := s.DiagnosticsSnapshot(nil).LastOpened
	// A short source plan can complete within one Windows clock tick. Exact row
	// and work counters below establish that it ran even when its duration is zero.
	if first == nil || first.Insert.Documents != 1 || first.Insert.SourceReplacementPlan < 0 || first.Insert.Publish <= 0 || first.Insert.ColumnPublishRows != 1 || first.Insert.ColumnPublishBuildColumnDelta <= 0 || first.Insert.ColumnPublishCommit <= 0 || first.Insert.ColumnPublishCommitExclusiveTotal() <= 0 || first.Insert.ColumnPublishManifestBytes <= 0 || first.Insert.ColumnPublishFinalizeCandidateResourceWork.SourceEntriesInspected == 0 || first.Insert.ColumnPublishFinalizeCandidateDependencyBytes == 0 || first.Insert.ColumnPublishFinalizeCandidateOwnedBytes < first.Insert.ColumnPublishFinalizeCandidateDependencyBytes || first.Insert.ColumnPublishFinalizeAdmissionPendingBytes < first.Insert.ColumnPublishFinalizeCandidateOwnedBytes || first.Insert.ColumnPublishFinalizeAdmissionPendingBytes == 0 || first.Insert.ColumnPublishFinalizeAdmissionPendingCommits == 0 {
		t.Fatalf("first typed insert diagnostics=%+v", first)
	}
	secondRequest := request("second")
	secondRequest.IDs = append(secondRequest.IDs, []byte("third"))
	secondRequest.Retained = append(secondRequest.Retained, []byte(`{"id":"third"}`))
	secondRequest.Columns[0].Float32Vectors = append(secondRequest.Columns[0].Float32Vectors, []float32{0, 1})
	secondRequest.Columns[1].Strings = append(secondRequest.Columns[1].Strings, "more text")
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, secondRequest); err != nil {
		t.Fatal(err)
	}
	second := s.DiagnosticsSnapshot(nil).LastOpened
	if second == nil || second.Insert.Documents != 2 || second.Insert.SourceReplacementPlan < 0 || second.Insert.Publish <= 0 || second.Insert.ColumnPublishRows != 2 || second.Insert.ColumnPublishBuildColumnDelta <= 0 || second.Insert.ColumnPublishCommit <= 0 || second.Insert.ColumnPublishCommitExclusiveTotal() <= 0 || second.Insert.ColumnPublishManifestBytes <= 0 || second.Insert.ColumnPublishFinalizeCandidateResourceWork.SourceEntriesInspected == 0 || second.Insert.ColumnPublishFinalizeCandidateDependencyBytes == 0 || second.Insert.ColumnPublishFinalizeCandidateOwnedBytes < second.Insert.ColumnPublishFinalizeCandidateDependencyBytes || second.Insert.ColumnPublishFinalizeAdmissionPendingBytes < second.Insert.ColumnPublishFinalizeCandidateOwnedBytes || second.Insert.ColumnPublishFinalizeAdmissionPendingBytes == 0 || second.Insert.ColumnPublishFinalizeAdmissionPendingCommits == 0 {
		t.Fatalf("second typed insert diagnostics=%+v", second)
	}
	if first.Insert.Documents != 1 {
		t.Fatalf("first snapshot changed after second completion: %+v", first.Insert)
	}
	failed := request("failed")
	failed.ExpectedGeneration++
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, failed); err == nil {
		t.Fatal("stale typed upsert succeeded")
	}
	if after := s.DiagnosticsSnapshot(nil).LastOpened; after == nil || after.Insert.Documents != 2 {
		t.Fatalf("failed request replaced last completed diagnostics: %+v", after)
	}
}
