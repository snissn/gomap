package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func partitionLivePersistV1ForTest(idx *VectorIndex) *vectorIndexPartitionLivePersistV1 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	if live == nil || live.invalid {
		return nil
	}
	persisted := &vectorIndexPartitionLivePersistV1{
		Version: 1, IndexDefinitionDigest: live.indexDefinitionDigest, Source: live.source,
		Generation: live.generation, Revision: live.revision, Coverage: live.coverage, Cutovers: live.cutovers,
		PackDomains:     append([]uint32(nil), live.packDomains...),
		Representatives: make([]vectorIndexPartitionLivePersistRepV1, len(live.representatives)),
		Owners:          make([]vectorIndexPartitionLivePersistOwnerV1, 0, len(live.owners)),
		Domains:         make([]vectorIndexPartitionLivePersistDomainV1, 0, len(live.domains)),
	}
	for i, rep := range live.representatives {
		persisted.Representatives[i] = vectorIndexPartitionLivePersistRepV1{Domain: rep.domain, Vector: append([]float32(nil), rep.vector...)}
	}
	for id, owner := range live.owners {
		persisted.Owners = append(persisted.Owners, vectorIndexPartitionLivePersistOwnerV1{ID: id, Domain: owner.domain, Deleted: owner.deleted})
	}
	sort.Slice(persisted.Owners, func(i, j int) bool { return persisted.Owners[i].ID < persisted.Owners[j].ID })
	domains := make([]uint32, 0, len(live.domains))
	for domain := range live.domains {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	for _, domain := range domains {
		snapshot, _ := live.domains[domain].persistSnapshot()
		persisted.Domains = append(persisted.Domains, vectorIndexPartitionLivePersistDomainV1{Domain: domain, Snapshot: snapshot})
	}
	return persisted
}

func TestVectorIndexPartitionLiveMutationMoveAndDeleteV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 3,
		Generation: 7, DomainCount: 2, PartitionCount: 3,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 2}},
	}
	representatives := []vectorPartitionLiveRepresentativeV1{
		{domain: 0, vector: []float32{1, 0}},
		{domain: 1, vector: []float32{0, 1}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, representatives); err != nil {
		t.Fatal(err)
	}

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !pin.ExcludesBaseIDV1("doc") || pin.DomainDeltaCountV1(0) != 1 || pin.DomainDeltaCountV1(1) != 0 {
		t.Fatalf("insert pin=%+v", pin.StatusV1())
	}
	pin.Release()

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err = idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if pin.DomainDeltaCountV1(0) != 0 || pin.DomainDeltaCountV1(1) != 1 {
		t.Fatalf("move pin=%+v", pin.StatusV1())
	}
	results, _, err := pin.SearchDomainV1(context.Background(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if err != nil || len(results) != 1 || results[0].ID != "doc" {
		t.Fatalf("move search results=%v err=%v", results, err)
	}
	pin.Release()

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), nil); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err = idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if !pin.ExcludesBaseIDV1("doc") || pin.DomainDeltaCountV1(0) != 0 || pin.DomainDeltaCountV1(1) != 0 {
		t.Fatalf("delete pin=%+v", pin.StatusV1())
	}
}

func TestVectorIndexPartitionLiveNativeDeltaTouchesOnlyChangedRecordsV2(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	idx.setNativePersistent(true)
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2,
		Generation: 7, DomainCount: 2, PartitionCount: 2,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{
		{domain: 0, vector: []float32{1, 0}},
		{domain: 1, vector: []float32{0, 1}},
	}); err != nil {
		t.Fatal(err)
	}
	apply := func(id string, vector []float32) {
		t.Helper()
		idx.mu.Lock()
		defer idx.mu.Unlock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
	}
	apply("a", []float32{1, 0})
	apply("z", []float32{0, 1})
	idx.recordPersistedSnapshot(1, 0, idx.nativeMutationSequence())

	deltaKeys := func() []string {
		t.Helper()
		table, _, seq, _, hasWork, err := idx.persistNativeDeltaTable(true)
		if err != nil {
			t.Fatal(err)
		}
		if !hasWork {
			t.Fatal("missing native delta")
		}
		table.Freeze()
		defer resetCollectionRunTable(table)
		it := table.NewIterator(nil, nil)
		defer it.Close()
		var keys []string
		for it.Valid() {
			keys = append(keys, string(it.KeyCopy(nil)))
			if string(it.KeyCopy(nil)) == vectorIndexNativeKeyMeta {
				value := string(it.ValueCopy(nil))
				if strings.Contains(value, `"owners"`) || strings.Contains(value, `"snapshot"`) {
					t.Fatalf("compact metadata embeds live records: %s", value)
				}
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		idx.recordPersistedSnapshot(1, 0, seq)
		return keys
	}
	requireTouched := func(keys []string, owners []string, domains []string) {
		t.Helper()
		var gotOwners, gotDomains []string
		for _, key := range keys {
			switch {
			case strings.HasPrefix(key, "partition_live/owner/"):
				gotOwners = append(gotOwners, key)
			case strings.HasPrefix(key, "partition_live/domain/"):
				rest := strings.TrimPrefix(key, "partition_live/domain/")
				domain, _, ok := strings.Cut(rest, "/")
				if ok && !slices.Contains(gotDomains, domain) {
					gotDomains = append(gotDomains, domain)
				}
			}
		}
		slices.Sort(gotOwners)
		slices.Sort(gotDomains)
		if !slices.Equal(gotOwners, owners) || !slices.Equal(gotDomains, domains) {
			t.Fatalf("keys=%q owners=%q domains=%q want owners=%q domains=%q", keys, gotOwners, gotDomains, owners, domains)
		}
	}

	apply("a", []float32{1, 0})
	requireTouched(deltaKeys(), nil, nil)
	apply("a", []float32{.9, .1})
	requireTouched(deltaKeys(), []string{"partition_live/owner/00000000000000000001/a"}, []string{"00000000000000000000"})
	apply("a", []float32{0, 1})
	requireTouched(deltaKeys(), []string{"partition_live/owner/00000000000000000001/a"}, []string{"00000000000000000000", "00000000000000000001"})
	apply("a", nil)
	requireTouched(deltaKeys(), []string{"partition_live/owner/00000000000000000001/a"}, []string{"00000000000000000001"})
}

func TestVectorIndexPartitionLiveReplayCandidateSynchronizesSharedOwnersV2(t *testing.T) {
	before, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1,
		Generation: 7, DomainCount: 1, PartitionCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	if err := before.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	before.mu.Lock()
	if err := before.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{1, 0}); err != nil {
		before.mu.Unlock()
		t.Fatal(err)
	}
	before.mu.Unlock()
	before.recordPersistedSnapshot(1, 0, before.nativeMutationSequence())

	candidate, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.clonePartitionLiveReplayStateV2(before, before.nativeMutationSequence(), 1); reason != "" {
		t.Fatalf("clone reason=%q", reason)
	}

	before.mu.Lock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		candidate.mu.RLock()
		close(started)
		_, _ = candidate.partitionLive.ownerV1("a")
		candidate.mu.RUnlock()
		close(done)
	}()
	<-started
	select {
	case <-done:
		before.mu.Unlock()
		t.Fatal("candidate read shared owners without source lock")
	case <-time.After(10 * time.Millisecond):
	}
	before.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("candidate owner read remained blocked")
	}
}

func TestVectorIndexPartitionLiveReplayDomainTransactionRollbackRetryV2(t *testing.T) {
	before, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2,
		Generation: 7, DomainCount: 1, PartitionCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	if err := before.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	before.mu.Lock()
	for _, row := range []struct {
		id     string
		vector []float32
	}{{"a", []float32{1, 0}}, {"b", []float32{0.6, 0.8}}} {
		if err := before.reconcileVectorPartitionMutationLocked([]byte(row.id), row.vector); err != nil {
			before.mu.Unlock()
			t.Fatal(err)
		}
	}
	before.mu.Unlock()
	before.recordPersistedSnapshot(1, 0, before.nativeMutationSequence())
	beforeSeq := before.nativeMutationSequence()
	domain := before.partitionLive.domains[0]
	baseDomainSeq := domain.nativeMutationSequence()
	baseSnapshot, _ := domain.persistSnapshot()
	baseJSON, err := json.Marshal(baseSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	baseView := domain.acquireSearchView()
	if baseView == nil {
		t.Fatal("missing base search view")
	}
	domain.releaseSearchView(baseView)

	type builtAttempt struct {
		attempt *vectorPartitionLiveReplayAttemptV1
		records [][]byte
	}
	build := func() builtAttempt {
		t.Helper()
		candidate, err := newVectorIndex(nil, VectorIndexOptions{
			Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
			Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		if reason := candidate.clonePartitionLiveReplayStateV2(before, beforeSeq, 1); reason != "" {
			t.Fatalf("clone reason=%q", reason)
		}
		attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{candidate: candidate}}}
		candidate.mu.Lock()
		err = candidate.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{0, 1})
		candidate.mu.Unlock()
		if err != nil {
			attempt.discard()
			t.Fatal(err)
		}
		table, _, _, _, hasWork, err := candidate.persistNativeDeltaTable(true)
		if err != nil || !hasWork {
			attempt.discard()
			t.Fatalf("persist work=%t err=%v", hasWork, err)
		}
		table.Freeze()
		it := table.NewIterator(nil, nil)
		var records [][]byte
		for it.Valid() {
			record := append(it.KeyCopy(nil), 0)
			record = append(record, it.ValueCopy(nil)...)
			records = append(records, record)
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			resetCollectionRunTable(table)
			attempt.discard()
			t.Fatal(err)
		}
		_ = it.Close()
		resetCollectionRunTable(table)
		return builtAttempt{attempt: attempt, records: records}
	}
	assertOldSearch := func() {
		t.Helper()
		pin, err := before.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
		pin.Release()
		if searchErr != nil || len(results) != 1 || results[0].ID != "b" {
			t.Fatalf("prepublication results=%+v err=%v", results, searchErr)
		}
	}

	first := build()
	viewDuring := domain.acquireSearchView()
	if viewDuring != baseView {
		domain.releaseSearchView(viewDuring)
		first.attempt.discard()
		t.Fatal("speculative mutation published a search view")
	}
	domain.releaseSearchView(viewDuring)
	assertOldSearch()
	first.attempt.discard()
	afterRollback, _ := domain.persistSnapshot()
	afterRollbackJSON, err := json.Marshal(afterRollback)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterRollbackJSON, baseJSON) || domain.nativeMutationSequence() != baseDomainSeq || domain.partitionLiveMutationUndo != nil {
		t.Fatalf("rollback changed domain: before=%s after=%s seq=%d want=%d undo=%v", baseJSON, afterRollbackJSON, domain.nativeMutationSequence(), baseDomainSeq, domain.partitionLiveMutationUndo != nil)
	}
	assertOldSearch()

	second := build()
	if !slices.EqualFunc(first.records, second.records, bytes.Equal) {
		second.attempt.discard()
		t.Fatalf("retry records changed: first=%q second=%q", first.records, second.records)
	}
	second.attempt.entries[0].candidate.mu.Lock()
	transaction := second.attempt.entries[0].candidate.partitionLive.domainTransactions[0]
	second.attempt.entries[0].candidate.partitionLive.domainTransactions = nil
	second.attempt.entries[0].candidate.mu.Unlock()
	transaction.mu.Lock()
	transaction.commitPartitionLiveDomainMutationV2Locked()
	transaction.mu.Unlock()
	pin, err := before.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("committed results=%+v err=%v", results, searchErr)
	}
}

func TestVectorIndexPartitionLivePublicInsertWaitsForReplayRollbackV2(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	current := collection.registeredVectorIndex(def.Name)
	if current == nil {
		t.Fatal("missing current carrier")
	}
	current.mu.Lock()
	if err := current.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{0, 1}); err != nil {
		current.mu.Unlock()
		t.Fatal(err)
	}
	current.mu.Unlock()
	if status, err := current.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		t.Fatalf("persist initial carrier status=%+v err=%v", status, err)
	}
	rootID, err := collection.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	beforeSeq := current.nativeMutationSequence()
	candidate, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	coord := collection.collectionSchemaCoordinator()
	if coord == nil {
		t.Fatal("missing collection schema coordinator")
	}
	coord.partitionLivePublishMu.Lock()
	if reason := candidate.clonePartitionLiveReplayStateV2(current, beforeSeq, rootID); reason != "" {
		coord.partitionLivePublishMu.Unlock()
		t.Fatalf("clone reason=%q", reason)
	}
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{candidate: candidate}}}
	candidate.mu.Lock()
	err = candidate.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{.8, .2})
	candidate.mu.Unlock()
	if err != nil {
		attempt.discard()
		coord.partitionLivePublishMu.Unlock()
		t.Fatal(err)
	}

	reachedPublication := make(chan struct{})
	var reachedOnce sync.Once
	restoreHook := setVectorPartitionLiveInsertDocumentBeforePublicationHookForTest(func() {
		reachedOnce.Do(func() { close(reachedPublication) })
	})
	defer restoreHook()
	insertDone := make(chan error, 1)
	go func() { insertDone <- current.InsertDocument([]byte("a")) }()
	select {
	case <-reachedPublication:
	case <-time.After(5 * time.Second):
		attempt.discard()
		coord.partitionLivePublishMu.Unlock()
		t.Fatal("public insert did not reach publication admission")
	}
	select {
	case err := <-insertDone:
		attempt.discard()
		coord.partitionLivePublishMu.Unlock()
		t.Fatalf("public insert crossed replay publication barrier: %v", err)
	default:
	}
	attempt.discard()
	coord.partitionLivePublishMu.Unlock()
	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("public insert remained blocked after replay rollback")
	}

	current.mu.RLock()
	live := current.partitionLive
	owner, ownerOK := live.ownerV1("a")
	delta := live.domains[owner.domain]
	current.mu.RUnlock()
	if !ownerOK || owner.deleted || delta == nil {
		t.Fatalf("post-rollback owner=%+v ok=%t delta=%p", owner, ownerOK, delta)
	}
	delta.mu.RLock()
	matched := delta.currentVectorMatchesLocked([]byte("a"), []float32{1, 0})
	undo := delta.partitionLiveMutationUndo
	delta.mu.RUnlock()
	if !matched || undo != nil {
		t.Fatalf("post-rollback current vector matched=%t undo=%v", matched, undo != nil)
	}
}

func BenchmarkVectorIndexPartitionLiveIncrementalPublicationV2(b *testing.B) {
	for _, owners := range []int{1, 1024} {
		b.Run(strconv.Itoa(owners)+"-owners-one-domain", func(b *testing.B) {
			before, err := newVectorIndex(nil, VectorIndexOptions{
				Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
				Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
			})
			if err != nil {
				b.Fatal(err)
			}
			manifest := VectorPartitionManifestV1{
				IndexName: "embedding", IndexDefinitionDigest: "definition",
				SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: uint64(owners),
				Generation: 7, DomainCount: 1, PartitionCount: 1,
				DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
			}
			if err := before.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
				b.Fatal(err)
			}
			before.mu.Lock()
			for i := 0; i < owners; i++ {
				id := []byte("doc-" + strconv.Itoa(i))
				if err := before.reconcileVectorPartitionMutationLocked(id, []float32{1, float32(i+1) / float32(owners*100)}); err != nil {
					before.mu.Unlock()
					b.Fatal(err)
				}
			}
			before.mu.Unlock()
			before.recordPersistedSnapshot(1, 0, before.nativeMutationSequence())
			beforeSeq := before.nativeMutationSequence()
			var emittedBytes, emittedRecords uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidate, err := newVectorIndex(nil, VectorIndexOptions{
					Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
					Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
				})
				if err != nil {
					b.Fatal(err)
				}
				if reason := candidate.clonePartitionLiveReplayStateV2(before, beforeSeq, 1); reason != "" {
					b.Fatalf("clone reason=%q", reason)
				}
				attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{candidate: candidate}}}
				candidate.mu.Lock()
				if err := candidate.preflightVectorPartitionMutationLocked([]byte("doc-0"), []float32{.9, .1}); err == nil {
					err = candidate.reconcileVectorPartitionMutationLocked([]byte("doc-0"), []float32{.9, .1})
				}
				candidate.mu.Unlock()
				if err != nil {
					b.Fatal(err)
				}
				table, bytesDisk, snapshotSeq, _, hasWork, err := candidate.persistNativeDeltaTable(true)
				if err != nil || !hasWork {
					b.Fatalf("persist work=%t err=%v", hasWork, err)
				}
				table.Freeze()
				it := table.NewIterator(nil, nil)
				for it.Valid() {
					emittedRecords++
					it.Next()
				}
				if err := it.Error(); err != nil {
					_ = it.Close()
					resetCollectionRunTable(table)
					b.Fatal(err)
				}
				_ = it.Close()
				emittedBytes += uint64(bytesDisk)
				resetCollectionRunTable(table)
				candidate.recordPersistedSnapshot(1, bytesDisk, snapshotSeq)
				attempt.discard()
			}
			b.StopTimer()
			if before.nativeMutationSequence() != beforeSeq {
				b.Fatalf("rollback sequence=%d want=%d", before.nativeMutationSequence(), beforeSeq)
			}
			b.ReportMetric(float64(emittedRecords)/float64(b.N), "records/op")
			b.ReportMetric(float64(emittedBytes)/float64(b.N), "published-B/op")
		})
	}
}

func TestVectorIndexPartitionLiveDomainSearchBoundsV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	err = idx.reconcileVectorPartitionMutationLocked([]byte("oversized"), []float32{1, 0})
	idx.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	pin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	tooSmall := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 8}
	if _, _, err := pin.DomainSearchPreflightV1(0, tooSmall); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("preflight err=%v want unavailable", err)
	}
	if _, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, tooSmall); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("search err=%v want unavailable", err)
	}
	if nodes, scratch, err := pin.DomainSearchPreflightV1(0, VectorPartitionSearchOptionsV1{TopK: 256, EfSearch: 256, MaxStableIDBytes: 4096}); err != nil || nodes != 1 || scratch != 64+4096 {
		t.Fatalf("nodes=%d scratch=%d err=%v", nodes, scratch, err)
	}

	view := &vectorIndexSearchView{nodes: make([]vectorIndexNode, 1), liveDocs: 1}
	many := &VectorIndexPartitionLiveSearchPinV1{domains: make(map[uint32]vectorIndexPartitionLiveDomainPinV1, 19)}
	total := uint64(64_000_000)
	for domain := uint32(0); domain < 19; domain++ {
		many.domains[domain] = vectorIndexPartitionLiveDomainPinV1{view: view, maxStableIDBytes: 1}
		_, scratch, err := many.DomainSearchPreflightV1(domain, VectorPartitionSearchOptionsV1{TopK: 256, EfSearch: 256, MaxStableIDBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		total += scratch
	}
	if total != 64_079_040 || total > 80<<20 {
		t.Fatalf("multi-domain bytes=%d", total)
	}
}

func TestVectorIndexPartitionLiveReplayRejectsDifferentInstalledCarrierV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	current := collection.registeredVectorIndex(def.Name)
	rootID, err := collection.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, beforeSeq := current.persistSnapshot()
	candidate, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("candidate restore reason=%q", reason)
	}
	candidate.recordPersistentDefinition(def)
	candidate.recordLoadedSnapshot(rootID, current.Stats().BytesDisk)
	if err := current.InsertDocument([]byte("a")); err != nil {
		t.Fatal(err)
	}
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{
		spec:      vectorPartitionLiveReplaySpecV1{before: current, definition: def, beforeSeq: beforeSeq, rootName: collectionVectorIndexRootName(collection.name, def.Name)},
		candidate: candidate, snapshotSeq: candidate.nativeMutationSequence(),
	}}}
	err = collection.installVectorPartitionLiveReplayAttemptV1(attempt, []string{attempt.entries[0].spec.rootName}, []uint64{rootID})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("install err=%v want concurrent mutation", err)
	}
	if !current.needsNativeAutoPersist() {
		t.Fatal("concurrent carrier mutation was incorrectly marked persisted")
	}
}

func TestVectorIndexPartitionLiveReplayRejectsCandidateMutationAfterSnapshotV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	current := collection.registeredVectorIndex(def.Name)
	rootID, err := collection.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, beforeSeq := current.persistSnapshot()
	candidate, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("candidate restore reason=%q", reason)
	}
	candidate.recordPersistentDefinition(def)
	candidate.recordLoadedSnapshot(rootID, current.Stats().BytesDisk)
	snapshotSeq := candidate.nativeMutationSequence()
	candidate.mu.Lock()
	candidate.dirtyMeta = true
	candidate.mutationSeq++
	candidate.mu.Unlock()
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{
		spec:      vectorPartitionLiveReplaySpecV1{before: current, definition: def, beforeSeq: beforeSeq, rootName: collectionVectorIndexRootName(collection.name, def.Name)},
		candidate: candidate, snapshotSeq: snapshotSeq,
	}}}
	if err := collection.installVectorPartitionLiveReplayAttemptV1(attempt, []string{attempt.entries[0].spec.rootName}, []uint64{rootID}); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("install err=%v want concurrent mutation", err)
	}
	if collection.registeredVectorIndex(def.Name) != current || !candidate.needsNativeAutoPersist() {
		t.Fatal("mutated candidate was installed or incorrectly marked persisted")
	}
}

func TestVectorIndexPartitionLiveReplayUsesPublicationSequenceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}

	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing base snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, collection.meta.Name)
	if err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	input := columnWritePublishInput{
		meta: collection.meta, catalog: catalog,
		baseCommitSeq: snapshotCommitSeq(snap), baseSystemRoot: snapshotSystemRoot(snap),
		commandWALIntent: &backenddb.CommandWALIntent{}, operation: ColumnPublishOperationDelete,
		sourceDeleteDocuments: []columnWriteDocument{{ID: []byte("a")}},
	}
	_ = snap.Close()
	specs, err := collection.vectorPartitionLiveReplaySpecsV1(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("unrelated-publication"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	state, ok := database.StateToken()
	if !ok || state.CommitSeq <= input.baseCommitSeq {
		t.Fatalf("state=%+v ok=%t base=%d", state, ok, input.baseCommitSeq)
	}

	attempt, err := collection.buildVectorPartitionLiveReplayAttemptV1(input, specs)
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.discard()
	if len(attempt.entries) != 1 {
		t.Fatalf("replay entries=%d want=1", len(attempt.entries))
	}
	persisted, _ := attempt.entries[0].candidate.persistSnapshot()
	want := state.CommitSeq + 1
	if persisted.Meta.SourceDocumentGeneration != want || persisted.Meta.PartitionLive == nil || persisted.Meta.PartitionLive.Coverage != want {
		t.Fatalf("persisted generation=%d live=%+v want=%d", persisted.Meta.SourceDocumentGeneration, persisted.Meta.PartitionLive, want)
	}
}

func TestVectorIndexPartitionLiveSnapshotRecoveryAndMismatchV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 2, PartitionCount: 2, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}}}
	reps := []vectorPartitionLiveRepresentativeV1{
		{domain: 1, vector: []float32{0, 1}},
		{domain: 0, vector: []float32{1, 0}},
		{domain: 0, vector: []float32{.8, .2}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, reps); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	idx.recordSourceDocumentStateLocked(9, backenddb.StateToken{})
	idx.mu.Unlock()
	snapshot, _ := idx.persistSnapshot()
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("restore reason=%q", reason)
	}
	if got := restored.partitionLive.representatives; len(got) != 3 || got[0].domain != 0 || got[1].domain != 0 || got[2].domain != 1 {
		t.Fatalf("restored representatives=%+v", got)
	}
	if restored.partitionLive.byteCapacity != vectorIndexPartitionLiveMaxBytesV1 || restored.partitionLive.nodeIDBytes != uint64(len("doc")) || restored.partitionLive.ownerIDBytes != uint64(len("doc")) {
		t.Fatalf("restored byte accounting capacity=%d node_ids=%d owner_ids=%d", restored.partitionLive.byteCapacity, restored.partitionLive.nodeIDBytes, restored.partitionLive.ownerIDBytes)
	}
	pin, err := restored.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, err := pin.SearchDomainV1(t.Context(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if err != nil || len(results) != 1 || results[0].ID != "doc" {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	tampered := snapshot
	tampered.Meta.PartitionLive = partitionLivePersistV1ForTest(idx)
	tampered.Meta.PartitionLive.Coverage++
	mismatch, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if reason := mismatch.loadPersistSnapshot(tampered); reason != "invalid_partition_live_meta" {
		t.Fatalf("mismatch reason=%q", reason)
	}
	wrongEpoch := snapshot
	liveV2 := *snapshot.Meta.PartitionLive
	liveV2.DomainEpochs = append([]vectorIndexPartitionLivePersistDomainEpochV2(nil), liveV2.DomainEpochs...)
	liveV2.DomainEpochs[0].Epoch++
	wrongEpoch.Meta.PartitionLive = &liveV2
	wrongEpochIndex, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := wrongEpochIndex.loadPersistSnapshot(wrongEpoch); reason != "invalid_partition_live_domain" {
		t.Fatalf("wrong active domain epoch reason=%q", reason)
	}
	orphan := partitionLivePersistV1ForTest(idx)
	orphan.Owners = nil
	if _, reason := restored.restorePartitionLiveV1(orphan, orphan.Coverage); reason != "invalid_partition_live_owner_node" {
		t.Fatalf("orphan reason=%q", reason)
	}
	unordered := partitionLivePersistV1ForTest(idx)
	unordered.Representatives[0], unordered.Representatives[1] = unordered.Representatives[1], unordered.Representatives[0]
	if _, reason := restored.restorePartitionLiveV1(unordered, unordered.Coverage); reason != "invalid_partition_live_representative" {
		t.Fatalf("unordered representatives reason=%q", reason)
	}
	nested := partitionLivePersistV1ForTest(idx)
	nested.Domains[0].Snapshot.Meta.PartitionLive = &vectorIndexPartitionLivePersistV1{Version: 1}
	if _, reason := restored.restorePartitionLiveV1(nested, nested.Coverage); reason != "invalid_partition_live_domain" {
		t.Fatalf("nested live domain reason=%q", reason)
	}
	unsafeV2 := snapshot
	unsafeV2Meta := *snapshot.Meta.PartitionLive
	unsafeV2Meta.Version = 2
	unsafeV2.Meta.PartitionLive = &unsafeV2Meta
	unsafeV2Index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := unsafeV2Index.loadPersistSnapshot(unsafeV2); reason != "invalid_partition_live_meta" {
		t.Fatalf("unsafe V2 reason=%q", reason)
	}
}

func TestVectorIndexPartitionLiveRetiredDomainEpochSurvivesReopenV2(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1,
		Generation: 7, DomainCount: 2, PartitionCount: 2,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}},
	}
	representatives := []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}, {domain: 1, vector: []float32{0, 1}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, representatives); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	retiredEpoch := idx.partitionLive.domainEpochs[1]
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), nil); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	if err := idx.cutoverVectorPartitionLiveLocked(); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	if idx.partitionLive.domains[1] != nil {
		idx.mu.Unlock()
		t.Fatal("domain 1 remained active after cutover")
	}
	idx.recordSourceDocumentStateLocked(9, backenddb.StateToken{})
	idx.mu.Unlock()

	snapshot, _ := idx.persistSnapshot()
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("restore reason=%q", reason)
	}
	if got := restored.partitionLive.domainEpochHighWater; got != retiredEpoch {
		t.Fatalf("restored domain epoch high-water=%d want=%d", got, retiredEpoch)
	}
	restored.mu.Lock()
	if err := restored.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		restored.mu.Unlock()
		t.Fatal(err)
	}
	reusedEpoch := restored.partitionLive.domainEpochs[1]
	restored.mu.Unlock()
	if reusedEpoch <= retiredEpoch {
		t.Fatalf("reused domain epoch=%d must exceed retired epoch=%d", reusedEpoch, retiredEpoch)
	}
	table, _, _, _, hasWork, err := restored.persistNativeDeltaTable(true)
	if err != nil || !hasWork {
		t.Fatalf("persist reused domain work=%t err=%v", hasWork, err)
	}
	table.Freeze()
	it := table.NewIterator(nil, nil)
	retiredPrefix := string(vectorIndexPartitionLiveDomainPrefixV2(1, retiredEpoch))
	reusedPrefix := string(vectorIndexPartitionLiveDomainPrefixV2(1, reusedEpoch))
	seenReused := false
	for it.Valid() {
		key := string(it.KeyCopy(nil))
		if strings.HasPrefix(key, retiredPrefix) {
			resetCollectionRunTable(table)
			t.Fatalf("reused publication rewrote retired prefix %q", key)
		}
		seenReused = seenReused || strings.HasPrefix(key, reusedPrefix)
		it.Next()
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		resetCollectionRunTable(table)
		t.Fatal(err)
	}
	_ = it.Close()
	resetCollectionRunTable(table)
	if !seenReused {
		t.Fatalf("reused publication missing epoch prefix %q", reusedPrefix)
	}
}

func TestVectorIndexPartitionLiveDomainEpochOverflowFailsClosedV3(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1,
		Generation: 7, DomainCount: 1, PartitionCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}

	idx.mu.Lock()
	live := idx.partitionLive
	live.domainEpochHighWater = ^uint64(0)
	beforeRevision, beforeMutationSeq := live.revision, idx.mutationSeq
	beforePublications := live.cutoverPublications
	delta, err := idx.ensurePartitionLiveDomainWritableLocked(0)
	if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) {
		idx.mu.Unlock()
		t.Fatalf("ensure domain err=%v, want capacity", err)
	}
	if delta != nil || len(live.domains) != 0 || len(live.domainEpochs) != 0 || len(live.dirtyDomains) != 0 || len(live.fullDomains) != 0 || live.domainEpochHighWater != ^uint64(0) || live.revision != beforeRevision || idx.mutationSeq != beforeMutationSeq || live.cutoverPublications != beforePublications {
		idx.mu.Unlock()
		t.Fatalf("overflow mutated state delta=%v domains=%d epochs=%d dirty=%d full=%d high_water=%d revision=%d/%d mutation_seq=%d/%d publications=%d/%d", delta, len(live.domains), len(live.domainEpochs), len(live.dirtyDomains), len(live.fullDomains), live.domainEpochHighWater, live.revision, beforeRevision, idx.mutationSeq, beforeMutationSeq, live.cutoverPublications, beforePublications)
	}
	idx.mu.Unlock()
}

func TestVectorIndexPartitionLiveRetiredDomainNativeRootEpochIsolationV3(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	carrier := collection.registeredVectorIndex(def.Name)
	if carrier == nil {
		database.Close()
		t.Fatal("missing registered carrier")
	}
	carrier.mu.Lock()
	if err := carrier.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{1, 0}); err != nil {
		carrier.mu.Unlock()
		database.Close()
		t.Fatal(err)
	}
	retiredEpoch := carrier.partitionLive.domainEpochs[0]
	carrier.mu.Unlock()
	if status, err := carrier.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		database.Close()
		t.Fatalf("persist initial epoch status=%+v err=%v", status, err)
	}
	carrier.mu.Lock()
	if err := carrier.reconcileVectorPartitionMutationLocked([]byte("a"), nil); err != nil {
		carrier.mu.Unlock()
		database.Close()
		t.Fatal(err)
	}
	if err := carrier.cutoverVectorPartitionLiveLocked(); err != nil {
		carrier.mu.Unlock()
		database.Close()
		t.Fatal(err)
	}
	if carrier.partitionLive.domains[0] != nil || carrier.partitionLive.domainEpochHighWater != retiredEpoch {
		carrier.mu.Unlock()
		database.Close()
		t.Fatalf("retired state domains=%v high-water=%d want=%d", carrier.partitionLive.domains, carrier.partitionLive.domainEpochHighWater, retiredEpoch)
	}
	carrier.mu.Unlock()
	if status, err := carrier.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		database.Close()
		t.Fatalf("persist retirement status=%+v err=%v", status, err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database = openCollectionCommandWALDB(t, dir)
	collection, err := NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	recoveredManifest, err := collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), recoveredManifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	carrier = collection.registeredVectorIndex(def.Name)
	carrier.mu.RLock()
	highWater := carrier.partitionLive.domainEpochHighWater
	carrier.mu.RUnlock()
	if highWater != retiredEpoch {
		database.Close()
		t.Fatalf("recovered high-water=%d want=%d", highWater, retiredEpoch)
	}
	if err := carrier.InsertDocument([]byte("a")); err != nil {
		database.Close()
		t.Fatal(err)
	}
	carrier.mu.RLock()
	reusedEpoch := carrier.partitionLive.domainEpochs[0]
	carrier.mu.RUnlock()
	if reusedEpoch <= retiredEpoch {
		database.Close()
		t.Fatalf("reused epoch=%d must exceed retired epoch=%d", reusedEpoch, retiredEpoch)
	}
	if status, err := carrier.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		database.Close()
		t.Fatalf("persist reused epoch status=%+v err=%v", status, err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database = openCollectionCommandWALDB(t, dir)
	defer database.Close()
	collection, err = NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredManifest, err = collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), recoveredManifest); err != nil {
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(recoveredManifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("reopened results=%+v err=%v", results, searchErr)
	}
	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing final snapshot")
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, collection.name)
	if err != nil {
		t.Fatal(err)
	}
	rootName := collectionVectorIndexRootName(collection.name, def.Name)
	prefix := []byte(vectorIndexNativeKeyPrefixLiveDomain)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, prefixEnd(prefix), false)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	retiredPrefix := string(vectorIndexPartitionLiveDomainPrefixV2(0, retiredEpoch))
	reusedPrefix := string(vectorIndexPartitionLiveDomainPrefixV2(0, reusedEpoch))
	retainedOld, activeNew := false, false
	for it.Valid() {
		key := string(it.KeyCopy(nil))
		retainedOld = retainedOld || strings.HasPrefix(key, retiredPrefix)
		activeNew = activeNew || strings.HasPrefix(key, reusedPrefix)
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if !retainedOld || !activeNew {
		t.Fatalf("native root retained_old=%t active_new=%t old=%q new=%q", retainedOld, activeNew, retiredPrefix, reusedPrefix)
	}
}

func TestVectorIndexPartitionLiveMissingTombstoneFailsClosedV3(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1,
		Generation: 7, DomainCount: 1, PartitionCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("stale-base-id"), nil); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	idx.recordSourceDocumentStateLocked(9, backenddb.StateToken{})
	idx.mu.Unlock()
	snapshot, _ := idx.persistSnapshot()
	if snapshot.Meta.PartitionLive == nil || snapshot.Meta.PartitionLive.Version != 3 || snapshot.Meta.PartitionLive.OwnerCount != 1 || len(snapshot.PartitionLiveOwners) != 1 || !snapshot.PartitionLiveOwners[0].Deleted {
		t.Fatalf("persisted tombstone meta=%+v owners=%+v", snapshot.Meta.PartitionLive, snapshot.PartitionLiveOwners)
	}
	snapshot.PartitionLiveOwners = nil
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "invalid_partition_live_meta" {
		t.Fatalf("missing tombstone reason=%q", reason)
	}
}

func TestVectorIndexPartitionLiveV1RestoreMaterializesCompleteV3(t *testing.T) {
	options := VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8}
	idx, err := newVectorIndex(nil, options)
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2,
		Generation: 7, DomainCount: 2, PartitionCount: 2,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}, {domain: 1, vector: []float32{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	for _, row := range []struct {
		id     string
		vector []float32
	}{{"a", []float32{1, 0}}, {"deleted-only", nil}, {"z", []float32{0, 1}}} {
		if err := idx.reconcileVectorPartitionMutationLocked([]byte(row.id), row.vector); err != nil {
			idx.mu.Unlock()
			t.Fatal(err)
		}
	}
	idx.recordSourceDocumentStateLocked(9, backenddb.StateToken{})
	idx.mu.Unlock()
	legacy, _ := idx.persistSnapshot()
	legacy.Meta.PartitionLive = partitionLivePersistV1ForTest(idx)
	if len(legacy.Meta.PartitionLive.Owners) != 3 || len(legacy.Meta.PartitionLive.Domains) != 2 {
		t.Fatalf("legacy owners=%+v domains=%+v", legacy.Meta.PartitionLive.Owners, legacy.Meta.PartitionLive.Domains)
	}
	legacy.PartitionLiveOwners = nil
	legacy.PartitionLiveDomains = nil

	migrated, err := newVectorIndex(nil, options)
	if err != nil {
		t.Fatal(err)
	}
	if reason := migrated.loadPersistSnapshot(legacy); reason != "" {
		t.Fatalf("V1 restore reason=%q", reason)
	}
	candidate, err := newVectorIndex(nil, options)
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.clonePartitionLiveReplayStateV2(migrated, migrated.nativeMutationSequence(), 1); reason != "" {
		t.Fatalf("clone reason=%q", reason)
	}
	candidate.mu.Lock()
	if err := candidate.reconcileVectorPartitionMutationLocked([]byte("a"), []float32{.8, .2}); err != nil {
		candidate.mu.Unlock()
		t.Fatal(err)
	}
	candidate.mu.Unlock()
	table, bytesDisk, seq, _, hasWork, err := candidate.persistNativeDeltaTable(true)
	if err != nil || !hasWork {
		t.Fatalf("V1 migration persist work=%t err=%v", hasWork, err)
	}
	table.Freeze()
	it := table.NewIterator(nil, nil)
	var ownerIDs []string
	var keys []string
	domains := make(map[string]struct{})
	for it.Valid() {
		key := string(it.KeyCopy(nil))
		keys = append(keys, key)
		switch {
		case key == vectorIndexNativeKeyMeta:
			var meta vectorIndexPersistMeta
			if err := json.Unmarshal(it.ValueCopy(nil), &meta); err != nil {
				_ = it.Close()
				resetCollectionRunTable(table)
				t.Fatal(err)
			}
			if meta.PartitionLive == nil || meta.PartitionLive.Version != 3 || meta.PartitionLive.OwnerCount != 3 || len(meta.PartitionLive.DomainEpochs) != 2 {
				_ = it.Close()
				resetCollectionRunTable(table)
				t.Fatalf("migrated meta=%+v", meta.PartitionLive)
			}
		case strings.HasPrefix(key, vectorIndexNativeKeyPrefixLiveOwner):
			_, id, ok := strings.Cut(strings.TrimPrefix(key, vectorIndexNativeKeyPrefixLiveOwner), "/")
			if ok {
				ownerIDs = append(ownerIDs, id)
			}
		case strings.HasPrefix(key, vectorIndexNativeKeyPrefixLiveDomain):
			rest := strings.TrimPrefix(key, vectorIndexNativeKeyPrefixLiveDomain)
			domain, _, ok := strings.Cut(rest, "/")
			if ok {
				domains[domain] = struct{}{}
			}
		}
		it.Next()
	}
	iterErr := it.Error()
	_ = it.Close()
	resetCollectionRunTable(table)
	if iterErr != nil {
		t.Fatal(iterErr)
	}
	slices.Sort(ownerIDs)
	if !slices.Equal(ownerIDs, []string{"a", "deleted-only", "z"}) || len(domains) != 2 {
		t.Fatalf("migrated owners=%q domains=%v keys=%q", ownerIDs, domains, keys)
	}
	candidate.recordPersistedSnapshot(1, bytesDisk, seq)
	finalSnapshot, _ := candidate.persistSnapshot()
	reopened, err := newVectorIndex(nil, options)
	if err != nil {
		t.Fatal(err)
	}
	if reason := reopened.loadPersistSnapshot(finalSnapshot); reason != "" {
		t.Fatalf("V3 reopen reason=%q", reason)
	}
	for domain, query := range [][]float32{{1, 0}, {0, 1}} {
		pin, err := reopened.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		results, _, searchErr := pin.SearchDomainV1(t.Context(), uint32(domain), query, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
		pin.Release()
		want := []string{"a", "z"}[domain]
		if searchErr != nil || len(results) != 1 || results[0].ID != want {
			t.Fatalf("domain=%d results=%+v err=%v want=%q", domain, results, searchErr, want)
		}
	}
}

func TestVectorIndexPartitionLiveByteCapacityRejectsBeforePublicationV1(t *testing.T) {
	const dimensions = 4096
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dimensions, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, dimensions)
	vector[0] = 1
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: vector}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.byteCapacity = 150 << 10
	revision := idx.partitionLive.revision
	err = idx.preflightVectorPartitionMutationLocked([]byte("high-dimensional"), vector)
	owners, nodes, afterRevision := len(idx.partitionLive.owners), idx.partitionLiveNodeCountLocked(), idx.partitionLive.revision
	idx.mu.Unlock()
	if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) {
		t.Fatalf("preflight err=%v, want capacity", err)
	}
	if owners != 0 || nodes != 0 || afterRevision != revision {
		t.Fatalf("rejected mutation published owners=%d nodes=%d revision=%d want=%d", owners, nodes, afterRevision, revision)
	}
}

func TestVectorIndexPartitionLivePinnedReaderAcrossRevisionV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 2, PartitionCount: 2, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}, {domain: 1, vector: []float32{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	_ = idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0})
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	_ = idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1})
	idx.mu.Unlock()
	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	oldPin.Release()
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
}

func TestVectorIndexPartitionLiveGenerationCutoverKeepsOldPinV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	oldManifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	reps := []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}
	if err := idx.bindVectorPartitionLiveV1(oldManifest, oldManifest.SourceGeneration, reps); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	retiredDomainEpoch := idx.partitionLive.domainEpochs[0]
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer oldPin.Release()

	newManifest := oldManifest
	newManifest.Generation = 8
	newManifest.SourceGeneration = 4
	newManifest.SourceChecksum = 6
	newCoverage := uint64(41)
	if err := idx.bindVectorPartitionLiveV1(newManifest, newCoverage, reps); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("cutover without exact source err=%v", err)
	}
	if pin, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest); err != nil {
		t.Fatalf("rejected cutover disturbed old generation: %v", err)
	} else {
		pin.Release()
	}

	idx.mu.Lock()
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = newCoverage
	idx.mu.Unlock()
	if err := idx.bindVectorPartitionLiveV1(newManifest, newCoverage, reps); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("retired generation err=%v", err)
	}
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(newManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	if status := newPin.StatusV1(); status.Generation != newManifest.Generation || status.Revision != 0 || status.Coverage != newCoverage || status.MutatedIDs != 0 || status.LiveIDs != 0 {
		t.Fatalf("new generation status=%+v", status)
	}
	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || newErr != nil || len(newResults) != 0 {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
	persisted := partitionLivePersistV1ForTest(idx)
	if persisted == nil || persisted.Generation != newManifest.Generation || len(persisted.Owners) != 0 || len(persisted.Domains) != 0 {
		t.Fatalf("persisted cutover=%+v", persisted)
	}
	idx.mu.Lock()
	if got := idx.partitionLive.domainEpochHighWater; got != retiredDomainEpoch {
		idx.mu.Unlock()
		t.Fatalf("rebound domain epoch high-water=%d want=%d", got, retiredDomainEpoch)
	}
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("next"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	newDomainEpoch := idx.partitionLive.domainEpochs[0]
	idx.mu.Unlock()
	if newDomainEpoch <= retiredDomainEpoch {
		t.Fatalf("rebound domain epoch=%d must exceed retired epoch=%d", newDomainEpoch, retiredDomainEpoch)
	}
}

func TestVectorIndexPartitionLiveGenerationRebindWaitsForCoordinatorPinV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, oldManifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), oldManifest); err != nil {
		t.Fatal(err)
	}
	oldPin, err := collection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(oldManifest)
	if err != nil {
		t.Fatal(err)
	}

	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	newManifest := VectorPartitionManifestV1{
		State: "building", Collection: collection.name, IndexName: def.Name,
		IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: oldManifest.Generation + 1, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}, BalancePolicy: "disjoint_v1",
		Placements: []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range rows {
		newManifest.Memberships = append(newManifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(VectorPartitionMembershipHomeV1)})
	}
	newManifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, newManifest, 7811, []VectorPartitionSearchAssetV1{{Source: source, Generation: newManifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}})
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	resources.Release()
	newManifest.Assets = assets
	newManifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(newManifest, nil); err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor, cfg.LeafSize, cfg.RepresentativeBudget = 2, 1, 1
	cfg.MaxDepth, cfg.MaxIterations, cfg.MaxVectors = 4, 8, len(rows)
	cfg.MaxDimensions, cfg.MaxRepresentatives, cfg.MaxScalarWork = 8, 32, 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), newManifest, []internalrouter.RouterPartitionV1{partition}, VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 7812, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	ready, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, newManifest.Generation)
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}

	transitionReached := make(chan struct{})
	continueTransition := make(chan struct{})
	var transitionOnce sync.Once
	restoreHook := setVectorPartitionLiveBeforeBindingTransitionHookForTest(func() {
		transitionOnce.Do(func() { close(transitionReached) })
		<-continueTransition
	})
	defer restoreHook()
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- collection.EnsureVectorPartitionLiveBindingV1(t.Context(), ready) }()
	select {
	case <-transitionReached:
	case <-time.After(5 * time.Second):
		oldPin.Release()
		t.Fatal("new generation did not reach binding transition")
	}
	close(continueTransition)
	coord := collection.collectionSchemaCoordinator()
	if coord == nil {
		oldPin.Release()
		t.Fatal("missing collection schema coordinator")
	}
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		if time.Now().After(deadline) {
			oldPin.Release()
			t.Fatal("generation rebind did not queue behind old coordinator pin")
		}
		runtime.Gosched()
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), oldManifest); err != nil {
		oldPin.Release()
		t.Fatalf("cold shard old binding: %v", err)
	}
	shardPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(oldManifest)
	if err != nil {
		oldPin.Release()
		t.Fatalf("cold shard old pin: %v", err)
	}
	if got, want := shardPin.StatusV1(), oldPin.StatusV1(); got.Generation != want.Generation || got.Revision != want.Revision || got.Coverage != want.Coverage {
		shardPin.Release()
		oldPin.Release()
		t.Fatalf("cold shard status=%+v want coordinator=%+v", got, want)
	}
	shardPin.Release()
	oldPin.Release()
	select {
	case err := <-ensureDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("generation rebind remained blocked after old coordinator release")
	}
	newPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	newPin.Release()
	if _, err := collection.AcquireVectorPartitionLiveSearchPinV1(oldManifest); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("retired generation remained searchable: %v", err)
	}
}

func TestVectorIndexPartitionLiveDropWaitsForCoordinatorPinV1(t *testing.T) {
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mgr := NewCollectionManager(database)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	collection, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	carrier := collection.registeredVectorIndex(def.Name)
	carrier.mu.RLock()
	coverage := carrier.sourceDocumentGeneration
	carrier.mu.RUnlock()
	manifest := VectorPartitionManifestV1{
		IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def), SourceGeneration: coverage,
		SourceChecksum: 1, SourceSchemaHash: 1, Generation: coverage + 1, DomainCount: 1, PartitionCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	carrier.setPartitionLiveCarrier(true)
	if err := carrier.bindVectorPartitionLiveV1(manifest, coverage, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	carrier.mu.Lock()
	carrier.partitionLive.bindingDurable = true
	carrier.mu.Unlock()
	coord := collection.collectionSchemaCoordinator()
	coord.registerPartitionLiveCarrier(carrier)
	if got := coord.partitionLiveCarrier(def.Name); got != carrier {
		t.Fatalf("registered carrier=%p want=%p", got, carrier)
	}
	pin, err := collection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := collection.DropVectorIndex(def.Name)
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		select {
		case err := <-done:
			pin.Release()
			t.Fatalf("vector index drop did not wait: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			pin.Release()
			t.Fatal("carrier removal did not queue behind coordinator pin")
		}
		runtime.Gosched()
	}
	if got := collection.registeredVectorIndex(def.Name); got != carrier {
		pin.Release()
		t.Fatalf("pinned carrier=%p want=%p", got, carrier)
	}
	shardPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		pin.Release()
		t.Fatalf("shard pin during carrier removal: %v", err)
	}
	shardPin.Release()
	pin.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("vector index drop remained blocked after coordinator release")
	}
	if got := collection.registeredVectorIndex(def.Name); got != nil {
		t.Fatalf("carrier remained registered: %p", got)
	}
}

func TestVectorIndexPartitionLiveUnregisterWaitsForCoordinatorPinV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	carrier := collection.registeredVectorIndex(def.Name)
	pin, err := collection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()

	done := make(chan struct{})
	go func() {
		collection.UnregisterVectorIndex(def.Name)
		close(done)
	}()
	coord := collection.collectionSchemaCoordinator()
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		select {
		case <-done:
			t.Fatal("vector index unregister did not wait")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("carrier unregister did not queue behind coordinator pin")
		}
		runtime.Gosched()
	}
	if got := collection.registeredVectorIndex(def.Name); got != carrier {
		t.Fatalf("pinned carrier=%p want=%p", got, carrier)
	}
	shardPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatalf("shard pin during carrier unregister: %v", err)
	}
	shardPin.Release()
	pin.Release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("vector index unregister remained blocked after coordinator release")
	}
	if got := collection.registeredVectorIndex(def.Name); got != nil {
		t.Fatalf("carrier remained registered: %p", got)
	}
}

func TestVectorIndexPartitionLiveUnregisterWaitsWithoutRegisteredCarrierV1(t *testing.T) {
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mgr := NewCollectionManager(database)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	collection, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte("a"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	coord := collection.collectionSchemaCoordinator()
	if coord == nil || coord.partitionLiveCarrier(def.Name) != nil {
		t.Fatal("fixture unexpectedly has a registered partition-live carrier")
	}

	coord.partitionLivePublishMu.RLock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		collection.UnregisterVectorIndex(def.Name)
		close(done)
	}()
	<-started
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		select {
		case <-done:
			coord.partitionLivePublishMu.RUnlock()
			t.Fatal("unregister crossed a no-carrier publication window")
		default:
		}
		if time.Now().After(deadline) {
			coord.partitionLivePublishMu.RUnlock()
			t.Fatal("unregister did not queue on the publication barrier")
		}
		runtime.Gosched()
	}
	if collection.registeredVectorIndex(def.Name) == nil {
		coord.partitionLivePublishMu.RUnlock()
		t.Fatal("index unregistered before publication window closed")
	}
	coord.partitionLivePublishMu.RUnlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("unregister remained blocked after publication window closed")
	}
	if got := collection.registeredVectorIndex(def.Name); got != nil {
		t.Fatalf("index remained registered: %p", got)
	}
}

func TestVectorIndexPartitionLiveUnchangedVectorDoesNotConsumeCapacityV1(t *testing.T) {
	for _, tc := range []struct {
		name     string
		encoding VectorIndexEncoding
		initial  []float32
		repeat   []float32
	}{
		{name: "fp32", encoding: VectorIndexEncodingFloat32, initial: []float32{1, .5}, repeat: []float32{1, .5}},
		{name: "int8 quantization equivalent", encoding: VectorIndexEncodingInt8, initial: []float32{1, .5}, repeat: []float32{1, .5001}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: tc.encoding, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
			if err != nil {
				t.Fatal(err)
			}
			manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
			if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
				t.Fatal(err)
			}
			idx.mu.Lock()
			idx.partitionLive.nodeCapacity = 1
			if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), tc.initial); err != nil {
				idx.mu.Unlock()
				t.Fatal(err)
			}
			if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), tc.initial); err != nil {
				idx.mu.Unlock()
				t.Fatal(err)
			}
			beforeRevision := idx.partitionLive.revision
			if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), tc.repeat); err != nil {
				idx.mu.Unlock()
				t.Fatalf("unchanged preflight: %v", err)
			}
			if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), tc.repeat); err != nil {
				idx.mu.Unlock()
				t.Fatal(err)
			}
			nodes := idx.partitionLiveNodeCountLocked()
			nodeIDBytes := idx.partitionLive.nodeIDBytes
			revision := idx.partitionLive.revision
			idx.mu.Unlock()
			if nodes != 1 || nodeIDBytes != uint64(len("doc")) || revision != beforeRevision+1 {
				t.Fatalf("nodes=%d node_id_bytes=%d revision=%d want nodes=1 bytes=%d revision=%d", nodes, nodeIDBytes, revision, len("doc"), beforeRevision+1)
			}
		})
	}
}

func TestVectorIndexPartitionLiveBatchRepeatedIDCapacityV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	beforeRevision := idx.partitionLive.revision
	idx.partitionLive.nodeCapacity = 2
	ids := [][]byte{[]byte("doc"), []byte("doc")}
	err = idx.preflightVectorPartitionMutationBatchLocked(ids, [][]float32{{.8, .2}, {0, 1}})
	if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) || idx.partitionLiveNodeCountLocked() != 1 || idx.partitionLive.revision != beforeRevision {
		idx.mu.Unlock()
		t.Fatalf("under-reserved batch err=%v nodes=%d revision=%d want capacity/nodes=1/revision=%d", err, idx.partitionLiveNodeCountLocked(), idx.partitionLive.revision, beforeRevision)
	}
	idx.partitionLive.nodeCapacity = 3
	ids = [][]byte{[]byte("doc"), []byte("doc"), []byte("doc")}
	vectors := [][]float32{{.8, .2}, {.8, .2}, {0, 1}}
	if err := idx.preflightVectorPartitionMutationBatchLocked(ids, vectors); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	for i := range ids {
		if err := idx.preflightVectorPartitionMutationLocked(ids[i], vectors[i]); err != nil {
			idx.mu.Unlock()
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked(ids[i], vectors[i]); err != nil {
			idx.mu.Unlock()
			t.Fatal(err)
		}
	}
	nodes := idx.partitionLiveNodeCountLocked()
	nodeIDBytes := idx.partitionLive.nodeIDBytes
	revision := idx.partitionLive.revision
	idx.mu.Unlock()
	if nodes != 3 || nodeIDBytes != 3*uint64(len("doc")) || revision != beforeRevision+3 {
		t.Fatalf("nodes=%d node_id_bytes=%d revision=%d want nodes=3 bytes=%d revision=%d", nodes, nodeIDBytes, revision, 3*len("doc"), beforeRevision+3)
	}
}

func TestVectorIndexPartitionLiveRepeatedUpdateCutoverKeepsPinnedViewV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	idx.sourceDocumentGeneration = manifest.SourceGeneration
	idx.sourceDocumentRootsValid = true
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.nodeCapacity = 3
	if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	oldDelta := oldPin.domains[0].index

	for _, vector := range [][]float32{{.8, .2}, {.6, .4}, {0, 1}} {
		idx.mu.Lock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), vector); err != nil {
			idx.mu.Unlock()
			oldPin.Release()
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), vector); err != nil {
			idx.mu.Unlock()
			oldPin.Release()
			t.Fatal(err)
		}
		idx.mu.Unlock()
	}

	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	oldPinStatus := oldPin.StatusV1()
	oldPin.Release()
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
	if newPin.StatusV1().Cutovers != 1 {
		t.Fatalf("cutovers=%d, want 1", newPin.StatusV1().Cutovers)
	}
	if oldDelta == newPin.domains[0].index || oldPinStatus.Cutovers != 0 {
		t.Fatal("cutover did not retire the old domain behind its pin")
	}
	idx.mu.RLock()
	wantNodeIDBytes := uint64(idx.partitionLiveNodeCountLocked() * len("doc"))
	gotNodeIDBytes := idx.partitionLive.nodeIDBytes
	idx.mu.RUnlock()
	if gotNodeIDBytes != wantNodeIDBytes {
		t.Fatalf("retained node ID bytes=%d want=%d", gotNodeIDBytes, wantNodeIDBytes)
	}
	snapshot, _ := idx.persistSnapshot()
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("restore reason=%q", reason)
	}
	if restored.partitionLive.nodeIDBytes != wantNodeIDBytes || restored.partitionLive.ownerIDBytes != uint64(len("doc")) {
		t.Fatalf("restored retained bytes node_ids=%d owner_ids=%d", restored.partitionLive.nodeIDBytes, restored.partitionLive.ownerIDBytes)
	}
}

func TestVectorIndexPartitionLiveByteCapacityCompactsBeforeRejectingV1(t *testing.T) {
	newIndex := func(t *testing.T) (*VectorIndex, VectorPartitionManifestV1) {
		t.Helper()
		idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
		if err != nil {
			t.Fatal(err)
		}
		manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
		if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
			t.Fatal(err)
		}
		idx.mu.Lock()
		idx.partitionLive.nodeCapacity = 1024
		idx.mu.Unlock()
		return idx, manifest
	}
	apply := func(t *testing.T, idx *VectorIndex, id string, vector []float32) {
		t.Helper()
		idx.mu.Lock()
		defer idx.mu.Unlock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("single replacement and held pin", func(t *testing.T) {
		idx, manifest := newIndex(t)
		apply(t, idx, "doc", []float32{1, 0})
		oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		defer oldPin.Release()

		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 50 << 10
		idx.mu.Unlock()
		apply(t, idx, "doc", []float32{.5, .5})
		apply(t, idx, "doc", []float32{0, 1})

		oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin.Release()
		if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
			t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
		}
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		if idx.partitionLive.cutovers != 1 || idx.partitionLiveNodeCountLocked() != 2 || idx.partitionLive.nodeIDBytes != 2*uint64(len("doc")) || idx.partitionLive.ownerIDBytes != uint64(len("doc")) {
			t.Fatalf("cutovers=%d nodes=%d node_id_bytes=%d owner_id_bytes=%d", idx.partitionLive.cutovers, idx.partitionLiveNodeCountLocked(), idx.partitionLive.nodeIDBytes, idx.partitionLive.ownerIDBytes)
		}
	})

	t.Run("batch replacement and unreclaimable cap", func(t *testing.T) {
		idx, manifest := newIndex(t)
		applyBatch := func(vectors [][]float32) error {
			ids := [][]byte{[]byte("a"), []byte("b")}
			idx.mu.Lock()
			defer idx.mu.Unlock()
			if err := idx.preflightVectorPartitionMutationBatchLocked(ids, vectors); err != nil {
				return err
			}
			for i := range ids {
				if err := idx.reconcileVectorPartitionMutationLocked(ids[i], vectors[i]); err != nil {
					return err
				}
			}
			return nil
		}
		if err := applyBatch([][]float32{{1, 0}, {.8, .2}}); err != nil {
			t.Fatal(err)
		}
		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 90 << 10
		idx.mu.Unlock()
		if err := applyBatch([][]float32{{.6, .4}, {.4, .6}}); err != nil {
			t.Fatal(err)
		}
		oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		defer oldPin.Release()
		if err := applyBatch([][]float32{{0, 1}, {1, 0}}); err != nil {
			t.Fatal(err)
		}
		oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{.4, .6}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin.Release()
		if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "b" || len(newResults) != 1 || newResults[0].ID != "a" {
			t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
		}

		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 60 << 10
		beforeRevision := idx.partitionLive.revision
		err = idx.preflightVectorPartitionMutationLocked([]byte("c"), []float32{1, 0})
		owners := len(idx.partitionLive.owners)
		nodes := idx.partitionLiveNodeCountLocked()
		nodeIDBytes := idx.partitionLive.nodeIDBytes
		ownerIDBytes := idx.partitionLive.ownerIDBytes
		cutovers := idx.partitionLive.cutovers
		afterRevision := idx.partitionLive.revision
		idx.mu.Unlock()
		if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) {
			t.Fatalf("unreclaimable preflight err=%v, want capacity", err)
		}
		if owners != 2 || nodes != 2 || nodeIDBytes != 2 || ownerIDBytes != 2 || cutovers != 2 || afterRevision != beforeRevision {
			t.Fatalf("rejected state owners=%d nodes=%d node_id_bytes=%d owner_id_bytes=%d cutovers=%d revision=%d want=%d", owners, nodes, nodeIDBytes, ownerIDBytes, cutovers, afterRevision, beforeRevision)
		}
	})
}

func TestVectorIndexPartitionLiveCutoverPublishesOncePerDomainV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 3, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.nodeCapacity = 4
	idx.mu.Unlock()
	update := func(id string, vector []float32) {
		t.Helper()
		idx.mu.Lock()
		defer idx.mu.Unlock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
	}
	update("a", []float32{1, 0})
	update("b", []float32{.9, .1})
	update("c", []float32{.8, .2})
	update("a", []float32{.7, .3})
	update("b", []float32{.6, .4}) // compacts three owners before this insert

	idx.mu.RLock()
	publications := idx.partitionLive.cutoverPublications
	cutovers := idx.partitionLive.cutovers
	idx.mu.RUnlock()
	if cutovers != 1 || publications != 1 {
		t.Fatalf("cutovers=%d publications=%d, want one publication for one rebuilt domain", cutovers, publications)
	}
}

func TestVectorPartitionLiveColumnGraphCarrierLoadsWithoutRebuildV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}}}
	_, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	rebuilds := 0
	restoreHook := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { rebuilds++ })
	defer restoreHook()

	missing, missingStatus, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || missing != nil || missingStatus.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot || rebuilds != 0 {
		t.Fatalf("missing carrier loaded=%v status=%+v rebuilds=%d err=%v", missing != nil, missingStatus, rebuilds, err)
	}
	source, _, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	carrier, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	carrier.setPartitionLiveCarrier(true)
	generation, state, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	carrier.recordSourceDocumentState(generation, state)
	if err := carrier.bindVectorPartitionLiveV1(manifest, generation, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	collection.registerVectorIndexCurrentCatalog(carrier)
	if status, err := carrier.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		t.Fatalf("persist carrier status=%+v err=%v", status, err)
	}
	collection.UnregisterVectorIndex(def.Name)
	loaded, status, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || loaded == nil || !status.Loaded || !loaded.isPartitionLiveCarrier() || rebuilds != 0 {
		t.Fatalf("loaded carrier=%v status=%+v rebuilds=%d err=%v", loaded != nil, status, rebuilds, err)
	}
	replayCollection, err := newCommandWALReplayCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if got := replayCollection.registeredVectorIndex(def.Name); got != loaded {
		t.Fatalf("replay manager carrier=%p want shared carrier=%p", got, loaded)
	}
	pin, err := replayCollection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	loaded.mu.RLock()
	pinnedState := loaded.sourceDocumentState
	loaded.mu.RUnlock()
	otherManifest := manifest
	otherManifest.IndexName = "other_embedding_graph"
	otherManifest.IndexDefinitionDigest = "other-definition"
	other, err := newVectorIndex(replayCollection, VectorIndexOptions{
		Name: otherManifest.IndexName, Field: def.Field, Metric: def.Metric, Dimensions: def.Dimensions,
		M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch,
	})
	if err != nil {
		t.Fatal(err)
	}
	other.recordSourceDocumentState(generation, pinnedState)
	if err := other.bindVectorPartitionLiveV1(otherManifest, generation, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	other.mu.Lock()
	other.partitionLive.bindingDurable = true
	other.mu.Unlock()
	replayCollection.registerVectorIndexCurrentCatalog(other)
	if replayCollection.vectorPartitionLiveCoordinatorBindingPinnedV1(otherManifest) {
		t.Fatal("index A coordinator pin authorized index B binding")
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(otherManifest.IndexName, otherManifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("index A coordinator pin authorized index B authority: %v", err)
	}
	pin.Release()
	replayCollection.UnregisterVectorIndex(otherManifest.IndexName)
	pin, err = replayCollection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorStatus := pin.StatusV1()
	replayIDs := [][]byte{[]byte("replayed")}
	replayDocuments := [][]byte{[]byte(`{"time_us":3,"kind":"vector","did":"replayed","embedding":[0.5,0.5]}`)}
	insertDone := make(chan error, 1)
	go func() {
		_, err := replayCollection.insertBatchWithCommandWALIntent(replayIDs, replayDocuments, false, nil, nil, insertBatchExecutionOptions{returnResultIDs: true})
		insertDone <- err
	}()
	coord := replayCollection.collectionSchemaCoordinator()
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		if time.Now().After(deadline) {
			pin.Release()
			t.Fatal("live mutation did not wait on coordinator pin")
		}
		runtime.Gosched()
	}
	select {
	case err := <-insertDone:
		t.Fatalf("mutation crossed coordinator pin: %v", err)
	default:
	}
	if currentState, ok := database.StateToken(); !ok || currentState != pinnedState {
		pin.Release()
		t.Fatalf("document publication crossed coordinator pin: state=%+v ok=%t want=%+v", currentState, ok, pinnedState)
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(def.Name, manifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); err != nil {
		t.Fatalf("coordinator-pinned authority: %v", err)
	}
	if coord.partitionLivePublishMu.TryLock() {
		coord.partitionLivePublishMu.Unlock()
		t.Fatal("live mutation publication lock crossed coordinator pin")
	}
	shardPin, err := replayCollection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if shardStatus := shardPin.StatusV1(); shardStatus.Revision != coordinatorStatus.Revision || shardStatus.Coverage != coordinatorStatus.Coverage {
		t.Fatalf("shard live status=%+v want coordinator status=%+v", shardStatus, coordinatorStatus)
	}
	shardPin.Release()
	pin.Release()
	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mutation did not complete after coordinator pin release")
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(def.Name, manifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("released coordinator pin retained authority: %v", err)
	}
	if err := replayCollection.reconcileVectorPartitionLiveReplay(replayIDs); err != nil {
		t.Fatal(err)
	}
	current := replayCollection.registeredVectorIndex(def.Name)
	if current == nil || current != loaded {
		t.Fatalf("atomic carrier handoff current=%p loaded=%p", current, loaded)
	}
	pin, err = current.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0.5, 0.5}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "replayed" || rebuilds != 0 {
		t.Fatalf("carrier-only replay results=%+v rebuilds=%d err=%v", results, rebuilds, searchErr)
	}

	if _, err := collection.Insert([]byte("c"), []byte(`{"time_us":3,"kind":"vector","did":"c","embedding":[0.3,0.7]}`)); err != nil {
		t.Fatal(err)
	}
	collection.UnregisterVectorIndex(def.Name)
	current, currentStatus, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || current == nil || !currentStatus.Loaded || rebuilds != 0 {
		t.Fatalf("current carrier loaded=%v status=%+v rebuilds=%d err=%v", current != nil, currentStatus, rebuilds, err)
	}
	pin, err = current.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr = pin.SearchDomainV1(t.Context(), 0, []float32{0.3, 0.7}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "c" || rebuilds != 0 {
		t.Fatalf("foreground carrier results=%+v rebuilds=%d err=%v", results, rebuilds, searchErr)
	}
}

func TestVectorPartitionLiveMissingCarrierAfterMutationFailsWithoutRebuildV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}}}
	_, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, _, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	rebuilds := 0
	restoreHook := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { rebuilds++ })
	defer restoreHook()

	if _, err := collection.Insert([]byte("d"), []byte(`{"time_us":4,"kind":"vector","did":"d","embedding":[0.5,0.5]}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err == nil {
		t.Fatal("missing carrier admitted after the immutable column graph became stale")
	}
	if rebuilds != 0 {
		t.Fatalf("request-path column graph rebuilds=%d, want 0", rebuilds)
	}
}

func TestVectorPartitionHNSWExcludesMoreThanTopKBeforeAdmissionV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	ordinals, err := vectorPartitionPreparedStableIDOrdinalsV1(view)
	if err != nil {
		t.Fatal(err)
	}
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 0, Dimensions: 3}, prepared: view, opened: 1, searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1, stableIDOrdinals: ordinals}
	t.Cleanup(func() { _ = searcher.Close() })
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3, ExcludedStableIDs: map[string]struct{}{"doc-a": {}, "doc-b": {}}}
	if _, scratchBytes, err := searcher.SearchPreflightV1(opts); err != nil || scratchBytes == 0 {
		t.Fatalf("preflight bytes=%d err=%v", scratchBytes, err)
	}
	results, metrics, err := searcher.SearchWithOptionsV1(t.Context(), []float32{1, 0, 0}, opts)
	if err != nil || len(results) != 1 || results[0].ID != "doc-c" || metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("results=%+v metrics=%+v err=%v", results, metrics, err)
	}
}

func TestVectorPartitionHNSWShadowedUpperBridgeRemainsTraversableV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.Levels = []uint16{1, 1, 1}
	input.MaxLayer = 1
	input.NormalizedVectors = []float32{
		0, 1, 0, 0,
		.70710677, .70710677, 0, 0,
		1, 0, 0, 0,
	}
	input.AdjacencyLayers = []columnHNSWSearchPackLayerInput{
		{Offsets: []uint64{0, 0, 0, 0}},
		{Offsets: []uint64{0, 1, 3, 4}, Neighbors: []uint32{1, 0, 2, 1}},
	}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	ordinals, err := vectorPartitionPreparedStableIDOrdinalsV1(view)
	if err != nil {
		t.Fatal(err)
	}
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 0, Dimensions: 3}, prepared: view, opened: 1, searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1, stableIDOrdinals: ordinals}
	t.Cleanup(func() { _ = searcher.Close() })
	baseOpts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 1}
	_, baseScratch, err := searcher.SearchPreflightV1(baseOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := baseOpts
	opts.ExcludedStableIDs = map[string]struct{}{"doc-b": {}}
	_, shadowScratch, err := searcher.SearchPreflightV1(opts)
	if err != nil || shadowScratch <= baseScratch {
		t.Fatalf("shadow preflight bytes=%d base=%d err=%v", shadowScratch, baseScratch, err)
	}
	results, metrics, err := searcher.SearchWithOptionsV1(t.Context(), []float32{1, 0, 0}, opts)
	if err != nil || len(results) != 1 || results[0].ID != "doc-c" || metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("results=%+v metrics=%+v err=%v", results, metrics, err)
	}
}

func TestVectorPartitionSearcherExcludesStaleBeforeTopKV1(t *testing.T) {
	searcher, err := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       1, PartitionID: 0, Dimensions: 2,
		IDs: []string{"stale", "fresh"}, Vectors: [][]float32{{1, 0}, {.8, .2}},
		Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, metrics, err := searcher.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{
		TopK: 1, EfSearch: 8, ExcludedStableIDs: map[string]struct{}{"stale": {}},
	})
	if err != nil || len(results) != 1 || results[0].ID != "fresh" || metrics.Candidates != 1 {
		t.Fatalf("results=%v metrics=%+v err=%v", results, metrics, err)
	}
}

func TestVectorIndexPartitionLiveRejectsNonAuthoritativeRoutingIdentityV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureWithPartitionsV1(t, nil, 2)
	defer database.Close()

	altered := manifest
	altered.DomainPacks = []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 0}}
	altered.Canonicalize()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), altered); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("cold altered binding err=%v want mismatch", err)
	}
	if idx := collection.registeredVectorIndex(def.Name); idx != nil {
		t.Fatalf("altered routing identity registered carrier=%p", idx)
	}

	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), altered); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("warm altered binding err=%v want mismatch", err)
	}
	if pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(altered); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		if pin != nil {
			pin.Release()
		}
		t.Fatalf("altered pin err=%v want mismatch", err)
	}

	collection.UnregisterVectorIndex(def.Name)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), altered); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("restored altered binding err=%v want mismatch", err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	for pack, wantDomain := range []uint32{0, 1} {
		if domain, ok := pin.DomainForPackV1(uint32(pack)); !ok || domain != wantDomain {
			t.Fatalf("pack=%d domain=%d ok=%t want=%d", pack, domain, ok, wantDomain)
		}
	}
}

func TestVectorIndexPartitionLiveFailedBindingPublicationUnregistersCarrierV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()

	publishErr := errors.New("binding publication failed")
	var restoreCatalogHook func()
	restoreBindingHook := setVectorPartitionLiveBeforeBindingPublicationHookForTest(func() {
		restoreCatalogHook = setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
			if ctx.Collection == collection.meta.Name && ctx.Stage == collectionCatalogLoadFaultMeta {
				return publishErr
			}
			return nil
		})
	})
	defer restoreBindingHook()
	defer func() {
		if restoreCatalogHook != nil {
			restoreCatalogHook()
		}
	}()

	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); !errors.Is(err, publishErr) {
		t.Fatalf("binding err=%v want %v", err, publishErr)
	}
	if got := collection.registeredVectorIndex(def.Name); got != nil {
		t.Fatalf("failed binding publication retained carrier=%p", got)
	}
}

func TestVectorIndexPartitionLiveFirstBindingConcurrentMutationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()

	publicationReached := make(chan struct{})
	continuePublication := make(chan struct{})
	var publicationOnce sync.Once
	restoreHook := setVectorPartitionLiveBeforeBindingPublicationHookForTest(func() {
		publicationOnce.Do(func() { close(publicationReached) })
		<-continuePublication
	})
	defer restoreHook()

	ensureDone := make(chan error, 1)
	go func() {
		ensureDone <- collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest)
	}()
	select {
	case <-publicationReached:
	case <-time.After(5 * time.Second):
		t.Fatal("first binding did not reach durable publication")
	}

	replacement, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutationDone := make(chan error, 1)
	mutationStarted := make(chan struct{})
	go func() {
		close(mutationStarted)
		matched, err := collection.Replace([]byte("a"), replacement)
		if err == nil && !matched {
			err = errors.New("replacement did not match")
		}
		mutationDone <- err
	}()
	<-mutationStarted
	select {
	case err := <-mutationDone:
		close(continuePublication)
		t.Fatalf("mutation acknowledged before first binding durability: %v", err)
	default:
	}
	if current, err := collection.Get([]byte("a")); err != nil || bytes.Contains(current, []byte(`"time_us":99`)) {
		close(continuePublication)
		t.Fatalf("mutation visible before first binding durability: document=%s err=%v", current, err)
	}
	close(continuePublication)
	select {
	case err := <-ensureDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first binding did not finish after concurrent mutation")
	}
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent mutation did not finish after durable first binding")
	}

	wantCoverage, _, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if err != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Coverage != wantCoverage {
		t.Fatalf("post-state results=%+v status=%+v wantCoverage=%d err=%v", results, pin.StatusV1(), wantCoverage, err)
	}
}

func TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1(t *testing.T) {
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR"); dir != "" {
		if os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_REPLAY_PUBLICATION_CRASH") == "1" {
			vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
			vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() {
				// The document, column, locator, and live carrier roots have been
				// accepted atomically. Crash before either in-memory candidate is
				// installed or the command handler can reconcile/finalize it.
				os.Exit(0)
			}
			vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
			database := openCollectionCommandWALDB(t, dir)
			database.Close()
			t.Fatal("replay completed without reaching the accepted-publication crash hook")
		}
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openCollectionCommandWALDB(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatalf("reload checkpointed binding: %v", err)
		}
		maxBaselineLSN := uint64(0)
		for _, frame := range collectionCommandWALFrames(t, dir) {
			if frame.LSN > maxBaselineLSN {
				maxBaselineLSN = frame.LSN
			}
		}
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		replayedMutation := false
		for _, frame := range collectionCommandWALFrames(t, dir) {
			if frame.LSN <= maxBaselineLSN {
				continue
			}
			if frame.Kind == commitlog.CommandKindCollectionPersistPartitionLive {
				t.Fatal("post-checkpoint replay unexpectedly depended on a partition-live binding command")
			}
			switch frame.Kind {
			case commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID,
				commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
				replayedMutation = true
			}
		}
		if !replayedMutation {
			t.Fatal("missing post-checkpoint document mutation frame")
		}
		os.Exit(0) // Process loss: preserve the checkpoint and replay only the acknowledged document WAL frame.
	}
	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		database.Close()
		t.Fatalf("first binding was not durably admitted: %v", err)
	}
	pin.Release()
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	maxBaselineLSN := uint64(0)
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > maxBaselineLSN {
			maxBaselineLSN = frame.LSN
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	replayedMutation := false
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN <= maxBaselineLSN {
			continue
		}
		if frame.Kind == commitlog.CommandKindCollectionPersistPartitionLive {
			t.Fatal("post-checkpoint replay unexpectedly depended on a partition-live binding command")
		}
		switch frame.Kind {
		case commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID,
			commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
			replayedMutation = true
		}
	}
	if !replayedMutation {
		t.Fatal("missing post-checkpoint document mutation frame")
	}
	replayCrashCmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1$")
	replayCrashCmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_REPLAY_PUBLICATION_CRASH=1",
	)
	if output, err := replayCrashCmd.CombinedOutput(); err != nil {
		t.Fatalf("replay publication crash helper: %v\n%s", err, output)
	}

	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatalf("recover durable binding after document replay: %v", err)
	}
	validatedManifest, authorityToken, err := reopened.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	authorityToken.Release()
	if validatedManifest.IntegrityDigest != reopenedManifest.IntegrityDigest {
		t.Fatal("live recovery authority manifest changed")
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	recoveredPin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredPin.Release()
	recovered, _, recoveredErr := recoveredPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if recoveredErr != nil || len(recovered) != 1 || recovered[0].ID != "a" || recoveredPin.StatusV1().Revision != 1 || recoveredPin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v err=%v", recovered, recoveredPin.StatusV1(), recoveredErr)
	}
}

func TestVectorIndexPartitionLiveForegroundDurablePublicationCrashV1(t *testing.T) {
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_FOREGROUND_CRASH_DIR"); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openVectorPartitionLiveDurableDBV1(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatal(err)
		}
		vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
		vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() {
			// The durable foreground command and the document, column, locator,
			// and live-carrier roots are accepted together. Crash before the
			// in-memory candidate install or ordinary reconciliation.
			os.Exit(0)
		}
		vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		database.Close()
		t.Fatal("foreground replacement completed without reaching the accepted-publication crash hook")
	}

	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{
		CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	if database.ResolvedProfile() != backenddb.ProfileCommandWALDurable {
		database.Close()
		t.Fatalf("profile=%s want %s", database.ResolvedProfile(), backenddb.ProfileCommandWALDurable)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveForegroundDurablePublicationCrashV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_FOREGROUND_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("foreground publication crash helper: %v\n%s", err, output)
	}

	reopenedDB := openVectorPartitionLiveDurableDBV1(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatal(err)
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveForegroundRelaxedCheckpointCrashV1(t *testing.T) {
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_RELAXED_CHECKPOINT_CRASH_DIR"); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openCollectionCommandWALDB(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatal(err)
		}
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Process loss after a successful checkpoint, before Close can persist anything else.
	}

	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveForegroundRelaxedCheckpointCrashV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_RELAXED_CHECKPOINT_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("foreground relaxed checkpoint crash helper: %v\n%s", err, output)
	}

	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatal(err)
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveForegroundDurableInstallsOnceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{
		CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	carrier := collection.registeredVectorIndex(manifest.IndexName)
	replacement, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
		t.Fatalf("replacement matched=%v err=%v", matched, err)
	}
	if current := collection.registeredVectorIndex(manifest.IndexName); current != carrier {
		t.Fatalf("accepted handoff replaced carrier: before=%p after=%p", carrier, current)
	}
	carrier.mu.RLock()
	for domain, delta := range carrier.partitionLive.domains {
		delta.mu.RLock()
		undo := delta.partitionLiveMutationUndo
		delta.mu.RUnlock()
		if undo != nil {
			carrier.mu.RUnlock()
			t.Fatalf("accepted domain %d retained transaction undo", domain)
		}
	}
	carrier.mu.RUnlock()
	expectedCoverage, _, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("results=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveReplayHandoffBlocksOrdinaryPinV2(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	replace := func(timeUS int64, vector []float32) error {
		document, err := json.Marshal(map[string]any{
			"time_us": timeUS, "kind": "vector", "did": "a", "embedding": vector,
		})
		if err != nil {
			return err
		}
		matched, err := collection.Replace([]byte("a"), document)
		if err == nil && !matched {
			return errors.New("replacement did not match")
		}
		return err
	}
	if err := replace(98, []float32{0, 1}); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	continueCommit := make(chan struct{})
	var enterOnce sync.Once
	vectorPartitionLiveReplayBeforeDomainCommitHookV2.Lock()
	vectorPartitionLiveReplayBeforeDomainCommitHookV2.fn = func() {
		enterOnce.Do(func() { close(entered) })
		<-continueCommit
	}
	vectorPartitionLiveReplayBeforeDomainCommitHookV2.Unlock()
	defer func() {
		vectorPartitionLiveReplayBeforeDomainCommitHookV2.Lock()
		vectorPartitionLiveReplayBeforeDomainCommitHookV2.fn = nil
		vectorPartitionLiveReplayBeforeDomainCommitHookV2.Unlock()
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(continueCommit) }) }
	defer release()

	mutationDone := make(chan error, 1)
	go func() { mutationDone <- replace(99, []float32{1, 0}) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("accepted handoff did not reach domain commit")
	}
	type pinResult struct {
		pin *VectorIndexPartitionLiveSearchPinV1
		err error
	}
	pinDone := make(chan pinResult, 1)
	go func() {
		pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
		pinDone <- pinResult{pin: pin, err: err}
	}()
	select {
	case result := <-pinDone:
		if result.pin != nil {
			result.pin.Release()
		}
		release()
		t.Fatalf("ordinary pin crossed incomplete handoff: %v", result.err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mutation remained blocked after handoff release")
	}
	select {
	case result := <-pinDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.pin.StatusV1().Revision != 2 {
			result.pin.Release()
			t.Fatalf("pin revision=%d want=2", result.pin.StatusV1().Revision)
		}
		results, _, err := result.pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
		result.pin.Release()
		if err != nil || len(results) != 1 || results[0].ID != "a" {
			t.Fatalf("post-handoff results=%+v err=%v", results, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary pin remained blocked after handoff")
	}
}

func TestVectorIndexPartitionLiveReplayAcceptedFailureInvalidatesV2(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	carrier := collection.registeredVectorIndex(manifest.IndexName)
	vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
	vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() {
		carrier.mu.Lock()
		carrier.mutationSeq++
		carrier.dirtyMeta = true
		carrier.mu.Unlock()
	}
	vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
	defer func() {
		vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
		vectorPartitionLiveReplayAfterAcceptedHookV1.fn = nil
		vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
	}()
	document, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if matched, err := collection.Replace([]byte("a"), document); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("accepted failure matched=%v err=%v", matched, err)
	}
	accepted, err := collection.Get([]byte("a"))
	if err != nil || !bytes.Contains(accepted, []byte(`"time_us":99`)) {
		t.Fatalf("accepted document=%s err=%v", accepted, err)
	}
	if carrier.hasValidSourceDocumentRoots() {
		t.Fatal("accepted handoff failure retained valid carrier authority")
	}
}

func TestVectorIndexPartitionLiveAlternatesCollectionManagerDomainsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name         string
		withOrdinary bool
	}{
		{name: "partition_live_only"},
		{name: "partition_live_and_native", withOrdinary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinaryDef := VectorIndexDefinition{
				Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine,
				Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8, Strategy: VectorIndexStrategyNativeRuntime,
			}
			var extra []VectorIndexDefinition
			if tc.withOrdinary {
				extra = []VectorIndexDefinition{ordinaryDef}
			}
			_, database, collectionA, _, manifest := newVectorPartitionLiveProductionFixtureWithIndexesV1(t, extra)
			defer database.Close()
			var ordinaryA *VectorIndex
			if tc.withOrdinary {
				ordinaryDef, _ = findVectorIndex(collectionA.Meta().VectorIndexes, ordinaryDef.Name)
				if _, err := collectionA.RebuildVectorIndex(ordinaryDef.Name); err != nil {
					t.Fatal(err)
				}
				ordinaryA = collectionA.registeredVectorIndex(ordinaryDef.Name)
				if ordinaryA == nil {
					t.Fatal("manager A ordinary native index is not loaded")
				}
			}
			if err := collectionA.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
				t.Fatal(err)
			}
			collectionB, err := NewCollectionManager(database).OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			var ordinaryB *VectorIndex
			if tc.withOrdinary {
				var loadStatus VectorIndexLoadStatus
				ordinaryB, loadStatus, err = collectionB.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(ordinaryDef))
				if err != nil || ordinaryB == nil || !loadStatus.Loaded {
					t.Fatalf("manager B ordinary native load=%v status=%+v err=%v", ordinaryB != nil, loadStatus, err)
				}
			}
			replace := func(collection *Collection, sequence int64, vector []float32) {
				t.Helper()
				replacement, err := json.Marshal(map[string]any{
					"time_us": sequence, "kind": "vector", "did": "a", "embedding": vector,
				})
				if err != nil {
					t.Fatal(err)
				}
				if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
					t.Fatalf("replacement %d matched=%v err=%v", sequence, matched, err)
				}
			}
			assertRevision := func(collection *Collection, wantRevision uint64, query []float32) {
				t.Helper()
				pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Release()
				results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, query, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
				if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != wantRevision {
					t.Fatalf("revision=%d results=%+v status=%+v err=%v", wantRevision, results, pin.StatusV1(), searchErr)
				}
			}

			replace(collectionB, 1, []float32{0.2, 1})
			assertRevision(collectionB, 1, []float32{0.2, 1})
			replace(collectionA, 2, []float32{1, 0.2})
			if ordinaryA != nil && ordinaryA.hasValidSourceDocumentRoots() {
				t.Fatal("manager A stale ordinary native index was not invalidated before rebuild")
			}
			assertRevision(collectionA, 2, []float32{1, 0.2})
			replace(collectionB, 3, []float32{0.3, 1})
			if ordinaryB != nil && ordinaryB.hasValidSourceDocumentRoots() {
				t.Fatal("manager B stale ordinary native index was not invalidated before rebuild")
			}
			assertRevision(collectionB, 3, []float32{0.3, 1})
			collectionA.invalidateOtherVectorIndexDocumentCoverage()
			assertRevision(collectionB, 3, []float32{0.3, 1})
		})
	}
}

func openVectorPartitionLiveDurableDBV1(t testing.TB, dir string) *backenddb.DB {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{
		Dir: dir, DisableBackgroundPrune: true, CommandWAL: true,
		ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	if err != nil {
		t.Fatalf("Open command WAL durable DB: %v", err)
	}
	return database
}

func TestVectorPartitionLiveReplayMutationCoalescingV1(t *testing.T) {
	vector := []float32{0, 1}
	declared := func(id string) columnDeclaredRow {
		return columnDeclaredRow{ID: []byte(id), Values: []columnDeclaredValue{{
			Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector,
		}}}
	}
	for _, tc := range []struct {
		name  string
		input columnWritePublishInput
		ids   []string
		live  []bool
	}{
		{name: "ordinary_insert", input: columnWritePublishInput{operation: ColumnPublishOperationInsert, declaredRows: []columnDeclaredRow{declared("insert")}}, ids: []string{"insert"}, live: []bool{true}},
		{name: "ordinary_update", input: columnWritePublishInput{operation: ColumnPublishOperationUpdate, declaredRows: []columnDeclaredRow{declared("update")}}, ids: []string{"update"}, live: []bool{true}},
		{name: "delete", input: columnWritePublishInput{operation: ColumnPublishOperationDelete, documents: []columnWriteDocument{{ID: []byte("delete")}}}, ids: []string{"delete"}, live: []bool{false}},
		{
			name: "source_delete_then_insert_wins",
			input: columnWritePublishInput{
				operation:             ColumnPublishOperationInsert,
				sourceDeleteDocuments: []columnWriteDocument{{ID: []byte("same")}, {ID: []byte("gone")}},
				declaredRows:          []columnDeclaredRow{declared("same")},
			},
			ids: []string{"same", "gone"}, live: []bool{true, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, vectors, err := vectorPartitionLiveReplayMutationsV1(tc.input, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != len(tc.ids) || len(vectors) != len(tc.live) {
				t.Fatalf("ids=%q vectors=%v", ids, vectors)
			}
			for i := range ids {
				if string(ids[i]) != tc.ids[i] || (vectors[i] != nil) != tc.live[i] {
					t.Fatalf("effect[%d]=%q/%v want=%q/live=%t", i, ids[i], vectors[i], tc.ids[i], tc.live[i])
				}
			}
		})
	}
}

func newVectorPartitionLiveProductionFixtureV1(t *testing.T, openOptions ...backenddb.Options) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	return newVectorPartitionLiveProductionFixtureWithIndexesV1(t, nil, openOptions...)
}

func newVectorPartitionLiveProductionFixtureWithIndexesV1(t *testing.T, extraVectorIndexes []VectorIndexDefinition, openOptions ...backenddb.Options) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	return newVectorPartitionLiveProductionFixtureWithPartitionsV1(t, extraVectorIndexes, 1, openOptions...)
}

func newVectorPartitionLiveProductionFixtureWithPartitionsV1(t *testing.T, extraVectorIndexes []VectorIndexDefinition, partitionCount uint32, openOptions ...backenddb.Options) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	t.Helper()
	if partitionCount == 0 || partitionCount > 2 {
		t.Fatalf("unsupported partition count %d", partitionCount)
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0}},
		{id: "b", vector: []float32{.8, .2}},
		{id: "c", vector: []float32{0, 1}},
	}
	dir, database, collection, def := openColumnGraphTypedColumnVectorTestCollectionWithIndexes1782(t, 2, 2, rows, extraVectorIndexes, openOptions...)
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		database.Close()
		t.Fatal(err)
	}
	source, sourceRows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		State: "building", Collection: collection.name, IndexName: def.Name,
		IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum,
		SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: partitionCount, DomainCount: partitionCount,
		BalancePolicy: "disjoint_v1",
	}
	partitions := make([]internalrouter.RouterPartitionV1, partitionCount)
	assetInputs := make([]VectorPartitionSearchAssetV1, partitionCount)
	for partitionID := range partitionCount {
		manifest.DomainPacks = append(manifest.DomainPacks, VectorPartitionDomainPackV1{DomainID: partitionID, PackID: partitionID})
		manifest.Placements = append(manifest.Placements, VectorPartitionPlacementV1{PartitionID: partitionID, GroupID: "raft-" + strconv.FormatUint(uint64(partitionID), 10)})
		partitions[partitionID].PartitionID = partitionID
		assetInputs[partitionID] = VectorPartitionSearchAssetV1{Source: source, Generation: manifest.Generation, PartitionID: partitionID, Dimensions: def.Dimensions}
	}
	for _, row := range sourceRows {
		partitionID := uint32(row.VectorOrdinal % uint64(partitionCount))
		manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: partitionID})
		partitions[partitionID].Vectors = append(partitions[partitionID].Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 7801, assetInputs)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	defer resources.Release()
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		database.Close()
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativeBudget = len(partitions)
	cfg.MaxDepth = 4
	cfg.MaxIterations = 8
	cfg.MaxVectors = len(sourceRows)
	cfg.MaxDimensions = 8
	cfg.MaxRepresentatives = 32
	cfg.MaxScalarWork = 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), manifest, partitions, VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 7802, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	ready, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	return dir, database, collection, def, ready
}
