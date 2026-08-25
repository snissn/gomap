package collections

import (
	"bytes"
	"fmt"
	"testing"
)

func TestColumnManifestMutationDeltaRepeatedPublishScalesWithChanges(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}

	var (
		currentIdentity *ColumnManifestIdentity
		currentRecords  []columnManifestRecord
		cumulativeBytes int64
	)
	const publishes = 32
	for generation := uint64(1); generation <= publishes; generation++ {
		asset := testColumnPublishPreparedAssetM10A()
		asset.Ref.Generation = generation
		asset.Ref.PartID = generation
		asset.Ref.FileID = uint32(generation)
		asset.GenerationID = generation
		asset.PublishID = generation
		manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
			Collection:             "events",
			ColumnStore:            *cfg,
			Operation:              ColumnPublishOperationInsert,
			AppliedCommandLSN:      100 + generation,
			CurrentManifest:        currentIdentity,
			CurrentManifestRecords: currentRecords,
			Prepared: ColumnPublishPreparedAssets{
				Assets:             []ColumnPreparedAsset{asset},
				RowCount:           10,
				ColumnPayloadBytes: asset.Bytes,
			},
		})
		if err != nil {
			t.Fatalf("encode generation %d: %v", generation, err)
		}

		mutations, err := buildColumnManifestMutationDelta(currentRecords, manifest.Records)
		if err != nil {
			t.Fatalf("build mutation generation %d: %v", generation, err)
		}
		if got, want := len(mutations), 2; got != want { // changed header + one new asset
			t.Fatalf("generation %d mutations=%d want %d", generation, got, want)
		}
		cumulativeBytes += columnManifestMutationBytes(mutations)

		oracle, err := applyColumnManifestMutationDelta(currentRecords, mutations)
		if err != nil {
			t.Fatalf("apply generation %d: %v", generation, err)
		}
		if !equalColumnManifestRecords(oracle, manifest.Records) {
			t.Fatalf("generation %d mutation result differs from full-snapshot oracle", generation)
		}
		if got := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
			Collection:        "events",
			ColumnStore:       *cfg,
			Operation:         ColumnPublishOperationInsert,
			AppliedCommandLSN: 100 + generation,
		}, generation, oracle); got != manifest.Identity.Checksum {
			t.Fatalf("generation %d checksum=%d want %d", generation, got, manifest.Identity.Checksum)
		}

		identity := manifest.Identity
		currentIdentity = &identity
		currentRecords = manifest.Records
	}

	lastMutationBytes := columnManifestMutationBytes(mustBuildColumnManifestMutationDelta(t,
		currentRecords[:len(currentRecords)-1], currentRecords))
	if cumulativeBytes > int64(publishes)*lastMutationBytes*2 {
		t.Fatalf("cumulative mutation bytes=%d scale with retained history; per-publish bound=%d", cumulativeBytes, lastMutationBytes)
	}
}

func TestColumnManifestMutationDeltaReplacementAndRemoval(t *testing.T) {
	base := []columnManifestRecord{
		{key: []byte("a"), value: []byte("one")},
		{key: []byte("b"), value: []byte("two")},
		{key: []byte("c"), value: []byte("three")},
	}
	next := []columnManifestRecord{
		{key: []byte("a"), value: []byte("one")},
		{key: []byte("b"), value: []byte("replacement")},
		{key: []byte("d"), value: []byte("four")},
	}
	mutations := mustBuildColumnManifestMutationDelta(t, base, next)
	if got, want := len(mutations), 3; got != want {
		t.Fatalf("mutations=%d want %d", got, want)
	}
	if mutations[0].deleted || string(mutations[0].record.key) != "b" || string(mutations[0].record.value) != "replacement" {
		t.Fatalf("replacement mutation=%+v", mutations[0])
	}
	if !mutations[1].deleted || string(mutations[1].record.key) != "c" {
		t.Fatalf("removal mutation=%+v", mutations[1])
	}
	if mutations[2].deleted || string(mutations[2].record.key) != "d" {
		t.Fatalf("addition mutation=%+v", mutations[2])
	}
	got, err := applyColumnManifestMutationDelta(base, mutations)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !equalColumnManifestRecords(got, next) {
		t.Fatalf("applied=%v want=%v", formatColumnManifestRecords(got), formatColumnManifestRecords(next))
	}
}

func TestColumnManifestMutationDeltaIteratorPublishesDurableTombstone(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 42}
	delta := ColumnManifestRootDelta{
		RootName:       collectionColumnManifestRootName("events"),
		BaseRootID:     7,
		StoragePolicy:  RootStorageFast,
		Identity:       identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(identity),
		Mutations: []columnManifestMutation{
			{record: columnManifestRecord{key: []byte("removed")}, deleted: true},
			{record: columnManifestRecord{key: []byte("replacement"), value: []byte("value")}},
		},
		MutationDelta: true,
	}
	ordered, err := delta.OrderedRootDeltaPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootDeltaPublishInput: %v", err)
	}
	defer func() { _ = ordered.Iter.Close() }()
	var sawDelete, sawReplacement bool
	for ordered.Iter.Valid() {
		switch string(ordered.Iter.UnsafeKey()) {
		case "removed":
			sawDelete = ordered.Iter.IsDeleted()
		case "replacement":
			sawReplacement = !ordered.Iter.IsDeleted() && string(ordered.Iter.UnsafeValue()) == "value"
		}
		ordered.Iter.Next()
	}
	if !sawDelete || !sawReplacement {
		t.Fatalf("mutation iterator delete=%t replacement=%t", sawDelete, sawReplacement)
	}
}

func TestColumnManifestMutationDeltaProductionRepeatedPublishReopen(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)

	var firstMutationBytes, lastLogicalBytes int64
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("e%d", i)
		doc := fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i+1, i)
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(doc)}); err != nil {
			t.Fatalf("InsertBatch %d: %v", i, err)
		}
		stats := col.LastInsertStats()
		candidateChildren := stats.ColumnPublishFinalizeCandidateVisibleBaseClone +
			stats.ColumnPublishFinalizeCandidateInheritedFilter +
			stats.ColumnPublishFinalizeCandidateFreshCapture +
			stats.ColumnPublishFinalizeCandidateClosureAssemble +
			stats.ColumnPublishFinalizeCandidateVisibleClone +
			stats.ColumnPublishFinalizeCandidateCOWPrepare +
			stats.ColumnPublishFinalizeCandidateOther
		if candidateChildren != stats.ColumnPublishFinalizeCandidateBuild {
			t.Fatalf("publish %d candidate child timings=%s want additive total=%s", i, candidateChildren, stats.ColumnPublishFinalizeCandidateBuild)
		}
		work := stats.ColumnPublishFinalizeCandidateResourceWork
		if work.AppendOnlyFastPath == 0 || work.NewlyAdmittedObligations == 0 {
			t.Fatalf("publish %d did not certify append-only resource admission: %+v", i, work)
		}
		// The retained semantic closure grows every generation, but candidate
		// construction may visit only this root-local mutation plus the
		// persistent-index path for each newly admitted obligation.
		if work.SourceObligationsInspected > work.NewlyAdmittedObligations || work.CopiedObligations != 0 {
			t.Fatalf("publish %d rescanned/copied retained obligation history: %+v", i, work)
		}
		if work.RetainedIndexNodeVisits > work.NewlyAdmittedObligations*16 || work.RetainedIndexNodeCopies > work.NewlyAdmittedObligations*16 {
			t.Fatalf("publish %d persistent-index work exceeds mutation-local depth bound: %+v", i, work)
		}
		if work.PhysicalHandleCopies > 12 {
			t.Fatalf("publish %d physical handle copies=%d grow with retained history: %+v", i, work.PhysicalHandleCopies, work)
		}
		if work.PhysicalEntryLookupComparisons > 128 {
			t.Fatalf("publish %d physical lookup work is unbounded: %+v", i, work)
		}
		if stats.ColumnPublishManifestMutationRecords == 0 || stats.ColumnPublishManifestMutationBytes == 0 {
			t.Fatalf("publish %d missing mutation accounting: %+v", i, stats)
		}
		if stats.ColumnPublishManifestMutationRecords > stats.ColumnPublishPreparedAssets+2 {
			t.Fatalf("publish %d mutation records=%d exceed prepared+header+identity=%d", i, stats.ColumnPublishManifestMutationRecords, stats.ColumnPublishPreparedAssets+2)
		}
		if i == 0 {
			firstMutationBytes = stats.ColumnPublishManifestMutationBytes
		} else if stats.ColumnPublishManifestMutationBytes > firstMutationBytes*2 {
			t.Fatalf("publish %d mutation bytes=%d scale with retained history; first=%d", i, stats.ColumnPublishManifestMutationBytes, firstMutationBytes)
		}
		if stats.ColumnPublishManifestBytes <= lastLogicalBytes {
			t.Fatalf("publish %d logical manifest bytes=%d want growth beyond %d", i, stats.ColumnPublishManifestBytes, lastLogicalBytes)
		}
		lastLogicalBytes = stats.ColumnPublishManifestBytes
	}
	lastLSN := d.State().AppliedCommandLSN
	assertColumnManifestStateM10B(t, col, 8, lastLSN)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopenedDB.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopenedDB)
	assertColumnManifestStateM10B(t, reopened, 8, lastLSN)
	for i := 0; i < 8; i++ {
		assertCollectionDocument(t, reopened, fmt.Sprintf("e%d", i), fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i+1, i))
	}
}

func BenchmarkColumnManifestRootDeltaMaterializationScaling(b *testing.B) {
	current := make([]columnManifestRecord, 1025)
	current[0] = columnManifestRecord{key: []byte("header"), value: []byte("generation-1")}
	for i := 1; i < len(current); i++ {
		current[i] = columnManifestRecord{
			key:   []byte(fmt.Sprintf("part/%08d", i)),
			value: bytes.Repeat([]byte{byte(i)}, 128),
		}
	}
	next := cloneColumnManifestRecords(current)
	next[0].value = []byte("generation-2")
	for i := 0; i < 5; i++ {
		next = append(next, columnManifestRecord{
			key:   []byte(fmt.Sprintf("part/%08d", len(current)+i)),
			value: bytes.Repeat([]byte{byte(i + 1)}, 128),
		})
	}
	mutations := mustBuildColumnManifestMutationDelta(b, current, next)
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 42}
	base := ColumnManifestRootDelta{
		RootName:       collectionColumnManifestRootName("events"),
		BaseRootID:     7,
		StoragePolicy:  RootStorageFast,
		Identity:       identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(identity),
		Records:        next,
	}
	cases := []struct {
		name  string
		delta ColumnManifestRootDelta
	}{
		{name: "full_snapshot", delta: base},
		{name: "mutation_delta", delta: func() ColumnManifestRootDelta {
			delta := base
			delta.Mutations = mutations
			delta.MutationDelta = true
			return delta
		}()},
	}
	b.Run("iterator", func(b *testing.B) {
		for _, tc := range cases {
			b.Run(tc.name, func(b *testing.B) {
				b.ReportAllocs()
				publishedRecords := columnManifestRootPublishedRecordCount(tc.delta)
				publishedBytes := columnManifestRootPublishedBytes(tc.delta)
				b.ReportMetric(float64(publishedRecords), "published_records/op")
				b.ReportMetric(float64(publishedBytes), "published_B/op")
				for i := 0; i < b.N; i++ {
					ordered, err := tc.delta.OrderedRootDeltaPublishInput()
					if err != nil {
						b.Fatal(err)
					}
					_ = ordered.Iter.Close()
				}
			})
		}
	})
	b.Run("batch", func(b *testing.B) {
		for _, tc := range cases {
			b.Run(tc.name, func(b *testing.B) {
				b.ReportAllocs()
				publishedRecords := columnManifestRootPublishedRecordCount(tc.delta)
				publishedBytes := columnManifestRootPublishedBytes(tc.delta)
				b.ReportMetric(float64(publishedRecords), "published_records/op")
				b.ReportMetric(float64(publishedBytes), "published_B/op")
				for i := 0; i < b.N; i++ {
					_, cleanup, err := tc.delta.OrderedRootDeltaBatchPublishInput()
					if err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		}
	})
}

func mustBuildColumnManifestMutationDelta(t testing.TB, current, next []columnManifestRecord) []columnManifestMutation {
	t.Helper()
	mutations, err := buildColumnManifestMutationDelta(current, next)
	if err != nil {
		t.Fatalf("buildColumnManifestMutationDelta: %v", err)
	}
	return mutations
}

func applyColumnManifestMutationDelta(current []columnManifestRecord, mutations []columnManifestMutation) ([]columnManifestRecord, error) {
	if err := validateSortedUniqueColumnManifestRecords(current, "current"); err != nil {
		return nil, err
	}
	byKey := make(map[string]columnManifestRecord, len(current)+len(mutations))
	for _, record := range current {
		byKey[string(record.key)] = columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)}
	}
	for i, mutation := range mutations {
		if len(mutation.record.key) == 0 {
			return nil, fmt.Errorf("mutation[%d] has empty key", i)
		}
		if i > 0 && bytes.Compare(mutations[i-1].record.key, mutation.record.key) >= 0 {
			return nil, fmt.Errorf("mutations are not strictly sorted at index %d", i)
		}
		key := string(mutation.record.key)
		if mutation.deleted {
			delete(byKey, key)
			continue
		}
		byKey[key] = columnManifestRecord{key: bytes.Clone(mutation.record.key), value: bytes.Clone(mutation.record.value)}
	}
	result := make([]columnManifestRecord, 0, len(byKey))
	for _, record := range byKey {
		result = append(result, record)
	}
	sortColumnManifestRecords(result)
	return result, nil
}

func equalColumnManifestRecords(a, b []columnManifestRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].key, b[i].key) || !bytes.Equal(a[i].value, b[i].value) {
			return false
		}
	}
	return true
}

func formatColumnManifestRecords(records []columnManifestRecord) []string {
	out := make([]string, len(records))
	for i, record := range records {
		out[i] = fmt.Sprintf("%s=%s", record.key, record.value)
	}
	return out
}
