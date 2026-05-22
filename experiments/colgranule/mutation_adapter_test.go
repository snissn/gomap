package colgranule

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestColumnMutationAdapterReplayAndJSONBenchParity(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	opts, err := JSONBenchColumnPartOptions(ds, 32)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	updates := map[int64]map[string]int64{
		5: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][40],
			"time_us":                ds.Columns["time_us"][5] - 10_000_000,
		},
		25: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][41],
			"time_us":                ds.Columns["time_us"][25] + 20_000_000,
		},
	}
	for _, update := range updates {
		update["hour_of_day"] = unixMicroHour(update["time_us"])
	}
	deletes := []int64{10, 11}

	dir := t.TempDir()
	workspace, adapter := testColumnMutationAdapter(t, dir, opts, ds.Dictionaries)
	defer workspace.Close()
	baseResult, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{
		SourceRowRootGeneration: 1,
		SourceRowVersionUpper:   uint64(ds.Rows),
	})
	if err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	if baseResult.GenerationID != 1 || baseResult.InsertedRows != ds.Rows {
		t.Fatalf("base result=%+v want generation=1 inserted=%d", baseResult, ds.Rows)
	}
	result, err := adapter.Apply(ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updates),
		Deletes:                 deletes,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updates) + len(deletes)),
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.GenerationID != 2 || result.UpdatedRows != len(updates) || result.DeletedRows != len(deletes) {
		t.Fatalf("mutation result=%+v", result)
	}
	if len(result.Manifest.PartSet.BaseParts) != 1 || len(result.Manifest.PartSet.DeltaParts) != 1 || len(result.Manifest.PartSet.Tombstones) != len(deletes) {
		t.Fatalf("manifest part set=%+v", result.Manifest.PartSet)
	}
	coverage := result.Manifest.PartSet.DeltaParts[0].Coverage
	if coverage.SourceRowRootGeneration != 2 || coverage.SourceRowVersionLower != uint64(ds.Rows) || coverage.SourceRowVersionUpper != uint64(ds.Rows+len(updates)+len(deletes)) {
		t.Fatalf("delta coverage=%+v", coverage)
	}

	reader, err := adapter.Reader(ColumnPartImageReadOptions{IncludeAggregateMetadata: true})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	expected := applyJSONBenchMutations(ds, updates, map[int64]bool{10: true, 11: true})
	if stats := reader.VisibilityStats(); stats.VisibleRows != expected.Rows || stats.SupersededRows != len(updates) || stats.DeletedRows != len(deletes) {
		t.Fatalf("visibility stats=%+v want visible=%d superseded=%d deleted=%d", stats, expected.Rows, len(updates), len(deletes))
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reader, expected)

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench", ValidationMode: ColumnWorkspaceValidateTCS1Header})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace(reopen): %v", err)
	}
	defer reopened.Close()
	reopenedManifest, err := reopened.LoadCollectionManifest()
	if err != nil {
		t.Fatalf("LoadCollectionManifest(reopen): %v", err)
	}
	reopenedReader, err := OpenColumnPartSetReader(reopened, reopenedManifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader(reopen): %v", err)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reopenedReader, expected)

	replayWorkspace, replayAdapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer replayWorkspace.Close()
	if _, err := replayAdapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("replay PublishBaseBatch: %v", err)
	}
	if _, err := replayAdapter.Apply(ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updates),
		Deletes:                 deletes,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updates) + len(deletes)),
	}); err != nil {
		t.Fatalf("replay Apply: %v", err)
	}
	replayReader, err := replayAdapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("replay Reader: %v", err)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, replayReader, expected)
}

func TestColumnMutationAdapterReplayIgnoresPhysicalDescriptorsM9D(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	opts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, 32, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(time): %v", err)
	}
	scenario := newJSONBenchMutationReplayScenario(t, ds)

	original := testJSONBenchMutationReplayState(t, t.TempDir(), ds, opts, scenario, testJSONBenchMutationReplayStateOptions{})
	physicalReplay := testJSONBenchMutationReplayState(t, t.TempDir(), ds, opts, scenario, testJSONBenchMutationReplayStateOptions{
		AdapterOptions: ColumnMutationAdapterOptions{
			InitialPartID:     1_000,
			InitialGeneration: 50,
		},
		PreseedAssetOffsets: true,
	})
	offsetReplay := testJSONBenchMutationReplayState(t, t.TempDir(), ds, opts, scenario, testJSONBenchMutationReplayStateOptions{
		PreseedAssetOffsets: true,
	})

	assertJSONBenchPartSetQueriesMatchRaw(t, original.reader, scenario.expected)
	assertJSONBenchPartSetQueriesMatchRaw(t, physicalReplay.reader, scenario.expected)
	assertJSONBenchPartSetQueriesMatchRaw(t, offsetReplay.reader, scenario.expected)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, original.reader, scenario.expected, JSONBenchColumnPartLayoutTimeUS)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, physicalReplay.reader, scenario.expected, JSONBenchColumnPartLayoutTimeUS)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, offsetReplay.reader, scenario.expected, JSONBenchColumnPartLayoutTimeUS)
	assertColumnPartSetLogicalDigestMatchesDataset(t, original.reader, scenario.expected, ds.Dictionaries)
	assertColumnPartSetLogicalDigestMatchesDataset(t, physicalReplay.reader, scenario.expected, ds.Dictionaries)
	assertColumnPartSetLogicalDigestMatchesDataset(t, offsetReplay.reader, scenario.expected, ds.Dictionaries)
	assertColumnReplayPhysicalDescriptorsDiffer(t, original, physicalReplay)
	assertColumnReplayAssetOffsetsDifferOnly(t, original, offsetReplay)
	if original.manifest.ByteAccounting != physicalReplay.manifest.ByteAccounting {
		t.Fatalf("byte accounting changed across physical replay\noriginal=%+v\nreplay=%+v", original.manifest.ByteAccounting, physicalReplay.manifest.ByteAccounting)
	}
	if original.manifest.ByteAccounting != offsetReplay.manifest.ByteAccounting {
		t.Fatalf("byte accounting changed across offset replay\noriginal=%+v\nreplay=%+v", original.manifest.ByteAccounting, offsetReplay.manifest.ByteAccounting)
	}
	if original.reader.VisibilityStats() != physicalReplay.reader.VisibilityStats() {
		t.Fatalf("visibility stats changed across physical replay\noriginal=%+v\nreplay=%+v", original.reader.VisibilityStats(), physicalReplay.reader.VisibilityStats())
	}
	if original.reader.VisibilityStats() != offsetReplay.reader.VisibilityStats() {
		t.Fatalf("visibility stats changed across offset replay\noriginal=%+v\nreplay=%+v", original.reader.VisibilityStats(), offsetReplay.reader.VisibilityStats())
	}

	remapped := remapJSONBenchDictionaryCodes(t, ds, 10_000)
	remappedOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(remapped, 32, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(remapped time): %v", err)
	}
	if ds.Dictionaries["commit_collection_code"]["app.bsky.feed.post"] == remapped.Dictionaries["commit_collection_code"]["app.bsky.feed.post"] {
		t.Fatal("dictionary remap did not change commit_collection_code/app.bsky.feed.post")
	}
	remappedScenario := newJSONBenchMutationReplayScenario(t, remapped)
	dictionaryReplay := testJSONBenchMutationReplayState(t, t.TempDir(), remapped, remappedOpts, remappedScenario, testJSONBenchMutationReplayStateOptions{
		AdapterOptions: ColumnMutationAdapterOptions{
			InitialPartID:     2_000,
			InitialGeneration: 90,
		},
		PreseedAssetOffsets: true,
	})
	assertJSONBenchPartSetQueriesMatchRaw(t, dictionaryReplay.reader, remappedScenario.expected)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, dictionaryReplay.reader, remappedScenario.expected, JSONBenchColumnPartLayoutTimeUS)
	assertColumnPartSetLogicalDigestMatchesDataset(t, dictionaryReplay.reader, remappedScenario.expected, ds.Dictionaries)
	canonicalReplayExpected := canonicalizeJSONBenchDictionaryCodes(t, remappedScenario.expected, ds.Dictionaries)
	assertJSONBenchQueryDigestsEqual(t, scenario.expected, canonicalReplayExpected)
	if got, want := jsonBenchCanonicalDatasetDigest(t, remappedScenario.expected, ds.Dictionaries), jsonBenchCanonicalDatasetDigest(t, scenario.expected, ds.Dictionaries); got != want {
		t.Fatalf("canonical declared-column digest=%d want %d", got, want)
	}

	clickHouseOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, 32, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(clickhouse): %v", err)
	}
	clickHouseOriginal := testJSONBenchMutationReplayState(t, t.TempDir(), ds, clickHouseOpts, scenario, testJSONBenchMutationReplayStateOptions{})
	clickHouseReplay := testJSONBenchMutationReplayState(t, t.TempDir(), ds, clickHouseOpts, scenario, testJSONBenchMutationReplayStateOptions{
		AdapterOptions: ColumnMutationAdapterOptions{
			InitialPartID:     3_000,
			InitialGeneration: 130,
		},
		PreseedAssetOffsets: true,
	})
	assertJSONBenchPartSetQueriesMatchRaw(t, clickHouseOriginal.reader, scenario.expected)
	assertJSONBenchPartSetQueriesMatchRaw(t, clickHouseReplay.reader, scenario.expected)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, clickHouseOriginal.reader, scenario.expected, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	assertJSONBenchReplaySpecializedQueriesMatchRaw(t, clickHouseReplay.reader, scenario.expected, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	assertColumnReplayPhysicalDescriptorsDiffer(t, clickHouseOriginal, clickHouseReplay)
	if clickHouseOriginal.manifest.ByteAccounting != clickHouseReplay.manifest.ByteAccounting {
		t.Fatalf("clickhouse byte accounting changed across physical replay\noriginal=%+v\nreplay=%+v", clickHouseOriginal.manifest.ByteAccounting, clickHouseReplay.manifest.ByteAccounting)
	}
}

func TestColumnMutationReplayProfileContractM9D(t *testing.T) {
	tests := []struct {
		name            string
		profile         ColumnMutationReplayProfile
		wantErr         bool
		wantErrContains string
		want            string
		wantLabel       string
	}{
		{name: "default durable", want: "durable"},
		{name: "durable", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayDurable}, want: "durable"},
		{name: "default durable benchmark only rejected", profile: ColumnMutationReplayProfile{BenchmarkOnly: true}, wantErr: true, wantErrContains: "durable (default)", wantLabel: "durable_benchmark_ceiling"},
		{name: "durable benchmark only rejected", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayDurable, BenchmarkOnly: true}, wantErr: true, wantErrContains: "set Durability", wantLabel: "durable_benchmark_ceiling"},
		{name: "wal on fast production rejected", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayWALOnFast}, wantErr: true, wantErrContains: "not supported for production"},
		{name: "fast production rejected", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayFast}, wantErr: true, wantErrContains: "not supported for production"},
		{name: "wal on fast benchmark ceiling", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayWALOnFast, BenchmarkOnly: true}, want: "wal_on_fast_benchmark_ceiling"},
		{name: "fast benchmark ceiling", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayFast, BenchmarkOnly: true}, want: "fast_benchmark_ceiling"},
		{name: "unknown rejected", profile: ColumnMutationReplayProfile{Durability: "unsafe"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.profile.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Validate() nil error")
				}
				if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("Validate() err=%q want containing %q", err, tt.wantErrContains)
				}
				if tt.wantLabel != "" {
					if got := tt.profile.Label(); got != tt.wantLabel {
						t.Fatalf("Label()=%q want %q", got, tt.wantLabel)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate(): %v", err)
			}
			if got := tt.profile.Label(); got != tt.want {
				t.Fatalf("Label()=%q want %q", got, tt.want)
			}
			if production := tt.profile.ProductionSupported(); production != (tt.want == "durable") {
				t.Fatalf("ProductionSupported()=%v label=%s", production, tt.want)
			}
		})
	}
}

func TestColumnWorkspaceOptionsForMutationReplayProfileValidatesM9D(t *testing.T) {
	tests := []struct {
		name     string
		profile  ColumnMutationReplayProfile
		wantMode ColumnWorkspaceManifestSyncMode
		wantErr  string
	}{
		{name: "default durable", wantMode: ColumnWorkspaceManifestSyncDurable},
		{name: "wal on fast benchmark", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayWALOnFast, BenchmarkOnly: true}, wantMode: ColumnWorkspaceManifestSyncDisabledForBenchmark},
		{name: "fast benchmark", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayFast, BenchmarkOnly: true}, wantMode: ColumnWorkspaceManifestSyncDisabledForBenchmark},
		{name: "unknown rejected before open", profile: ColumnMutationReplayProfile{Durability: "unsafe"}, wantErr: "unsupported"},
		{name: "durable benchmark rejected before open", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayDurable, BenchmarkOnly: true}, wantErr: "cannot be benchmark-only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := columnWorkspaceOptionsForMutationReplayProfile("jsonbench", tt.profile)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("columnWorkspaceOptionsForMutationReplayProfile() nil error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("columnWorkspaceOptionsForMutationReplayProfile() err=%q want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("columnWorkspaceOptionsForMutationReplayProfile(): %v", err)
			}
			if opts.Collection != "jsonbench" {
				t.Fatalf("Collection=%q want jsonbench", opts.Collection)
			}
			if opts.ManifestSyncMode != tt.wantMode {
				t.Fatalf("ManifestSyncMode=%q want %q", opts.ManifestSyncMode, tt.wantMode)
			}
		})
	}
}

func TestColumnMutationAdapterAppliesReplayProfileOptionM9D(t *testing.T) {
	ds := syntheticJSONBenchDataset(32)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	profile := ColumnMutationReplayProfile{Durability: ColumnMutationReplayWALOnFast, BenchmarkOnly: true}
	workspaceOpts, err := columnWorkspaceOptionsForMutationReplayProfile("jsonbench", profile)
	if err != nil {
		t.Fatalf("columnWorkspaceOptionsForMutationReplayProfile: %v", err)
	}
	workspace, err := OpenColumnWorkspace(t.TempDir(), workspaceOpts)
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:    "jsonbench",
		StoreOptions:  opts,
		Dictionaries:  ds.Dictionaries,
		ReplayProfile: profile,
	})
	if err != nil {
		t.Fatalf("NewColumnMutationAdapter benchmark profile: %v", err)
	}
	if got := adapter.ReplayProfile().Label(); got != "wal_on_fast_benchmark_ceiling" {
		t.Fatalf("ReplayProfile label=%q want wal_on_fast_benchmark_ceiling", got)
	}
	for _, tt := range []struct {
		name    string
		profile ColumnMutationReplayProfile
	}{
		{name: "default"},
		{name: "explicit_durable", profile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayDurable}},
	} {
		t.Run("reject_durable_on_no_sync_"+tt.name, func(t *testing.T) {
			if _, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
				Collection:    "jsonbench",
				StoreOptions:  opts,
				Dictionaries:  ds.Dictionaries,
				ReplayProfile: tt.profile,
			}); !errors.Is(err, ErrColumnMutationReplayWorkspaceSyncMode) {
				t.Fatalf("NewColumnMutationAdapter durable profile on no-sync workspace err=%v want sync-mode rejection", err)
			}
		})
	}

	durableWorkspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace durable: %v", err)
	}
	defer durableWorkspace.Close()
	if _, err := NewColumnMutationAdapter(durableWorkspace, ColumnMutationAdapterOptions{
		Collection:    "jsonbench",
		StoreOptions:  opts,
		Dictionaries:  ds.Dictionaries,
		ReplayProfile: profile,
	}); !errors.Is(err, ErrColumnMutationReplayWorkspaceSyncMode) {
		t.Fatalf("NewColumnMutationAdapter benchmark profile on durable workspace err=%v want sync-mode rejection", err)
	}

	if _, err := NewColumnMutationAdapter(durableWorkspace, ColumnMutationAdapterOptions{
		Collection:    "jsonbench",
		StoreOptions:  opts,
		Dictionaries:  ds.Dictionaries,
		ReplayProfile: ColumnMutationReplayProfile{Durability: ColumnMutationReplayWALOnFast},
	}); err == nil || !strings.Contains(err.Error(), "not supported for production") {
		t.Fatalf("NewColumnMutationAdapter production wal_on_fast err=%v want production rejection", err)
	}
}

func TestColumnMutationAdapterDeleteOnlyBatch(t *testing.T) {
	ds := syntheticJSONBenchDataset(96)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer workspace.Close()
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	result, err := adapter.Apply(ColumnMutationBatch{
		Deletes:                 []int64{3, 5, 5, 7},
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + 1),
	})
	if err != nil {
		t.Fatalf("Apply(delete-only): %v", err)
	}
	if result.GenerationID != 2 || result.DeletedRows != 3 || result.Part.PartID != 0 {
		t.Fatalf("delete-only result=%+v", result)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if stats := reader.VisibilityStats(); stats.VisibleRows != ds.Rows-3 || stats.DeletedRows != 3 || stats.DeltaParts != 0 {
		t.Fatalf("visibility stats=%+v", stats)
	}
	for _, id := range []int64{3, 5, 7} {
		if _, ok := reader.LatestLocator(id); ok {
			t.Fatalf("deleted id %d has latest locator", id)
		}
	}
}

func TestColumnPartSetLocatorDecisionPathsMatch(t *testing.T) {
	ds := syntheticJSONBenchDataset(128)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer workspace.Close()
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	for i := 0; i < 12; i++ {
		id := int64(i * 3)
		row := int(id)
		nextTime := ds.Columns["time_us"][row] + int64(i+1)*1_000_000
		updates := jsonBenchDeltaBatch(ds, map[int64]map[string]int64{
			id: {
				"time_us":     nextTime,
				"hour_of_day": unixMicroHour(nextTime),
			},
		})
		if _, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updates,
			SourceRowRootGeneration: uint64(i + 2),
			SourceRowVersionLower:   uint64(ds.Rows + i),
			SourceRowVersionUpper:   uint64(ds.Rows + i + 1),
		}); err != nil {
			t.Fatalf("Apply update %d: %v", i, err)
		}
	}
	if _, err := adapter.Apply(ColumnMutationBatch{
		Deletes:                 []int64{9},
		SourceRowRootGeneration: 20,
		SourceRowVersionLower:   uint64(ds.Rows + 12),
		SourceRowVersionUpper:   uint64(ds.Rows + 13),
	}); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	for _, id := range []int64{0, 3, 12, 45, 127} {
		side, sideOK := reader.LatestLocator(id)
		scan, scanOK := reader.ScanLatestLocator(id)
		if sideOK != scanOK || side != scan {
			t.Fatalf("locator id=%d side=(%+v,%v) scan=(%+v,%v)", id, side, sideOK, scan, scanOK)
		}
		sideValue, sideValueOK, err := reader.ValueAtLatest(id, "time_us")
		if err != nil {
			t.Fatalf("ValueAtLatest(%d): %v", id, err)
		}
		scanValue, scanValueOK, err := reader.ScanValueAtLatest(id, "time_us")
		if err != nil {
			t.Fatalf("ScanValueAtLatest(%d): %v", id, err)
		}
		if sideValueOK != scanValueOK || sideValue != scanValue {
			t.Fatalf("value id=%d side=(%d,%v) scan=(%d,%v)", id, sideValue, sideValueOK, scanValue, scanValueOK)
		}
	}
	if _, ok := reader.LatestLocator(9); ok {
		t.Fatal("deleted id 9 has side-index locator")
	}
	if _, ok := reader.ScanLatestLocator(9); ok {
		t.Fatal("deleted id 9 has scan locator")
	}
}

func BenchmarkColumnLocatorDecisionM8A(b *testing.B) {
	for _, deltaParts := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("delta_parts_%03d", deltaParts), func(b *testing.B) {
			reader, target := benchmarkColumnMutationLocatorReader(b, deltaParts)
			b.ReportMetric(float64(deltaParts+1), "parts")
			b.ReportMetric(float64(reader.VisibilityStats().VisibleRows), "visible_rows")
			b.Run("side_index_locator", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					locator, ok := reader.LatestLocator(target)
					if !ok {
						b.Fatal("missing side-index locator")
					}
					benchSink += int64(locator.PartRow)
				}
			})
			b.Run("part_local_scan_locator", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					locator, ok := reader.ScanLatestLocator(target)
					if !ok {
						b.Fatal("missing scan locator")
					}
					benchSink += int64(locator.PartRow)
				}
			})
			b.Run("side_index_point_value", func(b *testing.B) {
				var scratch ColumnPartSetPointLookupScratch
				if _, _, err := reader.ValueAtLatestWithScratch(target, "time_us", &scratch); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, ok, err := reader.ValueAtLatestWithScratch(target, "time_us", &scratch)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						b.Fatal("missing side-index value")
					}
					benchSink += value
				}
			})
			b.Run("part_local_scan_point_value", func(b *testing.B) {
				var scratch ColumnPartSetPointLookupScratch
				if _, _, err := reader.ScanValueAtLatestWithScratch(target, "time_us", &scratch); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, ok, err := reader.ScanValueAtLatestWithScratch(target, "time_us", &scratch)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						b.Fatal("missing scan value")
					}
					benchSink += value
				}
			})
		})
	}
}

func BenchmarkColumnMutationAdapterApplyM8A(b *testing.B) {
	ds := syntheticJSONBenchDataset(4096)
	opts, err := JSONBenchColumnPartOptions(ds, 128)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	updates := make(map[int64]map[string]int64, 128)
	for i := int64(0); i < 128; i++ {
		id := i * 7
		nextTime := ds.Columns["time_us"][int(id)] + 1_000_000
		updates[id] = map[string]int64{
			"time_us":     nextTime,
			"hour_of_day": unixMicroHour(nextTime),
		}
	}
	deletes := make([]int64, 0, 32)
	for i := int64(0); i < 32; i++ {
		deletes = append(deletes, 2048+i*3)
	}
	updateBatch := jsonBenchDeltaBatch(ds, updates)
	root := b.TempDir()
	b.ReportMetric(float64(updateBatch.Rows), "updated_rows")
	b.ReportMetric(float64(len(deletes)), "deleted_rows")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workspace, adapter := benchmarkColumnMutationAdapter(b, filepath.Join(root, fmt.Sprintf("iter-%06d", i)), opts, ds.Dictionaries)
		if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
			_ = workspace.Close()
			b.Fatalf("PublishBaseBatch: %v", err)
		}
		b.StartTimer()
		_, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updateBatch,
			Deletes:                 deletes,
			SourceRowRootGeneration: 2,
			SourceRowVersionLower:   uint64(ds.Rows),
			SourceRowVersionUpper:   uint64(ds.Rows + updateBatch.Rows + len(deletes)),
		})
		b.StopTimer()
		if err != nil {
			_ = workspace.Close()
			b.Fatalf("Apply: %v", err)
		}
		_ = workspace.Close()
	}
}

func BenchmarkColumnMutationReplayM9D(b *testing.B) {
	fixtures := []struct {
		name    string
		rows    int
		updates int
		deletes int
	}{
		{name: "small_1k", rows: 1_024, updates: 64, deletes: 16},
		{name: "medium_8k", rows: 8_192, updates: 256, deletes: 64},
	}
	profiles := []ColumnMutationReplayProfile{
		{Durability: ColumnMutationReplayDurable},
		{Durability: ColumnMutationReplayWALOnFast, BenchmarkOnly: true},
	}
	for _, fixture := range fixtures {
		ds := syntheticJSONBenchDataset(fixture.rows)
		opts, err := JSONBenchColumnPartOptions(ds, 128)
		if err != nil {
			b.Fatalf("JSONBenchColumnPartOptions(%s): %v", fixture.name, err)
		}
		scenario := benchmarkJSONBenchMutationReplayScenario(b, ds, fixture.updates, fixture.deletes)
		commandBytes := estimateColumnMutationReplayCommandBytes(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, []ColumnMutationBatch{scenario.batch})
		logicalRows := ds.Rows + scenario.batch.Updates.Rows + len(scenario.batch.Deletes)
		for _, profile := range profiles {
			if err := profile.Validate(); err != nil {
				b.Fatalf("profile %q: %v", profile.Label(), err)
			}
			b.Run(fixture.name+"/"+profile.Label(), func(b *testing.B) {
				root := b.TempDir()
				var last ColumnCollectionManifest
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dir := filepath.Join(root, fmt.Sprintf("iter-%06d", i))
					b.StartTimer()
					workspace, adapter := benchmarkColumnMutationAdapterWithProfile(b, dir, opts, ds.Dictionaries, profile)
					_, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)})
					if err == nil {
						_, err = adapter.Apply(scenario.batch)
					}
					if closeErr := workspace.Close(); err == nil {
						err = closeErr
					}
					if err != nil {
						b.Fatalf("replay: %v", err)
					}
					last = adapter.Manifest()
					b.StopTimer()
					if err := os.RemoveAll(dir); err != nil {
						b.Fatalf("cleanup %s: %v", dir, err)
					}
				}
				// Cleanup is outside the measured replay window, so rows/sec is
				// derived from the same timer window as the benchmark ns/op.
				elapsed := b.Elapsed()
				if b.N > 0 && elapsed > 0 {
					// The replay gate deliberately includes per-iteration workspace
					// open/publish/apply/close work in the timed loop.
					b.ReportMetric(float64(logicalRows)*float64(b.N)/elapsed.Seconds(), "rows/sec")
				}
				b.ReportMetric(float64(logicalRows), "logical_rows/op")
				b.ReportMetric(float64(commandBytes), "command_bytes/op")
				// Row remainder payloads are intentionally absent from M9D mutation replay.
				b.ReportMetric(0, "row_remainder_bytes/op")
				b.ReportMetric(float64(last.ByteAccounting.TotalAssetBytes), "column_asset_bytes/op")
				b.ReportMetric(float64(last.ByteAccounting.DescriptorBytes), "manifest_control_bytes/op")
				if !profile.ProductionSupported() {
					b.ReportMetric(1, "benchmark_ceiling")
				}
			})
		}
	}
}

func testColumnMutationAdapter(t testing.TB, dir string, opts ColumnStoreOptions, dictionaries map[string]map[string]int64) (*ColumnWorkspace, *ColumnMutationAdapter) {
	t.Helper()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:   "jsonbench",
		StoreOptions: opts,
		Dictionaries: dictionaries,
	})
	if err != nil {
		_ = workspace.Close()
		t.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	return workspace, adapter
}

func benchmarkColumnMutationAdapter(b *testing.B, dir string, opts ColumnStoreOptions, dictionaries map[string]map[string]int64) (*ColumnWorkspace, *ColumnMutationAdapter) {
	return benchmarkColumnMutationAdapterWithProfile(b, dir, opts, dictionaries, ColumnMutationReplayProfile{})
}

func benchmarkColumnMutationAdapterWithProfile(b *testing.B, dir string, opts ColumnStoreOptions, dictionaries map[string]map[string]int64, profile ColumnMutationReplayProfile) (*ColumnWorkspace, *ColumnMutationAdapter) {
	b.Helper()
	workspaceOpts, err := columnWorkspaceOptionsForMutationReplayProfile("jsonbench", profile)
	if err != nil {
		b.Fatalf("columnWorkspaceOptionsForMutationReplayProfile: %v", err)
	}
	workspace, err := OpenColumnWorkspace(dir, workspaceOpts)
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:    "jsonbench",
		StoreOptions:  opts,
		Dictionaries:  dictionaries,
		ReplayProfile: profile,
	})
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	return workspace, adapter
}

func benchmarkColumnMutationLocatorReader(b *testing.B, deltaParts int) (*ColumnPartSetReader, int64) {
	b.Helper()
	ds := syntheticJSONBenchDataset(8192)
	opts, err := JSONBenchColumnPartOptions(ds, 128)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := benchmarkColumnMutationAdapter(b, b.TempDir(), opts, ds.Dictionaries)
	b.Cleanup(func() { _ = workspace.Close() })
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		b.Fatalf("PublishBaseBatch: %v", err)
	}
	target := int64(4096)
	for i := 0; i < deltaParts; i++ {
		id := target
		if i%4 != 0 {
			id = int64((i * 97) % ds.Rows)
		}
		nextTime := ds.Columns["time_us"][int(id)] + int64(i+1)*1_000_000
		updates := jsonBenchDeltaBatch(ds, map[int64]map[string]int64{
			id: {
				"time_us":     nextTime,
				"hour_of_day": unixMicroHour(nextTime),
			},
		})
		if _, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updates,
			SourceRowRootGeneration: uint64(i + 2),
			SourceRowVersionLower:   uint64(ds.Rows + i),
			SourceRowVersionUpper:   uint64(ds.Rows + i + 1),
		}); err != nil {
			b.Fatalf("Apply(%d): %v", i, err)
		}
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		b.Fatalf("Reader: %v", err)
	}
	return reader, target
}

type jsonBenchMutationReplayScenario struct {
	batch    ColumnMutationBatch
	expected JSONBenchDataset
}

type jsonBenchMutationReplayState struct {
	reader   *ColumnPartSetReader
	manifest ColumnCollectionManifest
}

func newJSONBenchMutationReplayScenario(tb testing.TB, ds JSONBenchDataset) jsonBenchMutationReplayScenario {
	tb.Helper()
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		tb.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	updates := map[int64]map[string]int64{
		5: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][40],
			"time_us":                ds.Columns["time_us"][5] - 10_000_000,
		},
		25: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][41],
			"time_us":                ds.Columns["time_us"][25] + 20_000_000,
		},
		97: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][42],
			"time_us":                ds.Columns["time_us"][97] + 30_000_000,
		},
	}
	for _, update := range updates {
		update["hour_of_day"] = unixMicroHour(update["time_us"])
	}
	deletes := []int64{10, 11, 12}
	deleteSet := map[int64]bool{10: true, 11: true, 12: true}
	batch := ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updates),
		Deletes:                 deletes,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updates) + len(deletes)),
	}
	return jsonBenchMutationReplayScenario{
		batch:    batch,
		expected: applyJSONBenchMutations(ds, updates, deleteSet),
	}
}

func benchmarkJSONBenchMutationReplayScenario(tb testing.TB, ds JSONBenchDataset, updates int, deletes int) jsonBenchMutationReplayScenario {
	tb.Helper()
	if updates <= 0 || deletes < 0 || updates >= ds.Rows/2 || updates+deletes >= ds.Rows {
		tb.Fatalf("invalid replay scenario updates=%d deletes=%d rows=%d", updates, deletes, ds.Rows)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		tb.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	updateMap := make(map[int64]map[string]int64, updates)
	for i := 0; i < updates; i++ {
		id := int64((i*17 + 5) % (ds.Rows / 2))
		nextTime := ds.Columns["time_us"][int(id)] + int64(i+1)*1_000_000
		updateMap[id] = map[string]int64{
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][(int(id)+41)%ds.Rows],
			"time_us":                nextTime,
			"hour_of_day":            unixMicroHour(nextTime),
		}
	}
	deleteList := make([]int64, 0, deletes)
	deleteSet := make(map[int64]bool, deletes)
	for i := 0; i < deletes; i++ {
		id := int64(ds.Rows - 1 - i*3)
		deleteList = append(deleteList, id)
		deleteSet[id] = true
	}
	batch := ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updateMap),
		Deletes:                 deleteList,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updateMap) + len(deleteSet)),
	}
	return jsonBenchMutationReplayScenario{
		batch:    batch,
		expected: applyJSONBenchMutations(ds, updateMap, deleteSet),
	}
}

type testJSONBenchMutationReplayStateOptions struct {
	AdapterOptions      ColumnMutationAdapterOptions
	ReplayProfile       ColumnMutationReplayProfile
	PreseedAssetOffsets bool
}

func testJSONBenchMutationReplayState(t *testing.T, dir string, ds JSONBenchDataset, opts ColumnStoreOptions, scenario jsonBenchMutationReplayScenario, stateOpts testJSONBenchMutationReplayStateOptions) jsonBenchMutationReplayState {
	t.Helper()
	adapterOpts := stateOpts.AdapterOptions
	adapterOpts.ReplayProfile = stateOpts.ReplayProfile
	workspaceOpts, err := columnWorkspaceOptionsForMutationReplayProfile("jsonbench", stateOpts.ReplayProfile)
	if err != nil {
		t.Fatalf("columnWorkspaceOptionsForMutationReplayProfile: %v", err)
	}
	workspace, err := OpenColumnWorkspace(dir, workspaceOpts)
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	if stateOpts.PreseedAssetOffsets {
		part, err := BuildColumnPart(777, opts, ColumnBatch{Rows: 1, Columns: sliceJSONBenchColumns(ds, 0, 1)})
		if err != nil {
			t.Fatalf("BuildColumnPart(preseed): %v", err)
		}
		entry, err := workspace.PublishPart(part, ds.Dictionaries)
		if err != nil {
			t.Fatalf("PublishPart(preseed): %v", err)
		}
		if entry.AssetRef.Offset != 0 {
			t.Fatalf("preseed offset=%d want 0", entry.AssetRef.Offset)
		}
	}
	adapterOpts.Collection = "jsonbench"
	adapterOpts.StoreOptions = opts
	adapterOpts.Dictionaries = ds.Dictionaries
	adapter, err := NewColumnMutationAdapter(workspace, adapterOpts)
	if err != nil {
		t.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{
		SourceRowRootGeneration: 1,
		SourceRowVersionUpper:   uint64(ds.Rows),
	}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	if _, err := adapter.Apply(scenario.batch); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{IncludeAggregateMetadata: true})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	assertColumnPartSetReaderDecodedAggregateMetadata(t, reader, opts)
	return jsonBenchMutationReplayState{
		reader:   reader,
		manifest: adapter.Manifest(),
	}
}

func assertColumnPartSetReaderDecodedAggregateMetadata(t *testing.T, reader *ColumnPartSetReader, opts ColumnStoreOptions) {
	t.Helper()
	if len(opts.AggregateMetadata) == 0 {
		return
	}
	if reader == nil {
		t.Fatal("nil part set reader")
	}
	for _, loaded := range reader.parts {
		if loaded.Part == nil {
			t.Fatalf("reader loaded nil part for ref %+v", loaded.Ref)
		}
		for _, def := range opts.AggregateMetadata {
			metadata, ok := loaded.Part.AggregateMetadataByName(def.Name)
			if !ok {
				t.Fatalf("part %d missing decoded aggregate metadata %s", loaded.Part.Descriptor.PartID, def.Name)
			}
			if metadata.Definition.Name != def.Name {
				t.Fatalf("part %d aggregate metadata name=%q want %q", loaded.Part.Descriptor.PartID, metadata.Definition.Name, def.Name)
			}
		}
	}
}

func assertColumnReplayPhysicalDescriptorsDiffer(t *testing.T, original jsonBenchMutationReplayState, replay jsonBenchMutationReplayState) {
	t.Helper()
	if original.manifest.ActiveGeneration == replay.manifest.ActiveGeneration {
		t.Fatalf("active generation preserved: %d", original.manifest.ActiveGeneration)
	}
	if len(original.manifest.PartSet.BaseParts) != 1 || len(original.manifest.PartSet.DeltaParts) != 1 {
		t.Fatalf("original part set=%+v", original.manifest.PartSet)
	}
	if len(replay.manifest.PartSet.BaseParts) != 1 || len(replay.manifest.PartSet.DeltaParts) != 1 {
		t.Fatalf("replay part set=%+v", replay.manifest.PartSet)
	}
	originalBase := original.manifest.PartSet.BaseParts[0]
	replayBase := replay.manifest.PartSet.BaseParts[0]
	originalDelta := original.manifest.PartSet.DeltaParts[0]
	replayDelta := replay.manifest.PartSet.DeltaParts[0]
	if originalBase.GenerationID == replayBase.GenerationID || originalDelta.GenerationID == replayDelta.GenerationID {
		t.Fatalf("generation ids preserved: original base/delta=%d/%d replay=%d/%d", originalBase.GenerationID, originalDelta.GenerationID, replayBase.GenerationID, replayDelta.GenerationID)
	}
	if originalBase.Part.PartID == replayBase.Part.PartID || originalDelta.Part.PartID == replayDelta.Part.PartID {
		t.Fatalf("part ids preserved: original base/delta=%d/%d replay=%d/%d", originalBase.Part.PartID, originalDelta.Part.PartID, replayBase.Part.PartID, replayDelta.Part.PartID)
	}
	if originalBase.Part.AssetRef.Offset == replayBase.Part.AssetRef.Offset {
		t.Fatalf("base asset offset preserved: %d", originalBase.Part.AssetRef.Offset)
	}
	originalLocator, ok := original.reader.LatestLocator(5)
	if !ok {
		t.Fatal("original missing locator for id 5")
	}
	replayLocator, ok := replay.reader.LatestLocator(5)
	if !ok {
		t.Fatal("replay missing locator for id 5")
	}
	if originalLocator.PartID == replayLocator.PartID {
		t.Fatalf("locator part id preserved: %d", originalLocator.PartID)
	}
}

func assertColumnReplayAssetOffsetsDifferOnly(t *testing.T, original jsonBenchMutationReplayState, replay jsonBenchMutationReplayState) {
	t.Helper()
	if original.manifest.ActiveGeneration != replay.manifest.ActiveGeneration {
		t.Fatalf("active generation changed: original=%d replay=%d", original.manifest.ActiveGeneration, replay.manifest.ActiveGeneration)
	}
	if len(original.manifest.PartSet.BaseParts) != 1 || len(original.manifest.PartSet.DeltaParts) != 1 {
		t.Fatalf("original part set=%+v", original.manifest.PartSet)
	}
	if len(replay.manifest.PartSet.BaseParts) != 1 || len(replay.manifest.PartSet.DeltaParts) != 1 {
		t.Fatalf("replay part set=%+v", replay.manifest.PartSet)
	}
	originalBase := original.manifest.PartSet.BaseParts[0]
	replayBase := replay.manifest.PartSet.BaseParts[0]
	originalDelta := original.manifest.PartSet.DeltaParts[0]
	replayDelta := replay.manifest.PartSet.DeltaParts[0]
	if originalBase.GenerationID != replayBase.GenerationID || originalDelta.GenerationID != replayDelta.GenerationID {
		t.Fatalf("generation ids changed: original base/delta=%d/%d replay=%d/%d", originalBase.GenerationID, originalDelta.GenerationID, replayBase.GenerationID, replayDelta.GenerationID)
	}
	if originalBase.Part.PartID != replayBase.Part.PartID || originalDelta.Part.PartID != replayDelta.Part.PartID {
		t.Fatalf("part ids changed: original base/delta=%d/%d replay=%d/%d", originalBase.Part.PartID, originalDelta.Part.PartID, replayBase.Part.PartID, replayDelta.Part.PartID)
	}
	if originalBase.Part.AssetRef.Offset == replayBase.Part.AssetRef.Offset {
		t.Fatalf("base asset offset preserved: %d", originalBase.Part.AssetRef.Offset)
	}
	if originalDelta.Part.AssetRef.Offset == replayDelta.Part.AssetRef.Offset {
		t.Fatalf("delta asset offset preserved: %d", originalDelta.Part.AssetRef.Offset)
	}
	originalLocator, ok := original.reader.LatestLocator(5)
	if !ok {
		t.Fatal("original missing locator for id 5")
	}
	replayLocator, ok := replay.reader.LatestLocator(5)
	if !ok {
		t.Fatal("replay missing locator for id 5")
	}
	if originalLocator.PartID != replayLocator.PartID {
		t.Fatalf("locator part id changed: original=%d replay=%d", originalLocator.PartID, replayLocator.PartID)
	}
}

func assertColumnPartSetLogicalDigestMatchesDataset(t *testing.T, reader *ColumnPartSetReader, expected JSONBenchDataset, canonicalDictionaries map[string]map[string]int64) {
	t.Helper()
	columns := sortedJSONBenchColumnNames(expected)
	result, err := reader.ScanProjected(columns)
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	if result.Rows != expected.Rows {
		t.Fatalf("ScanProjected rows=%d want %d", result.Rows, expected.Rows)
	}
	// The projected values use the expected dataset's physical dictionary codes;
	// canonicalDictionaries is only the cross-replay comparison target.
	got := JSONBenchDataset{Rows: result.Rows, Columns: result.Columns, Dictionaries: expected.Dictionaries}
	gotDigest := jsonBenchCanonicalDatasetDigest(t, got, canonicalDictionaries)
	wantDigest := jsonBenchCanonicalDatasetDigest(t, expected, canonicalDictionaries)
	if gotDigest != wantDigest {
		t.Fatalf("projected declared-column digest=%d want %d", gotDigest, wantDigest)
	}
}

func assertJSONBenchQueryDigestsEqual(t *testing.T, a JSONBenchDataset, b JSONBenchDataset) {
	t.Helper()
	aTimings, err := RunJSONBenchQueries(a, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries(a): %v", err)
	}
	bTimings, err := RunJSONBenchQueries(b, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries(b): %v", err)
	}
	if len(aTimings) != len(bTimings) {
		t.Fatalf("query count=%d want %d", len(bTimings), len(aTimings))
	}
	for i := range aTimings {
		if aTimings[i].Query != bTimings[i].Query || aTimings[i].ResultRows != bTimings[i].ResultRows || aTimings[i].ResultDigest != bTimings[i].ResultDigest {
			t.Fatalf("%s rows/digest=(%d,%d) want %s (%d,%d)", bTimings[i].Query, bTimings[i].ResultRows, bTimings[i].ResultDigest, aTimings[i].Query, aTimings[i].ResultRows, aTimings[i].ResultDigest)
		}
	}
}

func assertJSONBenchReplaySpecializedQueriesMatchRaw(t *testing.T, reader *ColumnPartSetReader, ds JSONBenchDataset, layout JSONBenchColumnPartLayout) {
	t.Helper()
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	switch layout {
	case JSONBenchColumnPartLayoutTimeUS:
		rawQ4Rows, rawQ4Digest := runJSONBenchQ4(ds, codes)
		q4Rows, q4Digest, q4Diagnostics, err := runJSONBenchPartSetQ4TimeOrdered(reader, codes, &jsonBenchPartQueryScratch{})
		if err != nil {
			t.Fatalf("runJSONBenchPartSetQ4TimeOrdered: %v", err)
		}
		if q4Rows != rawQ4Rows || q4Digest != rawQ4Digest {
			t.Fatalf("q4a rows/digest=(%d,%d) raw=(%d,%d)", q4Rows, q4Digest, rawQ4Rows, rawQ4Digest)
		}
		if q4Diagnostics.AggregateKernel == "" {
			t.Fatalf("q4a diagnostics missing aggregate kernel: %+v", q4Diagnostics)
		}

		rawQ5Rows, rawQ5Digest := runJSONBenchQ5(ds, codes)
		q5Rows, q5Digest, q5Diagnostics, err := runJSONBenchPartSetQ5AggregateMetadata(reader, codes, &jsonBenchPartQueryScratch{})
		if err != nil {
			if !strings.Contains(err.Error(), "requires all-visible") {
				t.Fatalf("runJSONBenchPartSetQ5AggregateMetadata: %v", err)
			}
			t.Logf("skipping q5 metadata replay parity: %v", err)
		} else {
			if q5Rows != rawQ5Rows || q5Digest != rawQ5Digest {
				t.Fatalf("q5 metadata rows/digest=(%d,%d) raw=(%d,%d)", q5Rows, q5Digest, rawQ5Rows, rawQ5Digest)
			}
			if !q5Diagnostics.AggregateMetadataUsed || q5Diagnostics.RowsScanned != 0 || q5Diagnostics.BlocksDecoded != 0 {
				t.Fatalf("q5 metadata diagnostics=%+v want metadata-only", q5Diagnostics)
			}
		}
	case JSONBenchColumnPartLayoutClickHouseFilterUserTime:
		rawQ4Rows, rawQ4Digest := runJSONBenchQ4(ds, codes)
		q4Rows, q4Digest, q4Diagnostics, err := runJSONBenchPartSetQ4ClickHouseOrder(reader, codes, &jsonBenchPartQueryScratch{})
		if err != nil {
			t.Fatalf("runJSONBenchPartSetQ4ClickHouseOrder: %v", err)
		}
		if q4Rows != rawQ4Rows || q4Digest != rawQ4Digest {
			t.Fatalf("q4b rows/digest=(%d,%d) raw=(%d,%d)", q4Rows, q4Digest, rawQ4Rows, rawQ4Digest)
		}
		if q4Diagnostics.AggregateKernel != "multipart_clickhouse_order_prefix_scan_min_by_user" || q4Diagnostics.EarlyStopAvailable {
			t.Fatalf("q4b diagnostics=%+v want ClickHouse-order prefix scan", q4Diagnostics)
		}

		q4MetaRows, q4MetaDigest, q4MetaDiagnostics, err := runJSONBenchPartSetQ4AggregateMetadata(reader, codes, &jsonBenchPartQueryScratch{})
		if err != nil {
			if !strings.Contains(err.Error(), "requires all-visible") {
				t.Fatalf("runJSONBenchPartSetQ4AggregateMetadata: %v", err)
			}
			t.Logf("skipping q4 metadata replay parity: %v", err)
		} else {
			if q4MetaRows != rawQ4Rows || q4MetaDigest != rawQ4Digest {
				t.Fatalf("q4 metadata rows/digest=(%d,%d) raw=(%d,%d)", q4MetaRows, q4MetaDigest, rawQ4Rows, rawQ4Digest)
			}
			if !q4MetaDiagnostics.AggregateMetadataUsed || q4MetaDiagnostics.RowsScanned != 0 || q4MetaDiagnostics.BlocksDecoded != 0 {
				t.Fatalf("q4 metadata diagnostics=%+v want metadata-only", q4MetaDiagnostics)
			}
		}
	default:
		t.Fatalf("unsupported replay specialized layout %q", layout)
	}
}

func remapJSONBenchDictionaryCodes(t *testing.T, ds JSONBenchDataset, offset int64) JSONBenchDataset {
	t.Helper()
	out := cloneJSONBenchDataset(ds)
	out.Dictionaries = make(map[string]map[string]int64, len(ds.Dictionaries))
	for name, dict := range ds.Dictionaries {
		entries := sortedJSONBenchDictionaryEntries(dict)
		shift := 1
		if len(entries) > 1 {
			shift = int(offset % int64(len(entries)))
			if shift == 0 {
				shift = 1
			}
		}
		next := make(map[string]int64, len(entries))
		oldToNew := make(map[int64]int64, len(entries))
		for i, entry := range entries {
			newCode := entries[(i+shift)%len(entries)].code
			next[entry.label] = newCode
			oldToNew[entry.code] = newCode
		}
		out.Dictionaries[name] = next
		values := out.Columns[name]
		for i, value := range values {
			newValue, ok := oldToNew[value]
			if !ok {
				t.Fatalf("column %s row %d code=%d missing from dictionary", name, i, value)
			}
			values[i] = newValue
		}
	}
	return out
}

func canonicalizeJSONBenchDictionaryCodes(t *testing.T, ds JSONBenchDataset, canonicalDictionaries map[string]map[string]int64) JSONBenchDataset {
	t.Helper()
	out := cloneJSONBenchDataset(ds)
	out.Dictionaries = cloneJSONBenchDictionaries(canonicalDictionaries)
	for name, canonical := range canonicalDictionaries {
		source := ds.Dictionaries[name]
		if source == nil {
			continue
		}
		inverse := invertJSONBenchDictionary(t, name, source)
		values := out.Columns[name]
		for i, value := range values {
			label, ok := inverse[value]
			if !ok {
				t.Fatalf("column %s row %d code=%d missing from source dictionary", name, i, value)
			}
			canonicalValue, ok := canonical[label]
			if !ok {
				t.Fatalf("column %s row %d label=%q missing from canonical dictionary", name, i, label)
			}
			values[i] = canonicalValue
		}
	}
	return out
}

func jsonBenchCanonicalDatasetDigest(t *testing.T, ds JSONBenchDataset, canonicalDictionaries map[string]map[string]int64) uint64 {
	t.Helper()
	columns := sortedJSONBenchColumnNames(ds)
	rowIndex := ds.Columns["row_index"]
	if len(rowIndex) != ds.Rows {
		t.Fatalf("row_index rows=%d want %d", len(rowIndex), ds.Rows)
	}
	order := make([]int, len(rowIndex))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return rowIndex[order[i]] < rowIndex[order[j]]
	})
	inverses := make(map[string]map[int64]string, len(ds.Dictionaries))
	for name, dict := range ds.Dictionaries {
		inverses[name] = invertJSONBenchDictionary(t, name, dict)
	}
	var digest uint64
	digest = digestMix(digest, uint64(ds.Rows), uint64(len(columns)))
	for _, row := range order {
		digest = digestMix(digest, uint64(rowIndex[row]), 0)
		for _, name := range columns {
			values := ds.Columns[name]
			if len(values) != ds.Rows {
				t.Fatalf("column %s rows=%d want %d", name, len(values), ds.Rows)
			}
			value := values[row]
			if canonical := canonicalDictionaries[name]; canonical != nil {
				label, ok := inverses[name][value]
				if !ok {
					t.Fatalf("column %s row %d code=%d missing from source dictionary", name, row, value)
				}
				canonicalValue, ok := canonical[label]
				if !ok {
					t.Fatalf("column %s row %d label=%q missing from canonical dictionary", name, row, label)
				}
				value = canonicalValue
			}
			digest = digestMix(digest, hashString64(name), uint64(value))
		}
	}
	return digest
}

func estimateColumnMutationReplayCommandBytes(base ColumnBatch, batches []ColumnMutationBatch) int {
	total := estimateColumnBatchPayloadBytes(base)
	for _, batch := range batches {
		total += estimateColumnBatchPayloadBytes(batch.Inserts)
		total += estimateColumnBatchPayloadBytes(batch.Updates)
		total += len(batch.Deletes) * 8
		total += 5 * 8
	}
	return total
}

func estimateColumnBatchPayloadBytes(batch ColumnBatch) int {
	if batch.Rows == 0 && len(batch.Columns) == 0 {
		return 0
	}
	total := 8
	for name, values := range batch.Columns {
		total += len(name) + 8
		total += len(values) * 8
	}
	return total
}

type jsonBenchDictionaryEntry struct {
	label string
	code  int64
}

func sortedJSONBenchDictionaryEntries(dict map[string]int64) []jsonBenchDictionaryEntry {
	entries := make([]jsonBenchDictionaryEntry, 0, len(dict))
	for label, code := range dict {
		entries = append(entries, jsonBenchDictionaryEntry{label: label, code: code})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].code == entries[j].code {
			return entries[i].label < entries[j].label
		}
		return entries[i].code < entries[j].code
	})
	return entries
}

func invertJSONBenchDictionary(t *testing.T, name string, dict map[string]int64) map[int64]string {
	t.Helper()
	inverse := make(map[int64]string, len(dict))
	for label, code := range dict {
		if prev, ok := inverse[code]; ok {
			t.Fatalf("dictionary %s duplicate code=%d labels=%q,%q", name, code, prev, label)
		}
		inverse[code] = label
	}
	return inverse
}

func cloneJSONBenchDataset(ds JSONBenchDataset) JSONBenchDataset {
	out := JSONBenchDataset{
		Rows:         ds.Rows,
		Files:        append([]string(nil), ds.Files...),
		Columns:      make(map[string][]int64, len(ds.Columns)),
		Dictionaries: cloneJSONBenchDictionaries(ds.Dictionaries),
	}
	for name, values := range ds.Columns {
		out.Columns[name] = append([]int64(nil), values...)
	}
	return out
}

func cloneJSONBenchDictionaries(dicts map[string]map[string]int64) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(dicts))
	for name, dict := range dicts {
		next := make(map[string]int64, len(dict))
		for label, code := range dict {
			next[label] = code
		}
		out[name] = next
	}
	return out
}

func sortedJSONBenchColumnNames(ds JSONBenchDataset) []string {
	columns := make([]string, 0, len(ds.Columns))
	for name := range ds.Columns {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	return columns
}

func hashString64(value string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return h.Sum64()
}
