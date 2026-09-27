package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8ServingResourcesRetainedTopologyV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture := m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 256, 8, 8
	vectors, queries := fixtureData(fixture)
	fixture.Checksum = fixtureChecksumFromData(vectors, queries)
	root := t.TempDir()
	descriptor := testM8QualificationRetainedDescriptorV1(t, filepath.Join(root, "m3"), strings.Repeat("a", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	parent := m8ProductionReportV1{Dataset: fixture, DatasetDirectory: testM8QualificationDatasetDirectoryV1(t, root, fixture), Variant: &descriptor}
	for _, group := range []string{"m8-data-group-00", "m8-data-group-01", "m8-data-group-02", "m8-data-group-03"} {
		parent.Topology.Groups = append(parent.Topology.Groups, nativewire.VectorPartitionM8ProductionGroupEvidenceV1{GroupID: group})
	}
	cfg := config{partitions: 16, raftGroups: 4, topK: 10, probes: []int{16}, efSearch: []int{96}, routerCandidates: 256, concurrency: []int{1, 32}, warmup: 4}
	assets, err := m8OpenServingResourceAssetsV1(parent, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer assets.Close()
	ctx, cancel := context.WithTimeout(context.Background(), m8ProductionTopologyTestTimeoutV1)
	defer cancel()
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer topology.Close()
	for _, placement := range assets.manifest.Placements {
		want := parent.Topology.Groups[int(placement.PartitionID)%len(parent.Topology.Groups)].GroupID
		if placement.GroupID != want || assets.assetSetDigests[want] == "" {
			t.Fatalf("retained placement/digest differs from parent topology: %+v", placement)
		}
	}
	truth, err := m8ExactTruthFixtureV1(vectors, queries, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m8WarmProductionTopologyV1(ctx, topology.Coordinator(), assets, queries, cfg); err != nil {
		t.Fatal(err)
	}
	for _, concurrency := range cfg.concurrency {
		var resources m8ServingResourcesV1
		row, _, _, err := m8RunProductionCellWithResourcesV1(ctx, topology.Coordinator(), assets, queries, truth, 16, 96, concurrency, 10, 256, 64<<20, &resources)
		if err != nil || resources.validate() != nil || row.RecallAtK != 1 || row.Samples != len(queries) {
			t.Fatalf("retained resource cell c%d: row=%+v resources=%+v err=%v", concurrency, row, resources, err)
		}
	}
	if !reflect.DeepEqual(assets.descriptor, &descriptor) || reflect.DeepEqual(assets.manifest.Placements, assets.status.Manifest.Placements) {
		t.Fatal("descriptor changed or retained local placements were used unchanged")
	}
}

func TestM8ServingResourceCountersV1(t *testing.T) {
	valid := m8ServingResourcesV1{
		Before:          m8ProcessResourceSnapshotV1{TotalAlloc: 100, Mallocs: 10, CPUNanos: 20, CPUAvailable: true, SnapshotNanos: 1},
		After:           m8ProcessResourceSnapshotV1{TotalAlloc: 200, Mallocs: 20, CPUNanos: 30, CPUAvailable: true, SnapshotNanos: 1},
		WorkerWallNanos: 10,
	}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	// Coarse CPU clocks and an allocation-free interval can legitimately have
	// zero deltas. Availability is an explicit bit, not inferred from a delta.
	flat := valid
	flat.After = flat.Before
	if err := flat.validate(); err != nil {
		t.Fatalf("available nondecreasing counters rejected: %v", err)
	}
	for name, mutate := range map[string]func(*m8ServingResourcesV1){
		"before CPU unavailable":  func(r *m8ServingResourcesV1) { r.Before.CPUAvailable = false },
		"after CPU unavailable":   func(r *m8ServingResourcesV1) { r.After.CPUAvailable = false },
		"negative CPU":            func(r *m8ServingResourcesV1) { r.Before.CPUNanos = -1 },
		"CPU decreases":           func(r *m8ServingResourcesV1) { r.After.CPUNanos = r.Before.CPUNanos - 1 },
		"bytes decrease":          func(r *m8ServingResourcesV1) { r.After.TotalAlloc = r.Before.TotalAlloc - 1 },
		"allocations decrease":    func(r *m8ServingResourcesV1) { r.After.Mallocs = r.Before.Mallocs - 1 },
		"missing wall":            func(r *m8ServingResourcesV1) { r.WorkerWallNanos = 0 },
		"missing before duration": func(r *m8ServingResourcesV1) { r.Before.SnapshotNanos = 0 },
		"missing after duration":  func(r *m8ServingResourcesV1) { r.After.SnapshotNanos = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if candidate.validate() == nil {
				t.Fatal("accepted unavailable, decreasing or incomplete counters")
			}
		})
	}
}

func TestM8ServingResourceCanceledAttemptsAndNilV1(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name, class := "canceled", "canceled"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if deadline {
			name, class = "deadline", "timeout"
			ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
			defer cancel()
		}
		t.Run(name, func(t *testing.T) {
			queries := [][]float64{{1}, {2}, {3}}
			truth := make([][]m8CanonicalResultV1, len(queries))
			assets := &m8ProductionMultiGroupAssetsV1{}
			var resources m8ServingResourcesV1
			row, results, durations, err := m8RunProductionCellWithResourcesV1(ctx, nil, assets, queries, truth, 1, 10, 2, 1, 256, 1<<20, &resources)
			if err != nil {
				t.Fatal(err)
			}
			if row.Status == "pass" || row.QPS != 0 || row.Samples != len(queries) || len(results) != len(queries) || len(durations) != len(queries) || row.Accounting == nil || len(row.Accounting.Attempts) != len(queries) {
				t.Fatalf("failed population lost: %+v", row)
			}
			for i, attempt := range row.Accounting.Attempts {
				if attempt.Class != class || attempt.Dispatched || attempt.TruthHits != 0 || len(results[i]) != 0 || durations[i] != 0 {
					t.Fatalf("query %d: %+v results=%v duration=%d", i, attempt, results[i], durations[i])
				}
			}
			if resources.WorkerWallNanos != row.ElapsedNanos || resources.Before.SnapshotNanos == 0 || resources.After.SnapshotNanos == 0 {
				t.Fatalf("failed cell lost resource snapshots: %+v", resources)
			}
			if resources.Before.CPUAvailable && resources.After.CPUAvailable {
				if err := resources.validate(); err != nil {
					t.Fatal(err)
				}
			} else if resources.validate() == nil {
				t.Fatal("missing CPU support silently accepted")
			}
			legacy, legacyResults, legacyDurations, err := m8RunProductionCellV1(ctx, nil, assets, queries, truth, 1, 10, 2, 1, 256, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			nilRow, nilResults, nilDurations, err := m8RunProductionCellWithResourcesV1(ctx, nil, assets, queries, truth, 1, 10, 2, 1, 256, 1<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, candidate := range []m8ProductionRowV1{legacy, nilRow} {
				if candidate.Status != row.Status || candidate.Samples != row.Samples || !reflect.DeepEqual(candidate.Accounting.Attempts, row.Accounting.Attempts) {
					t.Fatalf("nil resource seam changed terminal evidence: %+v", candidate)
				}
			}
			if !reflect.DeepEqual(results, legacyResults) || !reflect.DeepEqual(results, nilResults) || !reflect.DeepEqual(durations, legacyDurations) || !reflect.DeepEqual(durations, nilDurations) {
				t.Fatal("nil resource seam changed retained outcomes")
			}
		})
	}
}

func TestM8ServingResourceReductionAndReceiptOutsideSnapshotsV1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const count = 1024
	queries := make([][]float64, count)
	truth := make([][]m8CanonicalResultV1, count)
	var resources m8ServingResourcesV1
	row, results, durations, err := m8RunProductionCellWithResourcesV1(ctx, nil, &m8ProductionMultiGroupAssetsV1{}, queries, truth, 1, 10, 2, 1, 256, 1<<20, &resources)
	if err != nil {
		t.Fatal(err)
	}
	// Read immediately, before the test itself allocates a transcript. At least
	// the retained per-attempt accounting array must have been allocated after
	// the serving snapshot. This catches moving After below response reduction.
	var afterReduction runtime.MemStats
	runtime.ReadMemStats(&afterReduction)
	minimum := uint64(count) * uint64(reflect.TypeFor[m8MeasuredAttemptV1]().Size())
	if afterReduction.TotalAlloc < resources.After.TotalAlloc || afterReduction.TotalAlloc-resources.After.TotalAlloc < minimum {
		t.Fatalf("response accounting included in serving allocation snapshot: after=%d snapshot=%d minimum=%d", afterReduction.TotalAlloc, resources.After.TotalAlloc, minimum)
	}
	frozen := resources
	if err := m8ValidateAttemptAccountingV1(row, 1); err != nil {
		t.Fatal(err)
	}
	outcomes, err := m8ProductionMeasurementTranscriptOutcomesV1(m8ProductionReportV1{Rows: []m8ProductionRowV1{row}}, []m8MeasuredCellV1{{rowIndex: 0, results: results, durations: durations}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(m8ServingResourceCellV1{Kind: "cell", Row: row, Outcomes: outcomes[0], Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	var afterReceipt runtime.MemStats
	runtime.ReadMemStats(&afterReceipt)
	if len(receipt) == 0 || afterReceipt.TotalAlloc <= afterReduction.TotalAlloc || resources != frozen || len(outcomes[0].TopKIDs) != count {
		t.Fatal("validation/receipt work changed serving snapshot or lost failed outcomes")
	}
}

func m8ServingResourceNativeFixtureV1(t testing.TB) (*m8ProductionMultiGroupAssetsV1, *nativewire.VectorPartitionCoordinatorV1, [][]float64, [][]m8CanonicalResultV1) {
	t.Helper()
	requireM8PersistentAssetSupportV1(t)
	f := m8QualificationFixturesV1[0]
	f.Vectors, f.Dimensions, f.Queries = 2048, 128, 16
	vectors, queries := fixtureData(f)
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { assets.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { topology.Close() })
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, queries, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m8WarmProductionTopologyV1(ctx, topology.Coordinator(), assets, queries, config{topK: 10, probes: []int{4}, routerCandidates: defaultRouterScoreBudgetV3, efSearch: []int{96}, concurrency: []int{1}, warmup: 16}); err != nil {
		t.Fatal(err)
	}
	return assets, topology.Coordinator(), queries, truth
}

func TestM8ServingResourceNativeFailedAttemptRetainedV1(t *testing.T) {
	assets, coordinator, queries, truth := m8ServingResourceNativeFixtureV1(t)
	// One invalid-dimensional request must not erase the successful requests or
	// disappear from the resource denominator and retained terminal population.
	queries[len(queries)-1] = []float64{1}
	var resources m8ServingResourcesV1
	row, results, durations, err := m8RunProductionCellWithResourcesV1(context.Background(), coordinator, assets, queries, truth, 4, 96, 2, 10, defaultRouterScoreBudgetV3, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes, &resources)
	var afterReduction runtime.MemStats
	runtime.ReadMemStats(&afterReduction)
	if err != nil {
		t.Fatal(err)
	}
	// Unlike the canceled-cell control, this cell exercises actual coordinator
	// response validation and canonical-result allocation after the snapshot.
	minimum := uint64(len(queries)) * uint64(reflect.TypeFor[m8MeasuredAttemptV1]().Size())
	minimum += uint64((len(queries)-1)*10) * uint64(reflect.TypeFor[m8CanonicalResultV1]().Size())
	if afterReduction.TotalAlloc < resources.After.TotalAlloc || afterReduction.TotalAlloc-resources.After.TotalAlloc < minimum {
		t.Fatal("native response reduction included in serving allocation snapshot")
	}
	if row.Status == "pass" || row.Accounting == nil || row.Samples != len(queries) || len(results) != len(queries) || len(durations) != len(queries) || len(row.Accounting.Attempts) != len(queries) || row.Accounting.Summary.Succeeded != len(queries)-1 {
		t.Fatalf("mixed cell population lost: %+v", row)
	}
	for i, attempt := range row.Accounting.Attempts {
		if !attempt.Dispatched || attempt.TerminalNanos == 0 {
			t.Fatalf("missing terminal %d: %+v", i, attempt)
		}
		if i == len(queries)-1 {
			if attempt.Class == "success" || len(results[i]) != 0 || durations[i] != 0 {
				t.Fatalf("failed request retained success: %+v", attempt)
			}
		} else if attempt.Class != "success" || len(results[i]) != 10 || durations[i] == 0 {
			t.Fatalf("successful request erased: %d %+v", i, attempt)
		}
	}
	if err := resources.validate(); err != nil {
		t.Fatal(err)
	}
	if resources.WorkerWallNanos != row.ElapsedNanos {
		t.Fatal("resource/attempt wall windows differ")
	}
	if err := m8ValidateAttemptAccountingV1(row, 10); err != nil {
		t.Fatal(err)
	}
}

// Whole-cell benchmark ns/op and B/op INCLUDE validation/accounting; the
// optional serving counters exclude it. Setup/truth/warmup are outside both.
func BenchmarkM8ServingResourcesNativeCellV1(b *testing.B) {
	assets, coordinator, queries, truth := m8ServingResourceNativeFixtureV1(b)
	for _, enabled := range []bool{false, true} {
		name := "nil"
		if enabled {
			name = "snapshots"
		}
		b.Run(name, func(b *testing.B) {
			var resources *m8ServingResourcesV1
			if enabled {
				resources = new(m8ServingResourcesV1)
			}
			b.ReportAllocs()
			for b.Loop() {
				row, _, _, err := m8RunProductionCellWithResourcesV1(context.Background(), coordinator, assets, queries, truth, 4, 96, 1, 10, defaultRouterScoreBudgetV3, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes, resources)
				if err != nil || row.Status != "pass" || row.RPCs == 0 || row.LocalScoreCalls == 0 {
					b.Fatalf("invalid native cell: %v %+v", err, row)
				}
				if resources != nil {
					if err := resources.validate(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
