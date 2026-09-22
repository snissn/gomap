package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const m8ServingResourceContractV1 = "prospective_colocated_serving_resources_not_qualification_v1"

type m8ProcessResourceSnapshotV1 struct {
	TotalAlloc    uint64 `json:"total_alloc"`
	Mallocs       uint64 `json:"mallocs"`
	CPUNanos      int64  `json:"cpu_nanos"`
	CPUAvailable  bool   `json:"cpu_available"`
	SnapshotNanos uint64 `json:"snapshot_nanos"`
}

type m8ServingResourcesV1 struct {
	Before          m8ProcessResourceSnapshotV1 `json:"before"`
	After           m8ProcessResourceSnapshotV1 `json:"after"`
	WorkerWallNanos uint64                      `json:"worker_wall_nanos"`
}

func m8ServingResourceSnapshotV1() m8ProcessResourceSnapshotV1 {
	started := time.Now()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	cpu, available := vectorPartitionBenchmarkCPUNanos()
	return m8ProcessResourceSnapshotV1{TotalAlloc: memory.TotalAlloc, Mallocs: memory.Mallocs, CPUNanos: cpu, CPUAvailable: available, SnapshotNanos: uint64(time.Since(started))}
}

func (r m8ServingResourcesV1) validate() error {
	if !r.Before.CPUAvailable || !r.After.CPUAvailable || r.Before.CPUNanos < 0 || r.After.CPUNanos < r.Before.CPUNanos || r.After.TotalAlloc < r.Before.TotalAlloc || r.After.Mallocs < r.Before.Mallocs || r.WorkerWallNanos == 0 || r.Before.SnapshotNanos == 0 || r.After.SnapshotNanos == 0 {
		return errors.New("serving resource counters unavailable, decreasing, or incomplete")
	}
	return nil
}

type m8ServingResourceCellV1 struct {
	Kind      string                    `json:"kind"`
	Row       m8ProductionRowV1         `json:"row"`
	Outcomes  m8ProductionRowOutcomesV1 `json:"outcomes"`
	Resources m8ServingResourcesV1      `json:"resources"`
	Error     string                    `json:"error,omitempty"`
}

type m8ServingResourceHeaderV1 struct {
	Kind                   string                       `json:"kind"`
	Contract               string                       `json:"contract"`
	Scope                  string                       `json:"scope"`
	ReplayReceipt          string                       `json:"replay_receipt"`
	HeadSHA                string                       `json:"head_sha"`
	ExecutableSHA256       string                       `json:"executable_sha256"`
	SourceCheckout         string                       `json:"source_checkout"`
	Command                []string                     `json:"command"`
	ParentExecutionID      string                       `json:"parent_execution_id"`
	ParentHeadSHA          string                       `json:"parent_head_sha"`
	ParentExecutableSHA256 string                       `json:"parent_executable_sha256"`
	Dataset                fixtureManifest              `json:"dataset"`
	Variant                *m3VariantDescriptorV1       `json:"variant"`
	Truth                  m8TruthCacheEvidenceV1       `json:"truth"`
	Config                 m8ProductionConfigEvidenceV1 `json:"config"`
	Host                   m8ProductionHostEvidenceV1   `json:"host"`
	GoVersion              string                       `json:"go_version"`
	GOOS                   string                       `json:"goos"`
	GOARCH                 string                       `json:"goarch"`
	GOMAXPROCS             int                          `json:"gomaxprocs"`
	LogicalCPUs            int                          `json:"logical_cpus"`
	Cells                  int                          `json:"cells"`
	ProfileDirectory       string                       `json:"profile_directory"`
	GoMemoryLimitBytes     int64                        `json:"go_memory_limit_bytes"`
}

type m8ServingResourceFooterV1 struct {
	Kind     string                          `json:"kind"`
	Cells    int                             `json:"cells"`
	Profiles []m8ProductionProfileArtifactV1 `json:"profiles"`
}

// New measurements have their own source/binary identity. Strict replay still
// authenticates the OLD producer, executable, assets and transcript unchanged.
// One pass uses repetition zero of every parent coordinate, not a chosen subset.
func runM8ServingResourcesV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("serving-resources", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var out, source, head, executableSHA, profiles string
	fs.StringVar(&out, "out", "", "fresh absolute JSONL receipt")
	fs.StringVar(&source, "source-checkout", "", "clean collector Git toplevel")
	fs.StringVar(&head, "head-sha", "", "frozen collector source revision")
	fs.StringVar(&executableSHA, "executable-sha256", "", "frozen collector executable digest")
	fs.StringVar(&profiles, "profiles", "", "fresh absolute profile directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(out) || !filepath.IsAbs(profiles) || !m8QualificationGitSHAV1(head) || !m8QualificationSHA256V1(executableSHA) || len(fs.Args()) == 0 {
		return errors.New("serving-resources requires fresh absolute out/profiles, collector provenance, and -- followed by all replay-m8-report arguments")
	}
	for _, path := range []string{out, profiles} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("serving resource output must not exist: %s", path)
		}
	}
	profiles, err := m8CanonicalPathV1(profiles)
	if err != nil {
		return err
	}
	checkout, err := m8SourceCheckoutV1(source, head)
	if err != nil || m8GitDirtyInV1(checkout) {
		return errors.New("serving resource collector requires matching clean source")
	}
	executable, err := os.Executable()
	if err != nil || !m8QualificationBenchmarkExecutableV1(filepath.Dir(executable), executable, head, executableSHA) {
		return errors.New("serving resource collector executable does not match frozen clean source/digest")
	}
	var parent m8ProductionReportV1
	var replay bytes.Buffer
	if err := replayM8ReportV1(fs.Args(), &replay, &parent); err != nil {
		return err
	}
	if parent.Variant == nil || parent.Config.MeasurementAccounting != m8CompleteAttemptsV1 || parent.Dataset.Queries > 4096 || parent.Config.TopK != 10 {
		return errors.New("serving resources require retained complete-attempt M8, top-k 10, at most 4096 queries")
	}
	cfg, err := parseConfig(parent.Command[1:])
	if err != nil {
		return err
	}
	queries, err := loadFixtureQueriesV1(parent.DatasetDirectory, parent.Dataset)
	if err != nil {
		return err
	}
	truth, _, err := m8ReadTruthCacheV1(m8TruthCacheArtifactPathV1(parent.TruthCacheDirectory, parent.TruthCache.Identity), parent.Dataset, len(queries), parent.Config.TopK, uint64(parent.Dataset.Vectors), parent.TruthCache.ArtifactSHA256)
	if err != nil {
		return err
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(parent)
	if err != nil {
		return err
	}
	assets, err := m8OpenServingResourceAssetsV1(parent, cfg)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, assets.Close()) }()
	host := m8ProductionHostV1(config{out: filepath.Dir(out), dataset: parent.DatasetDirectory}, assets.dir)
	if host != parent.Host || runtime.Version() != parent.GoVersion || runtime.GOOS != parent.GOOS || runtime.GOARCH != parent.GOARCH || runtime.NumCPU() != parent.LogicalCPUs || runtime.GOMAXPROCS(0) != parent.GOMAXPROCS || debug.SetMemoryLimit(-1) != parent.GoMemoryLimitBytes {
		return errors.New("serving resources require the parent host, mounts and runtime settings")
	}
	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	write := func(value any) error {
		if err := encoder.Encode(value); err != nil {
			return err
		}
		return file.Sync()
	}
	measuredConfig := parent.Config
	measuredConfig.MeasuredRepetitions = 1
	order := m8MeasurementOrderV1(measuredConfig)
	if len(order) < 1 || len(order) > 32 || len(parent.Rows) < len(order) || len(transcript.Outcomes) < len(order) {
		return errors.New("serving resources require at most 32 complete parent coordinates")
	}
	for i := range order {
		if !m8MeasuredCoordinateCompleteV1(parent, parent.Rows[i]) {
			return errors.New("serving resources require every parent repetition to pass completion and declared quality")
		}
	}
	header := m8ServingResourceHeaderV1{
		Kind: "header", Contract: m8ServingResourceContractV1,
		Scope:         "whole co-located process during bounded native M8 workers: request preparation, coordinator/client, TCP shards, Raft/background and Go runtime/GC; excludes setup, warmup, preallocated outcome slots, post-response validation, attribution and receipt encoding; CPU snapshot follows ReadMemStats, so CPU delta includes final snapshot overhead; wall excludes both snapshots; no forced GC or idle subtraction; standard M8 CPU/block/mutex/trace enabled; one prospective observation, not historical allocations or resource-improvement significance",
		ReplayReceipt: replay.String(), HeadSHA: head, ExecutableSHA256: executableSHA, SourceCheckout: checkout,
		Command: append([]string{executable, "serving-resources"}, args...), ParentExecutionID: parent.ExecutionID, ParentHeadSHA: parent.HeadSHA, ParentExecutableSHA256: parent.ExecutableSHA256,
		Dataset: parent.Dataset, Variant: assets.descriptor, Truth: parent.TruthCache, Config: measuredConfig, Host: host,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), LogicalCPUs: runtime.NumCPU(), Cells: len(order), GoMemoryLimitBytes: debug.SetMemoryLimit(-1), ProfileDirectory: profiles,
	}
	if err := write(header); err != nil {
		return err
	}
	offlineGraphVariant := collections.VectorPartitionLocalGraphVariantV1("")
	if cfg.m8FinalOfflineGraph {
		offlineGraphVariant = assets.graphVariant
	}
	topologyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(topologyCtx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{
		Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests,
		Database: "default", Catalog: "default", CoordinatorLimits: cfg.m8CoordinatorLimits, ShardLimits: cfg.m8ShardLimits, OfflineGraphVariant: offlineGraphVariant,
	})
	cancel()
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, topology.Close()) }()
	if _, err := m8WarmProductionTopologyV1(context.Background(), topology.Coordinator(), assets, queries, cfg); err != nil {
		return err
	}
	profile, err := startM8ProfileCaptureV1(profiles)
	if err != nil {
		return err
	}
	defer func() { _, err := profile.Stop(); runErr = errors.Join(runErr, err) }()
	for index, coordinate := range order {
		cell := m8ServingResourceCellV1{Kind: "cell"}
		row, results, durations, cellErr := m8RunProductionCellWithResourcesV1(context.Background(), topology.Coordinator(), assets, queries, truth, coordinate.probes, coordinate.efSearch, coordinate.concurrency, cfg.topK, cfg.routerCandidates, cfg.m8CoordinatorLimits.MaxCandidateBytes, &cell.Resources)
		row.Overlap, row.VariantID = parent.Rows[index].Overlap, assets.descriptor.VariantID
		cell.Row = row
		outcomes, outcomeErr := m8ProductionMeasurementTranscriptOutcomesV1(m8ProductionReportV1{Rows: []m8ProductionRowV1{row}}, []m8MeasuredCellV1{{rowIndex: 0, results: results, durations: durations}})
		if outcomeErr == nil {
			cell.Outcomes = outcomes[0]
		}
		cellErr = errors.Join(cellErr, outcomeErr, cell.Resources.validate(), m8ServingResourceCellMatchesV1(cell, parent.Rows[index], transcript.Outcomes[index]))
		if cellErr != nil {
			cell.Error = cellErr.Error()
		}
		if err := write(cell); err != nil {
			return errors.Join(cellErr, err)
		}
		if cellErr != nil {
			return cellErr // All observed failures remain; missing footer is incomplete.
		}
	}
	paths, err := profile.Stop()
	if err != nil {
		return err
	}
	artifacts, err := m8ProfileArtifactsV1(paths)
	if err != nil {
		return err
	}
	if err := write(m8ServingResourceFooterV1{"complete", len(order), artifacts}); err != nil {
		return err
	}
	receipt, err := os.Open(out)
	if err != nil {
		return err
	}
	_, verifyErr := m8ReadServingResourceCellsV1(receipt, header, parent, transcript)
	if err := errors.Join(verifyErr, receipt.Close()); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "SERVING_RESOURCE_OBSERVATIONS_NOT_QUALIFICATION", out)
	return err
}

func m8OpenServingResourceAssetsV1(parent m8ProductionReportV1, cfg config) (*m8ProductionMultiGroupAssetsV1, error) {
	vectors, err := loadFixtureVectorsV1(parent.DatasetDirectory, parent.Dataset)
	if err != nil {
		return nil, err
	}
	groups := make([]string, len(parent.Topology.Groups))
	for i, group := range parent.Topology.Groups {
		groups[i] = group.GroupID
	}
	// Use the same validated serving view as the producer: retained M3
	// placements are local, not the parent's multi-group placements or digests.
	assets, err := openM8ProductionMultiGroupExistingAssetsWithPolicyV1(parent.Variant.DatabaseDirectory, groups, cfg.partitions, parent.Dataset, vectors, cfg.m8FinalOfflineGraph)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(assets.descriptor, parent.Variant) {
		return nil, errors.Join(errors.New("serving resource assets changed after replay"), assets.Close())
	}
	return assets, nil
}

func m8ServingResourceCellMatchesV1(cell m8ServingResourceCellV1, parent m8ProductionRowV1, expected m8ProductionRowOutcomesV1) error {
	if cell.Kind != "cell" || cell.Error != "" || cell.Row.Repetition != 0 || parent.Repetition != 0 || !m8SameMeasurementCoordinateV1(cell.Row, parent) || cell.Row.Samples != parent.Samples || cell.Row.Samples < 1 || cell.Row.VariantID != parent.VariantID || cell.Row.Accounting == nil || cell.Row.Accounting.Summary.Succeeded != cell.Row.Samples || cell.Row.Status != "pass" || parent.Status != "pass" || cell.Row.RecallAtK != parent.RecallAtK || !reflect.DeepEqual(cell.Outcomes.TopKIDs, expected.TopKIDs) || !reflect.DeepEqual(cell.Outcomes.TopKScoreBits, expected.TopKScoreBits) || cell.Resources.WorkerWallNanos != cell.Row.ElapsedNanos {
		return errors.New("serving resource cell failed completion, parent coordinate, or exact result parity")
	}
	identity := cell.Outcomes
	identity.TopKIDs, identity.TopKScoreBits, identity.TotalNanos, identity.ExactRepresentativeTruthHits = nil, nil, nil, nil
	if !reflect.DeepEqual(identity, m8ProductionRowOutcomeIdentityV1(cell.Row)) || len(cell.Outcomes.TotalNanos) != cell.Row.Samples || len(cell.Outcomes.TopKIDs) != cell.Row.Samples || len(cell.Row.Accounting.Attempts) != cell.Row.Samples || parent.Accounting == nil || len(parent.Accounting.Attempts) != cell.Row.Samples {
		return errors.New("serving resource outcomes do not match their cell")
	}
	for i, attempt := range cell.Row.Accounting.Attempts {
		if cell.Outcomes.TotalNanos[i] != attempt.CoordinatorNanos || len(cell.Outcomes.TopKIDs[i]) != attempt.ReturnedResults || attempt.TruthHits != parent.Accounting.Attempts[i].TruthHits {
			return errors.New("serving resource outcomes do not match measured attempts")
		}
	}
	return nil
}

// expected is the independently frozen header; parent/transcript must come
// from strict parent replay. A truncated/duplicated stream is never a subset
// success. This checks the receipt, not source authenticity without those pins.
func m8ReadServingResourceCellsV1(input io.Reader, expected m8ServingResourceHeaderV1, parent m8ProductionReportV1, transcript m8ProductionMeasurementTranscriptV1) ([]m8ServingResourceCellV1, error) {
	bounded := &io.LimitedReader{R: input, N: m8CompleteMeasurementMaxBytesV1 + 1}
	decoder := json.NewDecoder(bounded)
	decoder.DisallowUnknownFields()
	var header m8ServingResourceHeaderV1
	if err := decoder.Decode(&header); err != nil {
		return nil, err
	}
	config := parent.Config
	config.MeasuredRepetitions = 1
	if !reflect.DeepEqual(header, expected) || header.Kind != "header" || header.Contract != m8ServingResourceContractV1 || header.ParentExecutionID != parent.ExecutionID || !reflect.DeepEqual(header.Dataset, parent.Dataset) || !reflect.DeepEqual(header.Config, config) || header.Cells != len(m8MeasurementOrderV1(config)) || header.Cells < 1 || header.Cells > 32 || len(parent.Rows) < header.Cells || len(transcript.Outcomes) < header.Cells {
		return nil, errors.New("serving resource header differs from frozen plan or parent")
	}
	cells := make([]m8ServingResourceCellV1, header.Cells)
	for i := range cells {
		if !m8MeasuredCoordinateCompleteV1(parent, parent.Rows[i]) {
			return nil, errors.New("serving resource parent contains incomplete or low-quality repetitions")
		}
		if err := decoder.Decode(&cells[i]); err != nil {
			return nil, err
		}
		if err := errors.Join(cells[i].Resources.validate(), m8ValidateAttemptAccountingV1(cells[i].Row, parent.Config.TopK), m8ServingResourceCellMatchesV1(cells[i], parent.Rows[i], transcript.Outcomes[i])); err != nil {
			return nil, err
		}
	}
	var footer m8ServingResourceFooterV1
	if err := decoder.Decode(&footer); err != nil {
		return nil, err
	}
	if footer.Kind != "complete" || footer.Cells != header.Cells {
		return nil, errors.New("serving resource receipt has no complete footer")
	}
	paths := make([]string, len(footer.Profiles))
	for i, artifact := range footer.Profiles {
		if !filepath.IsAbs(artifact.Path) || filepath.Dir(artifact.Path) != header.ProfileDirectory {
			return nil, errors.New("serving resource profile outside frozen directory")
		}
		paths[i] = artifact.Path
	}
	actual, err := m8ProfileArtifactsV1(paths)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual, footer.Profiles) {
		return nil, errors.New("serving resource profiles changed")
	}
	if err := decoder.Decode(new(any)); err != io.EOF || bounded.N == 0 {
		return nil, errors.New("serving resource receipt contains trailing data")
	}
	return cells, nil
}
