package main

import (
	"errors"
	"math"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWAttributionBuildSchemaV1 = "treedb_local_hnsw_attribution_build_v1"

type localHNSWAttributionBuildEvidenceV1 struct {
	Schema                   string `json:"schema"`
	Variant                  string `json:"variant"`
	VariantIdentity          string `json:"variant_identity"`
	FileID                   uint32 `json:"file_id"`
	Partitions               int    `json:"partitions"`
	ElapsedNanos             int64  `json:"elapsed_nanos"`
	CPUAvailable             bool   `json:"cpu_available"`
	CPUDeltaNanos            int64  `json:"cpu_delta_nanos"`
	AllocBytesDelta          uint64 `json:"alloc_bytes_delta"`
	ProcessPeakRSSBytesAfter int64  `json:"process_peak_rss_bytes_after"`
	PeakRSSAvailable         bool   `json:"peak_rss_available"`
	CloneLogicalBytes        int64  `json:"clone_logical_bytes"`
	PackBytes                uint64 `json:"pack_bytes"`
	MappedBytes              uint64 `json:"mapped_bytes"`
	HeapBytes                uint64 `json:"heap_bytes"`
}

func localHNSWAttributionBuildVariantV1(source *m8ProductionMultiGroupAssetsV1, tempRoot string, variant collections.VectorPartitionLocalGraphVariantV1, fileID uint32) (*localHNSWVariantHarnessV1, localHNSWAttributionBuildEvidenceV1, error) {
	var evidence localHNSWAttributionBuildEvidenceV1
	identity, err := collections.VectorPartitionLocalGraphVariantIdentityV1(variant)
	if err != nil {
		return nil, evidence, err
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, cpuBeforeOK := vectorPartitionBenchmarkCPUNanos()
	started := time.Now()
	harness, err := materializeRetainedLocalHNSWVariantV1(source, tempRoot, variant, fileID)
	elapsed := time.Since(started).Nanoseconds()
	cpuAfter, cpuAfterOK := vectorPartitionBenchmarkCPUNanos()
	runtime.ReadMemStats(&after)
	if err != nil {
		return nil, evidence, err
	}
	if elapsed <= 0 || cpuBeforeOK != cpuAfterOK || cpuBeforeOK && cpuAfter < cpuBefore || after.TotalAlloc < before.TotalAlloc {
		_ = harness.Close()
		return nil, evidence, errors.New("invalid local HNSW attribution build measurement")
	}
	evidence = localHNSWAttributionBuildEvidenceV1{Schema: localHNSWAttributionBuildSchemaV1, Variant: string(variant), VariantIdentity: identity, FileID: fileID, Partitions: len(harness.searchers), ElapsedNanos: elapsed, CPUAvailable: cpuBeforeOK, AllocBytesDelta: after.TotalAlloc - before.TotalAlloc}
	if cpuBeforeOK {
		evidence.CPUDeltaNanos = cpuAfter - cpuBefore
	}
	evidence.ProcessPeakRSSBytesAfter, evidence.PeakRSSAvailable = vectorPartitionBenchmarkPeakRSS()
	evidence.CloneLogicalBytes, err = m3DirectoryBytes(harness.assets.dir)
	if err != nil || evidence.CloneLogicalBytes <= 0 || evidence.Partitions == 0 || evidence.Partitions != len(harness.packAssets) {
		_ = harness.Close()
		return nil, localHNSWAttributionBuildEvidenceV1{}, errors.New("invalid local HNSW attribution build assets")
	}
	for partition, searcher := range harness.searchers {
		status := searcher.Status()
		asset := harness.packAssets[partition]
		if status.PartitionID != uint32(partition) || asset.PartitionID != uint32(partition) || status.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || status.PackBytes != asset.Bytes || status.PackBytes == 0 || math.MaxUint64-evidence.PackBytes < status.PackBytes || math.MaxUint64-evidence.MappedBytes < status.MappedBytes || math.MaxUint64-evidence.HeapBytes < status.HeapBytes {
			_ = harness.Close()
			return nil, localHNSWAttributionBuildEvidenceV1{}, errors.New("invalid local HNSW attribution build status")
		}
		evidence.PackBytes += status.PackBytes
		evidence.MappedBytes += status.MappedBytes
		evidence.HeapBytes += status.HeapBytes
	}
	return harness, evidence, nil
}
