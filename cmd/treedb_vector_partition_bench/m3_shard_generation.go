package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	m3ShardGenerationFileV1 = "vector_partition_shard_generation_v1.json"
	// The record carries one entry per realized membership, so a 250k corpus at
	// the selected 0.2 overlap ratio holds 300k entries. The cap is a
	// fail-closed bound on decode, not a target.
	m3ShardGenerationMaxBytesV1 = 256 << 20
)

// m3WriteShardGenerationDescriptorV1 persists the versioned shard-generation
// record beside the variant descriptor for a retained byte-bounded build. It is
// the only artifact that carries the realized membership list, its digest, and
// the membership-derived per-pack row/byte summaries, so without it the
// decode-time integrity checks would never protect a real benchmark database.
//
// The record is written immutably and immediately decoded back through the
// same validation a reopen uses, so a database is never retained with a record
// that could not be reopened.
func m3WriteShardGenerationDescriptorV1(dir string, plan vectorpartition.ShardPlanV1, ratio float64, overlap vectorpartition.OverlapResult) (int, error) {
	if plan.Partitions == 0 {
		return 0, nil
	}
	if ratio != plan.OverlapRatio {
		return 0, fmt.Errorf("retained byte-bounded row ratio %.4f does not match the planned ratio %.4f", ratio, plan.OverlapRatio)
	}
	descriptor, err := vectorpartition.NewShardGenerationDescriptorV1(plan, vectorpartition.OverlapConfig{
		Ratio: ratio, Capacity: overlap.Capacity, UsefulOnly: true,
	}, overlap)
	if err != nil {
		return 0, fmt.Errorf("build shard generation descriptor: %w", err)
	}
	raw, err := vectorpartition.CanonicalShardGenerationJSONV1(descriptor)
	if err != nil {
		return 0, fmt.Errorf("encode shard generation descriptor: %w", err)
	}
	raw = append(raw, '\n')
	if len(raw) > m3ShardGenerationMaxBytesV1 {
		return 0, fmt.Errorf("shard generation descriptor %d bytes exceeds the %d-byte cap", len(raw), m3ShardGenerationMaxBytesV1)
	}
	path := filepath.Join(dir, m3ShardGenerationFileV1)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, fmt.Errorf("create immutable shard generation descriptor: %w", err)
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return 0, err
	}
	if _, err := m3ReadShardGenerationDescriptorV1(dir); err != nil {
		return 0, fmt.Errorf("retained shard generation descriptor does not reopen: %w", err)
	}
	return len(raw), nil
}

// m3ReadShardGenerationDescriptorV1 reopens the retained record through the
// package's decode-time validation, which rebinds the plan, the overlap config,
// and every pack summary to the realized membership list.
func m3ReadShardGenerationDescriptorV1(dir string) (vectorpartition.ShardGenerationDescriptorV1, error) {
	raw, err := readBoundedRegularFileV1(filepath.Join(dir, m3ShardGenerationFileV1), m3ShardGenerationMaxBytesV1)
	if err != nil {
		return vectorpartition.ShardGenerationDescriptorV1{}, fmt.Errorf("read shard generation descriptor: %w", err)
	}
	if len(raw) == 0 {
		return vectorpartition.ShardGenerationDescriptorV1{}, errors.New("shard generation descriptor is empty")
	}
	return vectorpartition.DecodeShardGenerationDescriptorV1(raw, len(raw))
}
