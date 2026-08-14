package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	m3ShardGenerationFileV1 = "vector_partition_shard_generation_v1.json"
	// The record carries one entry per realized membership, so a 250k corpus at
	// the selected 0.2 overlap ratio holds 300k entries. The cap is a
	// fail-closed bound on decode, not a target.
	m3ShardGenerationMaxBytesV1 = 256 << 20
)

// m3ShardGenerationRecordV1 encodes the versioned shard-generation record for a
// byte-bounded build and returns it with its SHA-256. It is the only artifact
// carrying the realized membership list, its digest, and the membership-derived
// per-pack row/byte summaries, so without it the decode-time integrity checks
// would never protect a real benchmark database.
//
// It is produced before the build-identity digest so that digest binds the
// record, and a retained row may materialize less than the planned envelope but
// never more.
func m3ShardGenerationRecordV1(plan vectorpartition.ShardPlanV1, ratio float64, overlap vectorpartition.OverlapResult) ([]byte, string, error) {
	if plan.Partitions == 0 {
		return nil, "", nil
	}
	if ratio > plan.OverlapRatio {
		return nil, "", fmt.Errorf("retained row ratio %.4f exceeds the planned envelope %.4f", ratio, plan.OverlapRatio)
	}
	descriptor, err := vectorpartition.NewShardGenerationDescriptorV1(plan, vectorpartition.OverlapConfig{
		Ratio: ratio, Capacity: overlap.Capacity, UsefulOnly: true,
	}, overlap)
	if err != nil {
		return nil, "", fmt.Errorf("build shard generation descriptor: %w", err)
	}
	raw, err := vectorpartition.CanonicalShardGenerationJSONV1(descriptor)
	if err != nil {
		return nil, "", fmt.Errorf("encode shard generation descriptor: %w", err)
	}
	raw = append(raw, '\n')
	if len(raw) > m3ShardGenerationMaxBytesV1 {
		return nil, "", fmt.Errorf("shard generation descriptor %d bytes exceeds the %d-byte cap", len(raw), m3ShardGenerationMaxBytesV1)
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func m3WriteShardGenerationRecordV1(dir string, raw []byte, digest string) error {
	if len(raw) == 0 {
		return nil
	}
	path := filepath.Join(dir, m3ShardGenerationFileV1)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create immutable shard generation descriptor: %w", err)
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return err
	}
	if _, err := m3ReadShardGenerationDescriptorV1(dir, digest); err != nil {
		return fmt.Errorf("retained shard generation descriptor does not reopen: %w", err)
	}
	return nil
}

// m3VerifyRetainedShardGenerationV1 requires a byte-bounded retained database to
// still carry the generation record its variant descriptor was built with.
//
// Comparison variants deliberately share one plan, so matching the plan alone
// would accept a valid record belonging to a different variant. Every
// membership fact the record carries is therefore cross-checked against the
// descriptor's own accounting, which the build-identity digest already binds.
func m3VerifyRetainedShardGenerationV1(dir string, d m3VariantDescriptorV1) error {
	if d.ShardPlan == (vectorpartition.ShardPlanV1{}) {
		return nil
	}
	record, err := m3ReadShardGenerationDescriptorV1(dir, d.ShardGenerationDigest)
	if err != nil {
		return err
	}
	if record.Plan != d.ShardPlan {
		return errors.New("retained shard generation record does not describe the descriptor's plan")
	}
	if record.OverlapConfig.Ratio != d.OverlapRatio || record.OverlapConfig.Capacity != d.Capacity {
		return fmt.Errorf("retained shard generation record requests ratio %v capacity %d, descriptor declares %v/%d",
			record.OverlapConfig.Ratio, record.OverlapConfig.Capacity, d.OverlapRatio, d.Capacity)
	}
	if len(record.PackSummaries) != len(d.PartitionLoads) {
		return fmt.Errorf("retained shard generation record covers %d packs, descriptor declares %d", len(record.PackSummaries), len(d.PartitionLoads))
	}
	realizedOverlap := 0
	for partition, summary := range record.PackSummaries {
		if summary.Rows != d.PartitionLoads[partition] {
			return fmt.Errorf("retained shard generation pack %d holds %d rows, descriptor declares %d", partition, summary.Rows, d.PartitionLoads[partition])
		}
		realizedOverlap += summary.OverlapRows
	}
	if realizedOverlap != d.OverlapRealized || realizedOverlap != d.OverlapMemberships {
		return fmt.Errorf("retained shard generation record realizes %d non-home memberships, descriptor declares realized=%d memberships=%d",
			realizedOverlap, d.OverlapRealized, d.OverlapMemberships)
	}
	if uint64(record.Plan.Vectors) != d.SourceRows {
		return fmt.Errorf("retained shard generation record covers %d vectors, descriptor declares %d source rows", record.Plan.Vectors, d.SourceRows)
	}
	return nil
}

// m3VerifyShardGenerationMembershipsV1 compares the record's individual
// membership pairs against the per-partition artifact ordinals that actually
// materialized the packs.
//
// The aggregate checks in m3VerifyRetainedShardGenerationV1 cannot separate two
// assignments that happen to share per-partition loads and overlap totals, so
// swapping equal numbers of vectors between partitions would survive them. The
// membership lists are only both in scope during construction — a reopen has
// neither the manifest nor the artifact/source ordinal mapping — so this is the
// build-time half of the binding.
func m3VerifyShardGenerationMembershipsV1(record vectorpartition.ShardGenerationDescriptorV1, membershipOrdinals [][]int) error {
	if len(membershipOrdinals) != len(record.PackSummaries) {
		return fmt.Errorf("shard generation record covers %d packs, materialization used %d", len(record.PackSummaries), len(membershipOrdinals))
	}
	perPartition := make([][]int, len(membershipOrdinals))
	for _, membership := range record.Memberships {
		if membership.Partition < 0 || membership.Partition >= len(perPartition) {
			return fmt.Errorf("shard generation membership names partition %d outside the materialized %d", membership.Partition, len(perPartition))
		}
		perPartition[membership.Partition] = append(perPartition[membership.Partition], membership.VectorOrdinal)
	}
	for partition, want := range membershipOrdinals {
		got := perPartition[partition]
		if len(got) != len(want) {
			return fmt.Errorf("shard generation pack %d lists %d memberships, materialization used %d", partition, len(got), len(want))
		}
		sorted := append([]int(nil), want...)
		sort.Ints(sorted)
		sort.Ints(got)
		for i := range sorted {
			if got[i] != sorted[i] {
				return fmt.Errorf("shard generation pack %d membership %d is vector %d, materialization used %d", partition, i, got[i], sorted[i])
			}
		}
	}
	return nil
}

// m3ReadShardGenerationDescriptorV1 reopens the retained record through the
// package's decode-time validation, which rebinds the plan, the overlap config,
// and every pack summary to the realized membership list.
func m3ReadShardGenerationDescriptorV1(dir, digest string) (vectorpartition.ShardGenerationDescriptorV1, error) {
	if !m8SHA256V1(digest) {
		return vectorpartition.ShardGenerationDescriptorV1{}, errors.New("shard generation descriptor digest is not a SHA-256")
	}
	raw, err := readBoundedRegularFileV1(filepath.Join(dir, m3ShardGenerationFileV1), m3ShardGenerationMaxBytesV1)
	if err != nil {
		return vectorpartition.ShardGenerationDescriptorV1{}, fmt.Errorf("read shard generation descriptor: %w", err)
	}
	if len(raw) == 0 {
		return vectorpartition.ShardGenerationDescriptorV1{}, errors.New("shard generation descriptor is empty")
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != digest {
		return vectorpartition.ShardGenerationDescriptorV1{}, errors.New("shard generation descriptor digest mismatch")
	}
	return vectorpartition.DecodeShardGenerationDescriptorV1(raw, len(raw))
}
