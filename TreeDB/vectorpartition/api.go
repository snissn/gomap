// Package vectorpartition exposes the offline M2 builder, deterministic M4
// representative coarsening core, and the supported V1 partition search and
// generation lifecycle contract. Node construction supplies one opaque
// production backend; ordinary callers never construct transport, Raft, or
// lifecycle internals.
//
// TreeDB is pre-alpha. These APIs may change without compatibility guarantees.
package vectorpartition

import (
	"context"
	internal "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

type Vector = internal.Vector
type Config = internal.Config
type Graph = internal.Graph
type Metrics = internal.Metrics
type Artifact = internal.Artifact
type Source = internal.Source
type PhaseMetrics = internal.PhaseMetrics
type Partitioner = internal.Partitioner
type ReferencePartitioner = internal.ReferencePartitioner
type ExternalJSONLimits = internal.ExternalJSONLimits
type RouterConfigV1 = internal.RouterConfigV1
type RouterVectorV1 = internal.RouterVectorV1
type RouterPartitionV1 = internal.RouterPartitionV1
type RouterHierarchyNodeV1 = internal.RouterHierarchyNodeV1
type RouterRepresentativeV1 = internal.RouterRepresentativeV1
type RouterBuildMetricsV1 = internal.RouterBuildMetricsV1
type RouterModelV1 = internal.RouterModelV1
type RouterPartitionScoreV1 = internal.RouterPartitionScoreV1
type RouterRouteResultV1 = internal.RouterRouteResultV1
type OverlapConfig = internal.OverlapConfig
type Membership = internal.Membership
type Replica = internal.Replica
type ReplicaUtilityClassV1 = internal.ReplicaUtilityClassV1
type OverlapResult = internal.OverlapResult
type OverlapShortfallError = internal.OverlapShortfallError
type ShardPlanInputV1 = internal.ShardPlanInputV1
type ShardPlanRequestV1 = internal.ShardPlanRequestV1
type ShardPlanV1 = internal.ShardPlanV1
type ShardPackSummaryV1 = internal.ShardPackSummaryV1
type ShardGenerationDescriptorV1 = internal.ShardGenerationDescriptorV1

const (
	SchemaVersion                     = internal.SchemaVersion
	MaxOverlapMembershipsPerVector    = internal.MaxOverlapMembershipsPerVector
	ReplicaUtilityPositiveGainV1      = internal.ReplicaUtilityPositiveGainV1
	ReplicaUtilityZeroUtilityV1       = internal.ReplicaUtilityZeroUtilityV1
	FP32BytesPerDimensionV1           = internal.FP32BytesPerDimensionV1
	FP32VectorSectionAlignmentBytesV1 = internal.FP32VectorSectionAlignmentBytesV1
	GraphIdentityOverheadPerRowV1     = internal.GraphIdentityOverheadPerRowV1
	GraphIdentityOverheadBytesV1      = internal.GraphIdentityOverheadBytesV1
	PackFixedOverheadBytesV1          = internal.PackFixedOverheadBytesV1
	DefaultFP32DimensionsV1           = internal.DefaultFP32DimensionsV1
	DefaultTargetHotBytesV1           = internal.DefaultTargetHotBytesV1
	SelectedTargetHotBytesV1          = internal.SelectedTargetHotBytesV1
	SelectedOverlapRatioV1            = internal.SelectedOverlapRatioV1
	SelectedRouterCandidatesV1        = internal.SelectedRouterCandidatesV1
	SelectedPartitionProbesV1         = internal.SelectedPartitionProbesV1
	SelectedSearchableRowsPerPackV1   = internal.SelectedSearchableRowsPerPackV1
	ShardGenerationDescriptorSchemaV1 = internal.ShardGenerationDescriptorSchemaV1
	ShardGenerationDescriptorKindV1   = internal.ShardGenerationDescriptorKindV1
)

func DefaultConfig() Config         { return internal.DefaultConfig() }
func ValidateConfig(c Config) error { return internal.ValidateConfig(c) }
func DefaultRouterConfigV1() RouterConfigV1 {
	return internal.DefaultRouterConfigV1()
}
func ValidateRouterConfigV1(c RouterConfigV1) error {
	return internal.ValidateRouterConfigV1(c)
}
func BuildRouterV1(p []RouterPartitionV1, c RouterConfigV1) (RouterModelV1, error) {
	return internal.BuildRouterV1(p, c)
}
func ValidateRouterModelV1(m RouterModelV1) error { return internal.ValidateRouterModelV1(m) }
func ValidateRouterModelWithContextV1(ctx context.Context, m RouterModelV1) error {
	return internal.ValidateRouterModelWithContextV1(ctx, m)
}
func CanonicalRouterJSONV1(m RouterModelV1) ([]byte, error) {
	return internal.CanonicalRouterJSONV1(m)
}
func CanonicalRouterJSONWithContextV1(ctx context.Context, m RouterModelV1) ([]byte, error) {
	return internal.CanonicalRouterJSONWithContextV1(ctx, m)
}
func RouterDigestV1(m RouterModelV1) (string, error) { return internal.RouterDigestV1(m) }
func RouterDigestWithContextV1(ctx context.Context, m RouterModelV1) (string, error) {
	return internal.RouterDigestWithContextV1(ctx, m)
}
func RouteExactV1(m RouterModelV1, query []float32, candidateBudget, partitionProbes int) (RouterRouteResultV1, error) {
	return internal.RouteExactV1(m, query, candidateBudget, partitionProbes)
}
func ValidateInputShape(c Config, vectors, dimensions int) error {
	return internal.ValidateInputShape(c, vectors, dimensions)
}
func ValidateReferenceInputShape(c Config, vectors, dimensions int) error {
	return internal.ValidateReferenceInputShape(c, vectors, dimensions)
}

// VectorBitsFingerprintV1 exposes the builder's canonical float-bit
// fingerprint to repository commands that emit matching diagnostics.
func VectorBitsFingerprintV1(values []float64) [32]byte {
	return internal.VectorBitsFingerprintV1(values)
}
func Build(v []Vector, c Config) (Artifact, error) { return internal.Build(v, c) }
func BuildWithPartitioner(v []Vector, c Config, s Source, p Partitioner) (Artifact, error) {
	return internal.BuildWithPartitioner(v, c, s, p)
}
func BuildWithPartitionerPhased(v []Vector, c Config, s Source, p Partitioner) (Artifact, PhaseMetrics, error) {
	return internal.BuildWithPartitionerPhased(v, c, s, p)
}
func RepartitionArtifact(a Artifact, partitions int, p Partitioner) (Artifact, error) {
	return internal.RepartitionArtifact(a, partitions, p)
}
func ValidateArtifact(a Artifact) error { return internal.ValidateArtifact(a) }
func BuildStableIDHashBaseline(a Artifact) (Artifact, error) {
	return internal.BuildStableIDHashBaseline(a)
}
func BuildOverlap(a Artifact, c OverlapConfig) (OverlapResult, error) {
	return internal.BuildOverlap(a, c)
}
func DefaultShardPlanInputV1(vectors, dimensions int) ShardPlanInputV1 {
	return internal.DefaultShardPlanInputV1(vectors, dimensions)
}
func SelectedShardPlanRequestV1(sourceRows, dimensions int) ShardPlanRequestV1 {
	return internal.SelectedShardPlanRequestV1(sourceRows, dimensions)
}
func AlignedTraversalRowBytesV1(dimensions int) (int, bool) {
	return internal.AlignedTraversalRowBytesV1(dimensions)
}
func PlanByteBoundedShardsV1(in ShardPlanRequestV1) (ShardPlanV1, error) {
	return internal.PlanByteBoundedShardsV1(in)
}
func SelectedOverlapConfigV1(capacity int) OverlapConfig {
	return internal.SelectedOverlapConfigV1(capacity)
}
func AccountShardPacksV1(plan ShardPlanV1, memberships []Membership) ([]ShardPackSummaryV1, error) {
	return internal.AccountShardPacksV1(plan, memberships)
}
func NewShardGenerationDescriptorV1(plan ShardPlanV1, cfg OverlapConfig, overlap OverlapResult) (ShardGenerationDescriptorV1, error) {
	return internal.NewShardGenerationDescriptorV1(plan, cfg, overlap)
}
func CanonicalShardGenerationJSONV1(d ShardGenerationDescriptorV1) ([]byte, error) {
	return internal.CanonicalShardGenerationJSONV1(d)
}
func DecodeShardGenerationDescriptorV1(raw []byte, maxBytes int) (ShardGenerationDescriptorV1, error) {
	return internal.DecodeShardGenerationDescriptorV1(raw, maxBytes)
}
func ValidateShardGenerationDescriptorV1(d ShardGenerationDescriptorV1) error {
	return internal.ValidateShardGenerationDescriptorV1(d)
}
func MembershipDigestV1(memberships []Membership) (string, error) {
	return internal.MembershipDigestV1(memberships)
}
func RouterPartitionsFromMembershipsV1(memberships []Membership, values [][]float32, partitions int) ([]RouterPartitionV1, error) {
	return internal.RouterPartitionsFromMembershipsV1(memberships, values, partitions)
}
func ValidateOverlap(a Artifact, c OverlapConfig, r OverlapResult) error {
	return internal.ValidateOverlap(a, c, r)
}
func CanonicalJSON(a Artifact) ([]byte, error) { return internal.CanonicalJSON(a) }
func Digest(a Artifact) (string, error)        { return internal.Digest(a) }
func DecodeArtifact(raw []byte, maxBytes int) (Artifact, error) {
	return internal.DecodeArtifact(raw, maxBytes)
}
func RunExternalJSON(ctx context.Context, command []string, input []byte, maxOutput int) (Artifact, error) {
	return internal.RunExternalJSON(ctx, command, input, maxOutput)
}
func RunExternalJSONForSource(ctx context.Context, command []string, input []byte, maxOutput int, source Source) (Artifact, error) {
	return internal.RunExternalJSONForSource(ctx, command, input, maxOutput, source)
}

// RunExternalJSONForSourceWithLimits fails closed without running a backend.
// Use RunExternalJSONForRequestWithLimits for a fully bound request.
func RunExternalJSONForSourceWithLimits(ctx context.Context, command []string, input []byte, limits ExternalJSONLimits, source Source) (Artifact, error) {
	return internal.RunExternalJSONForSourceWithLimits(ctx, command, input, limits, source)
}
func RunExternalJSONForRequestWithLimits(ctx context.Context, command []string, input []byte, limits ExternalJSONLimits, request Artifact) (Artifact, error) {
	return internal.RunExternalJSONForRequestWithLimits(ctx, command, input, limits, request)
}
