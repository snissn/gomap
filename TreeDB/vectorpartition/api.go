// Package vectorpartition exposes the offline M2 builder to TreeDB commands.
// The implementation remains internal; this is not a server/runtime API.
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

const SchemaVersion = internal.SchemaVersion

func DefaultConfig() Config         { return internal.DefaultConfig() }
func ValidateConfig(c Config) error { return internal.ValidateConfig(c) }
func ValidateInputShape(c Config, vectors, dimensions int) error {
	return internal.ValidateInputShape(c, vectors, dimensions)
}
func ValidateReferenceInputShape(c Config, vectors, dimensions int) error {
	return internal.ValidateReferenceInputShape(c, vectors, dimensions)
}
func Build(v []Vector, c Config) (Artifact, error) { return internal.Build(v, c) }
func BuildWithPartitioner(v []Vector, c Config, s Source, p Partitioner) (Artifact, error) {
	return internal.BuildWithPartitioner(v, c, s, p)
}
func BuildWithPartitionerPhased(v []Vector, c Config, s Source, p Partitioner) (Artifact, PhaseMetrics, error) {
	return internal.BuildWithPartitionerPhased(v, c, s, p)
}
func ValidateArtifact(a Artifact) error        { return internal.ValidateArtifact(a) }
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
func RunExternalJSONForSourceWithLimits(ctx context.Context, command []string, input []byte, limits ExternalJSONLimits, source Source) (Artifact, error) {
	return internal.RunExternalJSONForSourceWithLimits(ctx, command, input, limits, source)
}
