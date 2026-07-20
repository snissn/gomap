// Package vectorpartition exposes the offline M2 builder to TreeDB commands.
// The implementation remains internal; this is not a server/runtime API.
package vectorpartition

import internal "github.com/snissn/gomap/TreeDB/internal/vectorpartition"

type Vector = internal.Vector
type Config = internal.Config
type Graph = internal.Graph
type Metrics = internal.Metrics
type Artifact = internal.Artifact
type Source = internal.Source
type Partitioner = internal.Partitioner
type ReferencePartitioner = internal.ReferencePartitioner

const SchemaVersion = internal.SchemaVersion

func DefaultConfig() Config                        { return internal.DefaultConfig() }
func Build(v []Vector, c Config) (Artifact, error) { return internal.Build(v, c) }
func BuildWithPartitioner(v []Vector, c Config, s Source, p Partitioner) (Artifact, error) {
	return internal.BuildWithPartitioner(v, c, s, p)
}
func ValidateArtifact(a Artifact) error        { return internal.ValidateArtifact(a) }
func CanonicalJSON(a Artifact) ([]byte, error) { return internal.CanonicalJSON(a) }
func Digest(a Artifact) (string, error)        { return internal.Digest(a) }
func DecodeArtifact(raw []byte, maxBytes int) (Artifact, error) {
	return internal.DecodeArtifact(raw, maxBytes)
}
