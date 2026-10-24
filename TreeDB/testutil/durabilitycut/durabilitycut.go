// Package durabilitycut exposes the maintained internal durability observation
// seam to deterministic integration tests in other modules. It adds no event
// source, persistence behavior or production configuration. Tests must not run
// in parallel while an observer is installed, and must call the returned restore
// function. Stable file handles in Event are borrowed only during the callback.
package durabilitycut

import internal "github.com/snissn/gomap/TreeDB/internal/durabilitycut"

type Event = internal.Event
type Point = internal.Point
type Resource = internal.Resource
type Observer = internal.Observer

const (
	AfterLogicalMVCCStaging      = internal.AfterLogicalMVCCStaging
	AfterDependencyAppend        = internal.AfterDependencyAppend
	BeforeCOWPreparation         = internal.BeforeCOWPreparation
	AfterCOWPreparation          = internal.AfterCOWPreparation
	AfterCOWCanonicalPreparation = internal.AfterCOWCanonicalPreparation
	BeforeDependencyFileSync     = internal.BeforeDependencyFileSync
	AfterDependencyFileSync      = internal.AfterDependencyFileSync
	ResourceCommandWAL           = internal.ResourceCommandWAL
)

// Install serializes process-global observers and returns a mandatory restore
// function. The callback runs synchronously at the original production boundary.
func Install(observer Observer) func() { return internal.Install(observer) }
