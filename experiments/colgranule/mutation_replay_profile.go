package colgranule

import "fmt"

type ColumnMutationReplayDurability string

const (
	ColumnMutationReplayDurable   ColumnMutationReplayDurability = "durable"
	ColumnMutationReplayWALOnFast ColumnMutationReplayDurability = "wal_on_fast"
	ColumnMutationReplayFast      ColumnMutationReplayDurability = "fast"
)

type ColumnMutationReplayProfile struct {
	Durability    ColumnMutationReplayDurability
	BenchmarkOnly bool
}

func (p ColumnMutationReplayProfile) Validate() error {
	durability := p.normalizedDurability()
	switch durability {
	case ColumnMutationReplayDurable:
		if p.BenchmarkOnly {
			return fmt.Errorf("colgranule: durable column mutation replay profile cannot be benchmark-only; choose %q or %q with BenchmarkOnly for benchmark-ceiling runs", ColumnMutationReplayWALOnFast, ColumnMutationReplayFast)
		}
		return nil
	case ColumnMutationReplayWALOnFast, ColumnMutationReplayFast:
		if p.BenchmarkOnly {
			return nil
		}
		return fmt.Errorf("colgranule: column mutation replay profile %q is not supported for production; set BenchmarkOnly for benchmark-ceiling runs until safe-root publication is available", durability)
	default:
		return fmt.Errorf("colgranule: unsupported column mutation replay profile %q", durability)
	}
}

func (p ColumnMutationReplayProfile) ProductionSupported() bool {
	return p.normalizedDurability() == ColumnMutationReplayDurable && !p.BenchmarkOnly
}

func (p ColumnMutationReplayProfile) Label() string {
	durability := p.normalizedDurability()
	if p.BenchmarkOnly {
		return string(durability) + "_benchmark_ceiling"
	}
	return string(durability)
}

func (p ColumnMutationReplayProfile) normalizedDurability() ColumnMutationReplayDurability {
	if p.Durability == "" {
		return ColumnMutationReplayDurable
	}
	return p.Durability
}

func (p ColumnMutationReplayProfile) normalized() ColumnMutationReplayProfile {
	p.Durability = p.normalizedDurability()
	return p
}

func (p ColumnMutationReplayProfile) workspaceManifestSyncMode() ColumnWorkspaceManifestSyncMode {
	if p.ProductionSupported() {
		return ColumnWorkspaceManifestSyncDurable
	}
	return ColumnWorkspaceManifestSyncDisabledForBenchmark
}

func columnWorkspaceOptionsForMutationReplayProfile(collection string, profile ColumnMutationReplayProfile) ColumnWorkspaceOptions {
	return ColumnWorkspaceOptions{
		Collection:       collection,
		ManifestSyncMode: profile.normalized().workspaceManifestSyncMode(),
	}
}
