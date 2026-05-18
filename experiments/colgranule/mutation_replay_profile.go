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
			label := "durable"
			if p.Durability == "" {
				label = "durable (default)"
			}
			return fmt.Errorf("colgranule: %s column mutation replay profile cannot be benchmark-only; for benchmark-ceiling runs set Durability to %q or %q (BenchmarkOnly=true is already set)", label, ColumnMutationReplayWALOnFast, ColumnMutationReplayFast)
		}
		return nil
	case ColumnMutationReplayWALOnFast, ColumnMutationReplayFast:
		if p.BenchmarkOnly {
			return nil
		}
		return fmt.Errorf("colgranule: column mutation replay profile %q is not supported for production; set BenchmarkOnly=true for benchmark-ceiling runs until safe-root publication is available", durability)
	default:
		return fmt.Errorf("colgranule: unsupported column mutation replay profile %q", durability)
	}
}

func (p ColumnMutationReplayProfile) ProductionSupported() bool {
	return p.normalizedDurability() == ColumnMutationReplayDurable && !p.BenchmarkOnly
}

// Label returns the normalized profile label and is safe to call before Validate.
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
