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
			return fmt.Errorf("colgranule: %s column mutation replay profile cannot be benchmark-only; for benchmark-ceiling runs set Durability to %q or %q while keeping BenchmarkOnly=true", label, ColumnMutationReplayWALOnFast, ColumnMutationReplayFast)
		}
		return nil
	case ColumnMutationReplayWALOnFast, ColumnMutationReplayFast:
		if p.BenchmarkOnly {
			return nil
		}
		return fmt.Errorf("colgranule: column mutation replay profile %q is not supported for production; production replay must use %q, while %q/%q are benchmark-ceiling-only and require BenchmarkOnly=true with a benchmark no-sync workspace", durability, ColumnMutationReplayDurable, ColumnMutationReplayWALOnFast, ColumnMutationReplayFast)
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

func (p ColumnMutationReplayProfile) workspaceManifestSyncMode() (ColumnWorkspaceManifestSyncMode, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	if p.normalized().ProductionSupported() {
		return ColumnWorkspaceManifestSyncDurable, nil
	}
	return ColumnWorkspaceManifestSyncDisabledForBenchmark, nil
}

func columnWorkspaceOptionsForMutationReplayProfile(collection string, profile ColumnMutationReplayProfile) (ColumnWorkspaceOptions, error) {
	mode, err := profile.workspaceManifestSyncMode()
	if err != nil {
		return ColumnWorkspaceOptions{}, err
	}
	return ColumnWorkspaceOptions{
		Collection:       collection,
		ManifestSyncMode: mode,
	}, nil
}
