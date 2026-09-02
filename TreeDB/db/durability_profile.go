package db

import "fmt"

// DurabilityProfile is the immutable, resolved public durability contract
// carried from the public constructor into the backend. Low-level option
// fields remain implementation details of the selected profile.
type DurabilityProfile string

const (
	ProfileCommandWALDurable DurabilityProfile = "command_wal_durable"
	ProfileCommandWALRelaxed DurabilityProfile = "command_wal_relaxed"
	ProfileNoWALFast         DurabilityProfile = "no_wal_fast"
	ProfileBenchUnsafe       DurabilityProfile = "bench_unsafe"
)

func (profile DurabilityProfile) Valid() bool {
	switch profile {
	case ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast, ProfileBenchUnsafe:
		return true
	default:
		return false
	}
}

func (profile DurabilityProfile) Production() bool {
	switch profile {
	case ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast:
		return true
	default:
		return false
	}
}

func (profile DurabilityProfile) OrdinaryAckClass() string {
	switch profile {
	case ProfileCommandWALDurable:
		return "durable_wal_prefix"
	case ProfileCommandWALRelaxed, ProfileNoWALFast:
		return "relaxed"
	case ProfileBenchUnsafe:
		return "unsafe"
	default:
		return "unknown"
	}
}

func validateResolvedDurabilityProfile(opts Options) error {
	profile := opts.ResolvedProfile
	if profile == "" {
		return nil
	}
	if !profile.Valid() {
		return fmt.Errorf("treedb: unsupported resolved durability profile %q", profile)
	}
	if profile == ProfileBenchUnsafe && !opts.UnsafeBenchmarkProfile {
		return fmt.Errorf("treedb: profile %q requires the explicit benchmark/test constructor boundary", profile)
	}

	wantCommandWAL := false
	wantDurability := DurabilityWALOffRelaxed
	wantIntegrity := IntegrityVerify
	switch profile {
	case ProfileCommandWALDurable:
		wantCommandWAL = true
		wantDurability = DurabilityDurable
	case ProfileCommandWALRelaxed:
		wantCommandWAL = true
		wantDurability = DurabilityWALOnRelaxed
	case ProfileNoWALFast:
		wantDurability = DurabilityWALOffRelaxed
	case ProfileBenchUnsafe:
		wantDurability = DurabilityWALOffRelaxed
		wantIntegrity = opts.ValueLog.ReadIntegrity
	}
	if opts.CommandWAL != wantCommandWAL || opts.Durability != wantDurability || opts.ValueLog.ReadIntegrity != wantIntegrity {
		return fmt.Errorf(
			"treedb: resolved profile %q conflicts with low-level durability options (command_wal=%t durability=%d integrity=%d); rebuild old pre-alpha directories and select one canonical profile",
			profile,
			opts.CommandWAL,
			opts.Durability,
			opts.ValueLog.ReadIntegrity,
		)
	}
	return nil
}
