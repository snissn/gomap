package db

import (
	"math"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// PreparedRootPublicationBaseProfile counts existing state that a prepared
// command may copy or scan during ordered root publication. It deliberately
// counts registered value-log files, including zombies, and all historical
// leaf-generation records rather than only currently active generations.
// These are source counts, not a byte reservation or a bound on newly produced
// pages and segments.
type PreparedRootPublicationBaseProfile struct {
	PagerPages              uint64
	RegisteredValueLogFiles uint64
	LeafGenerations         uint64
	LeafGenerationFileIDs   uint64
	PendingLeafFileIDs      uint64
	VisibleResources        uint64
	VisibleMembers          uint64
	AllocatorDebt           uint64
	Seals                   uint64
	SealPrefixEntries       uint64
	FreelistCOW             freelist.COWPrepareProfileV1
	ValueLogRead            valuelog.PreparedReadProfile
}

// PreparedRootPublicationLimits is a checked allocation ceiling for the
// prepared command-WAL publisher. A nil limits pointer leaves ordinary
// publication unchanged; a prepared caller must establish the pre-WAL
// allowance before enabling a limit; a later concurrent registration over
// the ceiling is a post-append publication failure, not a batch-size retry.
type PreparedRootPublicationLimits struct {
	MaxRegisteredValueLogFiles int
	MaxVisibleResources        int
	MaxPendingLeafFileIDs      int
	MaxLeafGenerations         int
	MaxLeafGenerationFileIDs   int
	MaxDescriptorEntries       int
	MaxDescriptorBytes         int64
	MaxDescriptorRootIDs       int
	MaxTotalOutputPages        uint64
	MaxVisibleMembers          int
	MaxAllocatorDebt           int
	MaxSeals                   int
	MaxSealPrefixEntries       int
	FreelistCOW                freelist.COWPrepareLimitsV1
	// InitialPointCensusLimit opts the already-materialized caller roots into
	// pre-WAL profiling under writeMu. A zero limit preserves manually supplied
	// profiles for focused publisher tests.
	InitialPointCensusLimit PreparedRootPointCensusLimit
	// ContextPointProfiles are supplied by the caller for roots whose deltas
	// depend on the assigned command LSN. They follow the initial roots.
	ContextPointProfiles []PreparedRootPointProfile
	// RootPointProfiles are filled by the serialized pre-WAL preflight in
	// publication order: caller roots, then context roots. They bind a
	// source-derived pure-point output ceiling to the exact captured bases.
	RootPointProfiles  []PreparedRootPointProfile
	SystemPointProfile PreparedRootPointProfile
	// The system delta is built after the command WAL append. Its source
	// shape must be admitted by the caller before append; these limits guard
	// the materialized batch against a violated source bound.
	MaxSystemDeltaEntries int
	MaxSystemDeltaBytes   int64
}

func (db *DB) currentValueLogSetForPublication(limits *PreparedRootPublicationLimits) (*valuelog.Set, error) {
	if db == nil || db.valueLogManager == nil {
		return nil, nil
	}
	if limits == nil {
		return db.valueLogManager.CurrentSetNoRefresh(), nil
	}
	return db.valueLogManager.CurrentSetNoRefreshWithMaxFiles(limits.MaxRegisteredValueLogFiles)
}

func preparedProfileAdd(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

// PreparedFreelistCOWLimitsForPages creates the uniform finite ceiling used by
// a prepared publisher after it has bounded base plus newly produced pages.
func PreparedFreelistCOWLimitsForPages(pages uint64) freelist.COWPrepareLimitsV1 {
	return freelist.COWPrepareLimitsV1{
		MaxHighWater: pages, MaxRetiredPages: pages, MaxAllocatedPages: pages,
		MaxAbandonedAppendExtents: pages, MaxChangedChunks: pages,
		MaxReplacedMetadataPages: pages, MaxBaseReservationExtents: pages,
		MaxBaseMetadataPages: pages, MaxLedgerOwners: pages,
		MaxLedgerCandidates: pages, MaxLedgerBurnedTailRanges: pages,
		MaxLedgerHighestReservedEnd: pages,
		MaxRetirementPages:          pages, MaxAuxiliaryPages: pages,
	}
}

// CheckPreparedFreelistCOWProfile applies the candidate-preparation profile
// limits without materializing a COW candidate.
func CheckPreparedFreelistCOWProfile(profile freelist.COWPrepareProfileV1, limits freelist.COWPrepareLimitsV1) error {
	return freelist.CheckCOWPrepareProfileLimitsV1(profile, limits)
}

// PreparedRootPublicationBaseProfile must be called inside the serialized
// ordered-publish preflight, while db.writeMu is held. The pager and manifest
// counts are then stable for this publish. Manager registration and pending
// leaf IDs have independent locks, so later allocation sites still need a
// checked maximum under their own locks after the command append.
func (db *DB) PreparedRootPublicationBaseProfile() PreparedRootPublicationBaseProfile {
	var profile PreparedRootPublicationBaseProfile
	if db == nil {
		return profile
	}
	if idx := db.idx.Load(); idx != nil && idx.pager != nil {
		profile.PagerPages = idx.pager.PageCount()
		if idx.allocator != nil {
			profile.FreelistCOW = idx.allocator.COWPrepareProfileV1()
		}
	}
	if db.valueLogManager != nil {
		profile.RegisteredValueLogFiles = uint64(db.valueLogManager.RegisteredFileCountNoRefresh())
		profile.ValueLogRead = db.valueLogManager.PreparedReadProfile()
	}
	if manifest := db.leafGenerationManifest; manifest != nil {
		profile.LeafGenerations = uint64(len(manifest.Generations))
		for i := range manifest.Generations {
			profile.LeafGenerationFileIDs = preparedProfileAdd(profile.LeafGenerationFileIDs, uint64(len(manifest.Generations[i].FileIDs)))
		}
	}
	db.leafGenerationPendingMu.Lock()
	profile.PendingLeafFileIDs = uint64(len(db.leafGenerationPendingFileIDs))
	db.leafGenerationPendingMu.Unlock()
	if runtime := db.rootPublication; runtime != nil {
		runtime.mu.Lock()
		profile.VisibleResources = uint64(runtime.visibleResources.Len())
		profile.VisibleMembers = uint64(len(runtime.visibleMembers))
		profile.AllocatorDebt = uint64(len(runtime.debt))
		profile.Seals = uint64(len(runtime.seals))
		for _, seal := range runtime.seals {
			if seal != nil {
				profile.SealPrefixEntries = preparedProfileAdd(profile.SealPrefixEntries, uint64(len(seal.prefix)))
			}
		}
		runtime.mu.Unlock()
	}
	return profile
}
