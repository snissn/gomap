package collectionwal

type SideRefLifecycleState uint8

const (
	SideRefStateAbsent SideRefLifecycleState = iota
	SideRefStatePrepared
	SideRefStateWALCommitted
	SideRefStateMaterializedUnpublished
	SideRefStateApplied
	SideRefStateCleanable
	SideRefStateCleaned
	SideRefStateQuarantined
)

func (s SideRefLifecycleState) String() string {
	switch s {
	case SideRefStateAbsent:
		return "S0 Absent"
	case SideRefStatePrepared:
		return "S1 PreparedSideRefs"
	case SideRefStateWALCommitted:
		return "S2 WALCommitted"
	case SideRefStateMaterializedUnpublished:
		return "S3 MaterializedUnpublished"
	case SideRefStateApplied:
		return "S4 Applied"
	case SideRefStateCleanable:
		return "S5 Cleanable"
	case SideRefStateCleaned:
		return "S6 Cleaned"
	case SideRefStateQuarantined:
		return "Q Quarantined"
	default:
		return "unknown"
	}
}

type ProtectionReason uint8

const (
	ProtectionPrepared ProtectionReason = iota + 1
	ProtectionWALCommitted
	ProtectionPendingPublish
	ProtectionCollectionReadView
	ProtectionBackupManifest
	ProtectionCleanupPending
)

func (r ProtectionReason) String() string {
	switch r {
	case ProtectionPrepared:
		return "prepared"
	case ProtectionWALCommitted:
		return "wal_committed"
	case ProtectionPendingPublish:
		return "pending_publish"
	case ProtectionCollectionReadView:
		return "collection_read_view"
	case ProtectionBackupManifest:
		return "backup_manifest"
	case ProtectionCleanupPending:
		return "cleanup_pending"
	default:
		return "unknown"
	}
}

type BackupMode uint8

const (
	BackupModeCleanCheckpoint BackupMode = iota + 1
	BackupModeWALSnapshot
)

func (m BackupMode) String() string {
	switch m {
	case BackupModeCleanCheckpoint:
		return "CleanCheckpoint"
	case BackupModeWALSnapshot:
		return "WALSnapshot"
	default:
		return "unknown"
	}
}

type ProtectedRef struct {
	Class         RefClass
	FileID        uint64
	Offset        uint64
	Length        uint64
	ChecksumCRC32 uint32
	CollectionSeq uint64
	WALLSN        uint64
	Reason        ProtectionReason
}

type MaintenanceBarrierSnapshot struct {
	ProtectedRefs        []ProtectedRef
	UnclassifiedPrepares int
	BackupPins           int
	RecoveryComplete     bool
}

func (s MaintenanceBarrierSnapshot) CanMutate() bool {
	return s.RecoveryComplete && s.UnclassifiedPrepares == 0
}
