package raftfsm

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// SnapshotOperationLimitsV1 exposes the existing immutable snapshot admission
// ceilings to the native replacement carrier; these are not capacity claims.
func (f *FSM) SnapshotOperationLimitsV1() (SnapshotCaptureLimitsV1, error) {
	if f == nil {
		return SnapshotCaptureLimitsV1{}, fmt.Errorf("raftfsm: FSM is not open")
	}
	return f.snapshotCaptureLimits.normalized()
}

// AdmitSnapshotRetainedCopyV1 charges the additional operation-owned native
// archive, including its small native metadata, before creating the copy. The
// source archive already exists and counts toward the two-archive ceiling;
// only the new copy needs additional currently available disk space.
func (f *FSM) AdmitSnapshotRetainedCopyV1(dir string, size int64) error {
	limits, err := f.SnapshotOperationLimitsV1()
	if err != nil {
		return err
	}
	const metadata = int64(1 << 20)
	if size <= 0 || size > limits.MaxBytes || size > (limits.MaxStagingBytes-metadata)/2 {
		return fmt.Errorf("raftfsm: replacement seed exceeds snapshot limits")
	}
	available, err := snapshotDiskAvailableV1(dir)
	if err != nil {
		return err
	}
	if available < uint64(size+metadata) {
		return fmt.Errorf("raftfsm: insufficient replacement seed disk space")
	}
	return nil
}

// HandoffSnapshotWorkReleaseV1 transfers one replacement worker's admission
// after its native future and all retained-copy calls have actually returned.
// Failed capture cleanup may outlive that work; the existing owner keeps this
// release until cleanup succeeds. A concurrent newer capture may conservatively
// retain it too. This never waits for a carrier while holding snapshotMu.
func (f *FSM) HandoffSnapshotWorkReleaseV1(release func()) error {
	if f == nil || release == nil {
		return raftcluster.ErrInvalidConfig
	}
	f.snapshotMu.Lock()
	if f.snapshotWorkRelease != nil {
		f.snapshotMu.Unlock()
		return raftcluster.ErrAdmissionUnavailable
	}
	if f.snapshotOperationActive.Load() {
		f.snapshotWorkRelease = release
		f.snapshotMu.Unlock()
		return nil
	}
	f.snapshotMu.Unlock()
	release()
	return nil
}

func (f *FSM) releaseSnapshotOperationV1() {
	f.snapshotMu.Lock()
	f.snapshotOwner = raftcluster.RaftSnapshotV1{}
	f.snapshotOperationActive.Store(false)
	release := f.snapshotWorkRelease
	f.snapshotWorkRelease = nil
	f.snapshotMu.Unlock()
	if release != nil {
		release()
	}
}
