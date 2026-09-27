package raftfsm

import "fmt"

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
