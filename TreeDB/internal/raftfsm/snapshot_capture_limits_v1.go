package raftfsm

import (
	"fmt"
	"math"
	"time"
)

// SnapshotCaptureLimitsV1 are admission ceilings, not a qualified capacity.
// MaxStagingBytes includes the staged storage image, archive and tar overhead.
type SnapshotCaptureLimitsV1 struct {
	MaxFiles        int
	MaxBytes        int64
	MaxStagingBytes int64
	Lifetime        time.Duration
}

func (l SnapshotCaptureLimitsV1) normalized() (SnapshotCaptureLimitsV1, error) {
	if l.MaxFiles == 0 {
		l.MaxFiles = 4096
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 64 << 30
	}
	if l.MaxStagingBytes == 0 {
		l.MaxStagingBytes = 129 << 30
	}
	if l.Lifetime == 0 {
		l.Lifetime = 30 * time.Minute
	}
	if l.MaxFiles < 1 || uint64(l.MaxFiles) > uint64((math.MaxInt64-(1<<20)-8192)/8192) || l.MaxBytes < 1 || l.MaxStagingBytes < 1 || l.Lifetime < time.Millisecond {
		return l, fmt.Errorf("raftfsm: invalid snapshot capture limits")
	}
	return l, nil
}
