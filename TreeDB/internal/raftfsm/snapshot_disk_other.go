//go:build !unix && !windows

package raftfsm

import "errors"

func snapshotDiskAvailableV1(string) (uint64, error) {
	return 0, errors.New("raftfsm: snapshot disk admission unsupported")
}
