//go:build !linux

package ycsbevidence

import "errors"

func processIdentity(string) (Process, uint64, uint64, error) {
	return Process{}, 0, 0, errors.New("YCSB process evidence requires Linux procfs")
}
