//go:build !linux

package db

func (db *DB) adviseCommandWALRetirementPages(durableWALCleanupProofV1) {}
