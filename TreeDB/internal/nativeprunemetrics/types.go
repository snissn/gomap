// Package nativeprunemetrics contains scalar receipts for the optional native
// prune harness. It is not a DB API and owns no database/source references.
package nativeprunemetrics

import "time"

type Stamp = time.Time
type SyncKind uint8

const (
	File SyncKind = iota
	Namespace
	Mmap
	DurableWrite
	syncKindCount
)

type Fence uint8

const (
	Store Fence = iota
	CachePrepare
	CacheRotate
	CacheAccept
	BackendSource
	BackendApply
	BackendAccept
	CacheForeground
	BackendWriter
	fenceCount
)

var fenceNames = [fenceCount]string{"store.mu", "cache.prepare.writeMu", "cache.rotate.writeMu", "cache.accept.writeMu", "backend.source.writeMu", "backend.apply.writeMu", "backend.accept.writeMu", "cache.foreground.writeMu", "backend.writer.writeMu"}

type FenceStats struct {
	Site                                                                            string
	ForegroundAttempts, ForegroundWaitMaxNS, MaintenanceHolds, MaintenanceHoldMaxNS uint64
	WriterAttempts, WriterWaitMaxNS                                                 uint64
}
type Receipt struct {
	Scope                                                                  string
	FileAttempts, FileFailures, NamespaceAttempts, NamespaceFailures       uint64
	MmapAttempts, MmapFailures, DurableWriteAttempts, DurableWriteFailures uint64
	StorageSyncCount                                                       uint64
	FenceWaitMaxNS, FenceHoldMaxNS                                         uint64
	Fences                                                                 [fenceCount]FenceStats
}
