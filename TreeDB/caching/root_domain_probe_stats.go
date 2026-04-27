package caching

import (
	"fmt"
	"sync/atomic"
)

type rootDomainProbeStats struct {
	getManyNativeCalls           atomic.Uint64
	getManyNativeKeys            atomic.Uint64
	getManyNativeUnique          atomic.Uint64
	getManyFallbackCalls         atomic.Uint64
	getManyFallbackKeys          atomic.Uint64
	getManyBackendFallbackCalls  atomic.Uint64
	getManyBackendFallbackUnique atomic.Uint64

	snapshotHasManyNativeCalls           atomic.Uint64
	snapshotHasManyNativeKeys            atomic.Uint64
	snapshotHasManyNativeUnique          atomic.Uint64
	snapshotHasManyBackendFallbackCalls  atomic.Uint64
	snapshotHasManyBackendFallbackUnique atomic.Uint64

	snapshotHasPrefixesNativeCalls    atomic.Uint64
	snapshotHasPrefixesNativePrefixes atomic.Uint64
	snapshotHasPrefixesNativeUnique   atomic.Uint64
	snapshotHasPrefixesFallbackCalls  atomic.Uint64
	snapshotHasPrefixesFallbackUnique atomic.Uint64
}

func (db *DB) noteRootDomainGetManyNative(requested, unique int) {
	if db == nil {
		return
	}
	db.rootDomainProbeStats.getManyNativeCalls.Add(1)
	db.rootDomainProbeStats.getManyNativeKeys.Add(uint64(requested))
	db.rootDomainProbeStats.getManyNativeUnique.Add(uint64(unique))
}

func (db *DB) noteRootDomainGetManyFallback(requested int) {
	if db == nil {
		return
	}
	db.rootDomainProbeStats.getManyFallbackCalls.Add(1)
	db.rootDomainProbeStats.getManyFallbackKeys.Add(uint64(requested))
}

func (db *DB) noteRootDomainGetManyBackendFallback(unique int) {
	if db == nil || unique <= 0 {
		return
	}
	db.rootDomainProbeStats.getManyBackendFallbackCalls.Add(1)
	db.rootDomainProbeStats.getManyBackendFallbackUnique.Add(uint64(unique))
}

func (db *DB) noteRootDomainSnapshotHasManyNative(requested, unique int) {
	if db == nil {
		return
	}
	db.rootDomainProbeStats.snapshotHasManyNativeCalls.Add(1)
	db.rootDomainProbeStats.snapshotHasManyNativeKeys.Add(uint64(requested))
	db.rootDomainProbeStats.snapshotHasManyNativeUnique.Add(uint64(unique))
}

func (db *DB) noteRootDomainSnapshotHasManyBackendFallback(unique int) {
	if db == nil || unique <= 0 {
		return
	}
	db.rootDomainProbeStats.snapshotHasManyBackendFallbackCalls.Add(1)
	db.rootDomainProbeStats.snapshotHasManyBackendFallbackUnique.Add(uint64(unique))
}

func (db *DB) noteRootDomainSnapshotHasPrefixesNative(requested, unique int) {
	if db == nil {
		return
	}
	db.rootDomainProbeStats.snapshotHasPrefixesNativeCalls.Add(1)
	db.rootDomainProbeStats.snapshotHasPrefixesNativePrefixes.Add(uint64(requested))
	db.rootDomainProbeStats.snapshotHasPrefixesNativeUnique.Add(uint64(unique))
}

func (db *DB) noteRootDomainSnapshotHasPrefixesFallback(unique int) {
	if db == nil || unique <= 0 {
		return
	}
	db.rootDomainProbeStats.snapshotHasPrefixesFallbackCalls.Add(1)
	db.rootDomainProbeStats.snapshotHasPrefixesFallbackUnique.Add(uint64(unique))
}

func (stats *rootDomainProbeStats) appendStats(out map[string]string) {
	if stats == nil || out == nil {
		return
	}
	out["treedb.cache.root_domain_probes.getmany.native_calls"] = fmt.Sprintf("%d", stats.getManyNativeCalls.Load())
	out["treedb.cache.root_domain_probes.getmany.native_keys"] = fmt.Sprintf("%d", stats.getManyNativeKeys.Load())
	out["treedb.cache.root_domain_probes.getmany.native_unique"] = fmt.Sprintf("%d", stats.getManyNativeUnique.Load())
	out["treedb.cache.root_domain_probes.getmany.fallback_calls"] = fmt.Sprintf("%d", stats.getManyFallbackCalls.Load())
	out["treedb.cache.root_domain_probes.getmany.fallback_keys"] = fmt.Sprintf("%d", stats.getManyFallbackKeys.Load())
	out["treedb.cache.root_domain_probes.getmany.backend_fallback_calls"] = fmt.Sprintf("%d", stats.getManyBackendFallbackCalls.Load())
	out["treedb.cache.root_domain_probes.getmany.backend_fallback_unique"] = fmt.Sprintf("%d", stats.getManyBackendFallbackUnique.Load())

	out["treedb.cache.root_domain_probes.snapshot_hasmany.native_calls"] = fmt.Sprintf("%d", stats.snapshotHasManyNativeCalls.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasmany.native_keys"] = fmt.Sprintf("%d", stats.snapshotHasManyNativeKeys.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasmany.native_unique"] = fmt.Sprintf("%d", stats.snapshotHasManyNativeUnique.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasmany.backend_fallback_calls"] = fmt.Sprintf("%d", stats.snapshotHasManyBackendFallbackCalls.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasmany.backend_fallback_unique"] = fmt.Sprintf("%d", stats.snapshotHasManyBackendFallbackUnique.Load())

	out["treedb.cache.root_domain_probes.snapshot_hasprefixes.native_calls"] = fmt.Sprintf("%d", stats.snapshotHasPrefixesNativeCalls.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasprefixes.native_prefixes"] = fmt.Sprintf("%d", stats.snapshotHasPrefixesNativePrefixes.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasprefixes.native_unique"] = fmt.Sprintf("%d", stats.snapshotHasPrefixesNativeUnique.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasprefixes.fallback_calls"] = fmt.Sprintf("%d", stats.snapshotHasPrefixesFallbackCalls.Load())
	out["treedb.cache.root_domain_probes.snapshot_hasprefixes.fallback_unique"] = fmt.Sprintf("%d", stats.snapshotHasPrefixesFallbackUnique.Load())
}
