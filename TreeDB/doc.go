// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open/OpenCached, which returns the cached DB
// wrapper for improved write throughput. Advanced usage can open the underlying
// uncached engine via OpenBackend.

package treedb
