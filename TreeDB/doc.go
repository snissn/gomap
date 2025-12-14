// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open/OpenCached, which enables the cached
// write-back layer for improved write throughput. To open the backend-only
// engine (no caching), use OpenBackend or set Options.Mode = ModeBackend.

package treedb
