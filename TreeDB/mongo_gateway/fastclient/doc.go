// Package fastclient provides a narrow, low-overhead client for bulk inserts
// into the TreeDB Mongo gateway.
//
// It is intentionally not a general MongoDB driver. Use the official MongoDB Go
// driver for compatibility coverage. Use this package when the workload already
// has validated BSON documents and wants acknowledged insert throughput without
// the official driver's InsertMany _id extraction and InsertedIDs bookkeeping.
package fastclient
