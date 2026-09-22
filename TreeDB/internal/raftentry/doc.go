// Package raftentry defines the R3a v1 deterministic command-entry contract.
//
// R3a v1 does not introduce a second semantic command encoding. CommandEntryV1
// bytes are the existing native-wire deterministic entry bytes. This package
// adds the Raft-apply contract around those bytes: digest domain separation,
// target/scope identity, command classification, idempotency requirements, and
// deterministic result vocabulary.
//
// Fixture stability is checked with:
//
//	GOWORK=off go test ./TreeDB/internal/raftentry ./TreeDB/internal/nativewire ./TreeDB/docs
package raftentry
