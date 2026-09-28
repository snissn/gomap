# Pinned HashiCorp Raft v1.7.3 patch

This directory contains the Go source and tests from `github.com/hashicorp/raft`
v1.7.3, retaining its MPL-2.0 license; upstream repository metadata and prose
were omitted. It is selected by the root module's `replace` directive.

The local change in `raft.go` recognizes the term of the installed snapshot
when an AppendEntries predecessor is exactly its compacted index. All other
predecessor lookups and the normal term check remain upstream behavior.
`snapshot_boundary_append_test.go` exercises the accepted boundary, the
following committed command, and wrong-term/unknown-older refusal.

This is needed for a restarted replacement follower that has snapshot index 3
and a persisted configuration at index 4 but has compacted the log at index 3.
It must receive a retransmitted index-4 configuration before command traffic
can catch up. The upstream v1.7.3 receiver otherwise rejects that predecessor,
and leader backtracking can become stuck below the follower's compacted floor.
