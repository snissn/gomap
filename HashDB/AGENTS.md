# HashDB agent notes

HashDB is a Go implementation; see [README.md](README.md) for its architecture.
The historical Rust-port proposal in [docs/history](../docs/history/hashdb-rust-port-proposal.md)
is not an active implementation plan or instruction.

For benchmark/profile artifacts, follow the repository-wide profiling contract
in [AGENTS.md](../AGENTS.md). HashDB uses slab segments; TreeDB does not.
