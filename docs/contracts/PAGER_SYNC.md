# Pager file-sync custody

`Pager.Sync` and the index-data sync methods synchronously wait for their real
file barrier. Each file-wide attempt selects its exact handle under the pager
guard, admits the descriptor/validation/snapshot work, and acquires a call-owned
duplicate before detaching dirty membership or emitting a durability observation.
The diagnostic path is never reopened. Before/after index-data events use that
same owned descriptor, including when the original pager closes or its path is
replaced. A supplied retained handle must match a live pager's file identity;
after pager retirement the retained handle remains the caller's authority.

On Linux the existing file-only `fdatasync` route does not dereference mapped
chunks and releases the mapping guard during I/O. Allocation within represented
capacity and page writes may proceed while the file barrier is outstanding.
Flush-only and platforms requiring mapped-range synchronization retain mapping
protection. `SyncPages` and its range/paired-publication authority are unchanged.
This removes unnecessary exclusion; it does not reduce or defer durability work.

The durable file-extent cache publishes only the prefix captured before a
successful barrier, never a larger size observed after concurrent growth. An
older/smaller completion cannot lower a larger completed prefix. A successful
attempt consumes only its detached dirty snapshot; later writes remain dirty,
including writes to the same chunk. Errors restore by union only into still-owned
mapped storage and preserve the original error together with duplicate cleanup
errors. Retired mappings are never recreated to support a retry.

An outstanding synchronous call owns its duplicate and uncancellable kernel I/O
until return. `Pager.Close` may finish retiring its own descriptor/mappings while
that independent call remains outstanding. Callers must join their sync calls
before claiming that all resources are released; there is no background sync
worker, cancellation guarantee or bound on kernel I/O duration. The call closes
its duplicate exactly once on completion or failure.

The pager custody controls hold the existing file-fence seam and require actual
allocation plus page-write progress before releasing it. They also cover captured
extent/overlapping completions, same-chunk re-dirty, refusal and invalid targets,
observer/barrier/cleanup errors, exact identity under path replacement, Close
overlap and retained-handle retry. These controls do not establish a public
performance result.
