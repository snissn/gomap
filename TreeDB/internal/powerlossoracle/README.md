# Stable namespace model

The oracle models file bytes by inode and durable immediate directory entries
by physical directory identity. The stable pathname maps are derived views of
those objects reachable from the stable root, not independent persistence
state. `SyncFile` promotes only one inode's bytes. `SyncDir` replaces only one
directory inode's immediate entries; it does not sync child bytes or descendant
directory contents.

A directory rename moves volatile paths while retaining directory identities
and file inode IDs. Before the containing parent is synced, a crash still sees
the old edge. After that barrier, previously synced child contents are reachable
through the new edge. Syncing a moved child directory before its parent updates
the same directory object, so those contents can be reachable through the old
stable edge. A cross-parent rename needs both parents synced: selective sync can
retain both names or neither. This is the same conservative independent-parent
contract already used for file renames.

`Capture`, `Clone`, selective namespace/data promotion, stable materialization,
identity overrides, and `Crash` use these same objects. Unreachable directory
objects can retain synced contents while a process is running, but a crash drops
them. Recursive unlink removes volatile edges only; the stable subtree survives
until its containing parent is synced. A later same-name directory cannot inherit
the old inode's entries.

Observers report completed real namespace mutations and persistence barriers.
An early scan can see a completed rename before its synchronous callback; the
model reconciles physical file and directory identities without promoting names
or bytes. Directory rename rejects missing destination parents, nonempty
replacement directories, and moves into the source subtree. No model operation
makes `os.Exit`, userspace flush, or a copied live filesystem equivalent to power
loss. These model controls are prerequisites for public-path durability tests;
they do not by themselves establish an external checkpoint ACK guarantee.
