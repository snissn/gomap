# M3 source index epoch

For `-stage overlap,partition_index`, `-m3-index-epoch` sets the new source
index's schema generation before collection creation. Its default is `0`,
preserving historical index definitions. Use `-m3-index-epoch 1` when building
a fresh fixture for the public vector lifecycle. The option accepts a uint64;
nonzero values require the M3 stage.

The persisted collection, retained variant descriptor and ready manifest bind
the resulting index-definition digest. Each M3 result row reports `index_epoch`
and `index_definition_digest` after reopening and checking those bindings.
The index epoch is separate from source, partition and router generations.
This option creates fresh data; it does not update an existing retained DB or
qualify its public search, recall or performance.
