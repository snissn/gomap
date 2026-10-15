# Prepared post-zipper allocator qualification (not run)

Root supplies exact final-main SHA containing landed zipper, exact allocator
merge HEAD, and the exclusive serial runtime grant. The script defaults to
read-only preflight and never invokes Go without `--execute`.

Example (replace both exact SHA placeholders only after root supplies them):

```sh
python3 /home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/allocation-main-post-zipper-5130-20261015/run-post-zipper.py \
  --baseline FINAL_MAIN_40_HEX \
  --candidate-head ALLOCATOR_MERGED_HEAD_40_HEX \
  --out /home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/allocation-main-post-zipper-5130-20261015/final \
  --execute
```

Omit `--execute` for read-only source/base preflight. The output directory must
not exist and must be outside the worktree. Prior pure-main evidence/binaries
are neither read as runtime inputs nor overwritten/deleted. There are no Git
writes or source edits. The baseline uses overlays for16allocator-owned Go
inputs, restoring exact final-main blobs and package-only stubs for the three
new candidate test files. Zipper source must equal final main in both builds.
All non-owned Go/module inputs and the public benchmark fixture remain equal.

The single runtime window includes full freelist; focused public allocator,
format, fresh/readonly/reopen/both-slot controls; two binary builds; and three
alternating fixed1000x pairs. Expected warm-cache window is90–150seconds;
reserve up to3minutes for normal compilation variance, with hard per-command
timeouts and preserved failures. There is no broad DB/race run or profiling.
All input, overlay, source, binary, command and raw manifests are distinct from
prior evidence. The script prints TIMED_PAIRS_START before paired measurements;
other host work must already be quiesced. Additive zipper/allocator savings are
a hypothesis until these rows are measured.
