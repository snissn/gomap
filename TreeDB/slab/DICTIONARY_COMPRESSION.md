# Dictionary Compression in TreeDB Slabs

## Overview

TreeDB supports optional dictionary-based compression for slab values using the zstd compression library. Dictionary compression can provide better compression ratios than standard compression for workloads with repetitive or similar data patterns.

## Dictionary Training

Dictionary training is the process of analyzing sample data to build an optimized compression dictionary. This process can fail for several valid reasons:

### Common Training Failures

#### "invalid offset in dictionary"
This error occurs when the training samples contain patterns that result in invalid byte offsets in the generated dictionary. This is **not an error** and simply means the training data wasn't suitable for dictionary compression.

**Common causes:**
- Insufficient variety in training samples
- Samples too small or too uniform
- Random or highly entropic data
- Incompatible data patterns

**Resolution:**
When dictionary training fails, the system automatically falls back to standard zstd compression (without a dictionary). This is the expected behavior and does not indicate a problem.

### Log Messages

When dictionary training is attempted and fails, you may see log messages like:

```
Note: Dictionary training skipped (training samples insufficient or incompatible). Using baseline zstd compression only.
```

These messages are **informational only** and do not indicate an error condition. They inform you that:
1. Dictionary training was attempted
2. The training failed for the data provided
3. Standard compression will be used instead

### When to Be Concerned

You should only be concerned if:
- **Actual data corruption is detected** (checksum failures, read errors)
- **Compression ratio is significantly worse than expected** for known compressible data
- **Performance degradation** occurs during compression operations

Dictionary training failures alone are **not** a sign of corruption or malfunction.

## Best Practices

1. **Don't silence training failure logs** - they provide useful insight into compression behavior
2. **Monitor compression ratios** over time to understand your data patterns
3. **Use appropriate sample sizes** for dictionary training (typically 8x dictionary size in bytes)
4. **Test with representative workload data** before production deployment

## Future Work

The dictionary compression system is part of the ongoing optimization sprint detailed in `slab-optimization/spec.md`. Future enhancements may include:
- Automatic dictionary rotation and refresh
- Per-slab dictionary selection
- Dictionary storage in a separate TreeDB instance ("dictdb")
- Dynamic-K grouped compression frames

## References

- [zstd dictionary compression](https://github.com/facebook/zstd#dictionary-compression-how-to)
- `TreeDB/bench_trace_ratio_test.go` - compression ratio benchmarks
- `slab-optimization/spec.md` - compression optimization roadmap
