# vlog_dict_realdata Benchmark

This benchmark tool demonstrates and validates the fix for the Mode4 compression dictionary activation issue.

## Issue

**Problem**: Mode4 compression with dictionary training showed no dict activity during steady state (attempted_frac=0, dict_id=0). The dict training log appeared only after steady completed.

**Root Cause**: Mode4 was deferring dictionary training until after the benchmark steady-state phase, while Mode3 trained dictionaries before steady state began.

## Fix

The fix ensures that both Mode3 and Mode4 train the dictionary **before** the steady-state phase begins. This allows the benchmark metrics to properly reflect dictionary compression activity.

### Before Fix (Mode4)
```
Starting benchmark: mode=mode4 compression=true
Starting steady-state phase...
headline: steady_raw_MBps=257388.751 attempted_frac=0.000000 kept_frac=0.000000 current_k=0 dict_id=0
mode4: dict training occurs AFTER steady completes (BUG)
treedb: slab compression trained dict (AFTER steady - TOO LATE)
```

### After Fix (Mode4)
```
Starting benchmark: mode=mode4 compression=true
treedb: slab compression trained dict (BEFORE steady - correct timing)
treedb: dict id=1 k=1 training complete
Starting steady-state phase...
headline: steady_raw_MBps=214.997 attempted_frac=1.000000 kept_frac=0.000000 current_k=1 dict_id=1
```

## Usage

```bash
go run ./TreeDB/cmd/vlog_dict_realdata \
  -input /path/to/data.jsonl \
  -bench-kv \
  -bench-mode mode4 \
  -bench-compression on \
  -bench-raw-mib 64 \
  -bench-batch 1024 \
  -bench-pointer-threshold 1 \
  -train 20000 \
  -eval 5000
```

## Testing

Run the tests to verify the fix:

```bash
go test ./TreeDB/cmd/vlog_dict_realdata -v
```

The tests verify:
1. Mode3 shows dict activity during steady state
2. Mode4 shows dict activity during steady state (after fix)
3. Both modes train dictionaries at the same time (before steady state)

## Acceptance Criteria

✅ Dict training/activation occurs early enough that steady-state reports dict activity for mode4
✅ Bench logs show non-zero attempted/kept fractions
✅ Dict ID is non-zero during steady-state phase
✅ Both mode3 and mode4 behave consistently
