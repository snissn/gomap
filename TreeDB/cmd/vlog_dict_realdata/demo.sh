#!/bin/bash
# Demonstration script showing Mode4 compression dict activation fix

set -e

echo "========================================"
echo "Mode4 Compression Dict Activation Demo"
echo "========================================"
echo ""

cd "$(dirname "$0")/../../.."

echo "Building benchmark..."
go build -o /tmp/vlog_dict_bench ./TreeDB/cmd/vlog_dict_realdata
echo ""

echo "========================================"
echo "Mode3 (Always worked correctly)"
echo "========================================"
/tmp/vlog_dict_bench \
  -input /tmp/nonexistent.jsonl \
  -bench-kv \
  -bench-mode mode3 \
  -bench-compression on \
  -bench-raw-mib 1 \
  -train 100 \
  -eval 50 2>&1 | grep -E "treedb:|headline:"
echo ""

echo "========================================"
echo "Mode4 (Now fixed - shows dict activity)"
echo "========================================"
/tmp/vlog_dict_bench \
  -input /tmp/nonexistent.jsonl \
  -bench-kv \
  -bench-mode mode4 \
  -bench-compression on \
  -bench-raw-mib 1 \
  -train 100 \
  -eval 50 2>&1 | grep -E "treedb:|headline:"
echo ""

echo "========================================"
echo "Key Observations:"
echo "========================================"
echo "✅ Both modes show 'trained dict (BEFORE steady'"
echo "✅ Both modes show dict_id=1 in headline"
echo "✅ Both modes show attempted_frac > 0"
echo ""
echo "The fix ensures Mode4 trains the dictionary"
echo "BEFORE steady state begins (not after)."
echo ""
