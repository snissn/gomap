#!/bin/bash

# generate_perf_report.sh
# Runs TreeDB performance benchmarks and generates a summary report with profiling.

REPORT_FILE="PERF_REPORT.md"
DATE=$(date)
SYSTEM_INFO=$(uname -sr)
BENCHMARKS=("BenchmarkStress" "BenchmarkGet" "BenchmarkScan" "BenchmarkBatch" "BenchmarkLargeVal")

# Variables to hold snapshot data
STRESS_OPS_SEC=""
BATCH_KEYS_SEC=""
GET_OPS_SEC=""
SCAN_SCANS_SEC=""

# Temporary file for raw output
RAW_OUTPUT_FILE="perf_raw.txt"
echo "" > "$RAW_OUTPUT_FILE"

echo "Running benchmarks and profiling..."

# --- Collect Benchmark Data ---
for BENCH in "${BENCHMARKS[@]}"; do
    echo "Running $BENCH..."
    
    # Run benchmark with cpuprofile
    OUTPUT=$(go test -bench="^$BENCH$" -benchmem -cpuprofile="${BENCH}.prof" ./db)
    
    # Append raw output
    echo "$OUTPUT" >> "$RAW_OUTPUT_FILE"
    
    # Parse Benchmark Result Line
    LINE=$(echo "$OUTPUT" | grep "^$BENCH")
    
    if [ -z "$LINE" ]; then
        echo "Error running $BENCH"
        continue
    fi
    
    NAME=$(echo "$LINE" | awk '{print $1}')
    ITER=$(echo "$LINE" | awk '{print $2}')
    NS_OP=$(echo "$LINE" | awk '{print $3}')
    MEM_B=$(echo "$LINE" | awk '{print $5}')
    ALLOCS=$(echo "$LINE" | awk '{print $7}')
    
    # Calculate Throughput
    OPS_SEC="0" # Default in case of division by zero
    if [ "$NS_OP" -gt 0 ]; then
        OPS_SEC=$(echo "1000000000 / $NS_OP" | bc)
    fi
    
    # Store snapshot data
    case "$NAME" in
        "BenchmarkStress"*) 
            STRESS_OPS_SEC="$OPS_SEC"
            ;;
        "BenchmarkBatch"*) 
            BATCH_KEYS_SEC=$(echo "$OPS_SEC * 1000" | bc) # Convert batches/sec to keys/sec
            ;;
        "BenchmarkGet"*) 
            GET_OPS_SEC="$OPS_SEC"
            ;;
        "BenchmarkScan"*) 
            SCAN_SCANS_SEC="$OPS_SEC"
            ;;
    esac
    
    # Format Time for table
    TIME_FMT="$NS_OP"
    TIME_UNIT="ns"
    if [ "$NS_OP" -gt 1000000 ]; then
        TIME_FMT=$(echo "scale=2; $NS_OP / 1000000" | bc)
        TIME_UNIT="ms"
    elif [ "$NS_OP" -gt 1000 ]; then
        TIME_FMT=$(echo "scale=2; $NS_OP / 1000" | bc)
        TIME_UNIT="µs"
    fi
    
    # Append to a temporary results array for later writing to table
    BENCHMARK_TABLE_LINES+=("| $NAME | $ITER | $TIME_FMT $TIME_UNIT | $OPS_SEC | $MEM_B B | $ALLOCS |
")
done


# --- Generate Report Header and Snapshot ---
echo "# TreeDB Performance Report" > "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "**Date:** $DATE" >> "$REPORT_FILE"
echo "**System:** $SYSTEM_INFO" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

echo "## Performance Snapshot" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "\`\`\`" >> "$REPORT_FILE"
echo "  - Strict Writes: ~$STRESS_OPS_SEC ops/sec" >> "$REPORT_FILE"
echo "  - Batch Writes: ~$BATCH_KEYS_SEC keys/sec" >> "$REPORT_FILE"
echo "  - Reads: ~$GET_OPS_SEC ops/sec" >> "$REPORT_FILE"
echo "  - Full Scans: ~$SCAN_SCANS_SEC scans/sec" >> "$REPORT_FILE"
echo "\`\`\`" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

echo "## Benchmark Results" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "| Benchmark | Iterations | Time/Op | Throughput | Memory/Op | Alloc/Op |" >> "$REPORT_FILE"
echo "|---|---|---|---|---|---|" >> "$REPORT_FILE"

# --- Append Benchmark Table Lines ---
for line in "${BENCHMARK_TABLE_LINES[@]}"; do
    echo -e "$line" >> "$REPORT_FILE"
done


echo "" >> "$REPORT_FILE"
echo "## Hotspots (Top 5 Functions)" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# --- Generate Hotspots ---
for BENCH in "${BENCHMARKS[@]}"; do
    echo "### $BENCH" >> "$REPORT_FILE"
    echo '```' >> "$REPORT_FILE"
    
    if [ -f "${BENCH}.prof" ]; then
        go tool pprof -top -nodecount=5 "${BENCH}.prof" >> "$REPORT_FILE"
        
        # Clean up profile
        rm "${BENCH}.prof"
        rm "${BENCH}.test" 2>/dev/null
    else
        echo "No profile found." >> "$REPORT_FILE"
    fi
    
    echo '```' >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
done

echo "" >> "$REPORT_FILE"
echo "## Raw Output" >> "$REPORT_FILE"
echo '```' >> "$REPORT_FILE"
cat "$RAW_OUTPUT_FILE" >> "$REPORT_FILE"
echo '```' >> "$REPORT_FILE"

rm "$RAW_OUTPUT_FILE"

echo "Report generated at $REPORT_FILE"
cat "$REPORT_FILE"
