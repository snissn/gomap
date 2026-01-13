package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

// debug_ingest opens a slab manager directly, ingests N records, and prints
// compression trainer/profile stats and grouped write counts.
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <db_dir> [n_records] [value_bytes]\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}
	dir := os.Args[1]
	nRecords := 20000
	valBytes := 256
	if len(os.Args) >= 3 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil && n > 0 {
			nRecords = n
		}
	}
	if len(os.Args) >= 4 {
		if n, err := strconv.Atoi(os.Args[3]); err == nil && n > 0 {
			valBytes = n
		}
	}

	opts := slab.Options{
		Compression: slab.CompressionOptions{
			Kind:            slab.CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		OmitSlabKeys:                      true,
		CompressionAdaptiveTrainBytes:     parseEnvInt("TREEDB_SLAB_COMPRESSION_TRAIN_BYTES", 1<<20),
		CompressionAdaptiveTrainDictBytes: parseEnvInt("TREEDB_SLAB_COMPRESSION_TRAIN_DICT_BYTES", 32<<10),
		CompressionAdaptiveTrainMinRecords: parseEnvInt(
			"TREEDB_SLAB_COMPRESSION_TRAIN_MIN_RECORDS", 64),
		CompressionAdaptiveTrainMaxRecordBytes: parseEnvInt(
			"TREEDB_SLAB_COMPRESSION_TRAIN_MAX_RECORD_BYTES", 64<<10),
		CompressionAdaptiveTrainSampleStride: parseEnvInt(
			"TREEDB_SLAB_COMPRESSION_TRAIN_SAMPLE_STRIDE", 1),
		CompressionAdaptiveTrainDedupWindow: parseEnvInt(
			"TREEDB_SLAB_COMPRESSION_TRAIN_DEDUP_WINDOW", 16),
	}
	sm, err := slab.NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open slab manager: %v\n", err)
		os.Exit(1)
	}
	defer sm.Close()
	sm.ForceTrainerCollecting()

	groupedCnt := 0
	legacyCnt := 0

	batch := 1024
	start := time.Now()
	for i := 0; i < nRecords; i += batch {
		end := i + batch
		if end > nRecords {
			end = nRecords
		}
		keys := make([][]byte, end-i)
		values := make([][]byte, end-i)
		for j := i; j < end; j++ {
			keys[j-i] = nil // keys omitted
			v := make([]byte, valBytes)
			for k := 0; k < valBytes; k++ {
				v[k] = byte('A' + (k % 26))
			}
			// Encode record index in first 4 bytes to avoid trivial deduplication.
			v[0] = byte(j >> 24)
			v[1] = byte(j >> 16)
			v[2] = byte(j >> 8)
			v[3] = byte(j)
			values[j-i] = v
		}
		ptrs, err := sm.AppendMany(keys, values)
		if err != nil {
			fmt.Fprintf(os.Stderr, "AppendMany: %v\n", err)
			os.Exit(1)
		}
		for _, p := range ptrs {
			if page.ValuePtrIsGrouped(p) {
				groupedCnt++
			} else {
				legacyCnt++
			}
		}
	}
	writeElapsed := time.Since(start)

	// Wait briefly for trainer to process samples.
	var stats compression.TrainerStats
	for i := 0; i < 20; i++ {
		stats, _ = sm.CompressionTrainerStats()
		if stats.ProfileAccepts > 0 || stats.ProfileRejects > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("ingest complete: records=%d value_bytes=%d elapsed=%s\n", nRecords, valBytes, writeElapsed)
	fmt.Printf("grouped_records=%d legacy_records=%d\n", groupedCnt, legacyCnt)
	fmt.Printf("trainer: enabled=%t collecting=%t training=%t queue_len=%d/%d\n", stats.Enabled, stats.Collecting, stats.Training, stats.QueueLen, stats.QueueCap)
	fmt.Printf("profile: dict_bytes=%d K=%d attempts=%d accepts=%d rejects=%d reject_reason=%s payload_ratio=%.6f total_ratio=%.6f\n",
		stats.LastTrainDict, stats.ProfileK, stats.ProfileAttempts, stats.ProfileAccepts, stats.ProfileRejects, stats.ProfileRejectReason, stats.ProfilePayloadRatio, stats.ProfileTotalRatio)
	if !stats.ProfileTimestamp.IsZero() {
		fmt.Printf("profile_timestamp=%s\n", stats.ProfileTimestamp.Format(time.RFC3339))
	}

	// If we have an accepted profile with K>1, run a second write pass to confirm grouped records are emitted.
	if stats.ProfileK > 1 {
		groupedCnt = 0
		legacyCnt = 0
		start = time.Now()
		for i := 0; i < nRecords; i += batch {
			end := i + batch
			if end > nRecords {
				end = nRecords
			}
			keys := make([][]byte, end-i)
			values := make([][]byte, end-i)
			for j := i; j < end; j++ {
				keys[j-i] = nil
				v := make([]byte, valBytes)
				for k := 0; k < valBytes; k++ {
					v[k] = byte('A' + (k % 26))
				}
				v[0] = byte(j >> 24)
				v[1] = byte(j >> 16)
				v[2] = byte(j >> 8)
				v[3] = byte(j)
				values[j-i] = v
			}
			ptrs, err := sm.AppendMany(keys, values)
			if err != nil {
				fmt.Fprintf(os.Stderr, "AppendMany (pass2): %v\n", err)
				os.Exit(1)
			}
			for _, p := range ptrs {
				if page.ValuePtrIsGrouped(p) {
					groupedCnt++
				} else {
					legacyCnt++
				}
			}
		}
		writeElapsed = time.Since(start)
		fmt.Printf("pass2 (grouped) complete: records=%d elapsed=%s grouped=%d legacy=%d\n", nRecords, writeElapsed, groupedCnt, legacyCnt)
	}
}

func parseEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
