package main

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This deliberately conservative bound is for benchmark-owned diagnostics,
// separate from the existing model of serving/engine work. It is checked before
// allocation and independently from the exact source-query visit envelope.
const maxM8QualityMembershipsV1 = 1 + vectorpartition.MaxOverlapMembershipsPerVector

func m8QualityDiagnosticRowBytesV1(cfg config, m fixtureManifest, domains int, cells int64) (int64, error) {
	if !cfg.m8QualityDiagnostics {
		return 0, nil
	}
	if m.Queries < 1 || domains < 1 || cells < 1 || len(cfg.concurrency) < 1 {
		return 0, errors.New("invalid quality diagnostic row shape")
	}
	rowCopies, err := memoryMul(int64(m.Queries), cells, int64(len(cfg.concurrency)), int64(max(1, cfg.m8MeasuredRepetitions)))
	if err != nil {
		return 0, err
	}
	// This bounds the retained query record, its variable domain slices, and
	// JSON punctuation/encoding. The factor of two also covers the immutable
	// row copy held while the report or transcript is encoded.
	return memoryMul(rowCopies, 2*(int64(unsafe.Sizeof(m8QualityQueryV1{}))+24*int64(domains)+256))
}

func m8QualityOwnedBoundsV1(k, domains, packs, queries, cells, traceQueries int, limits m8CoverageLimitsV1) (int64, int64, error) {
	if k < 1 || k > 10 || domains < 1 || domains > packs || packs > maxPartitions || queries < 1 || cells < 1 || traceQueries < 0 || traceQueries > queries || traceQueries > m8QualityTraceMaxQueriesV1 || limits.WorkUnits < 1 || limits.Bytes < 1 {
		return 0, 0, errors.New("invalid quality work shape or caps")
	}
	states := int64(1 << uint(k))
	var work, mem int64
	addProduct := func(total *int64, values ...int64) error {
		x := int64(1)
		var err error
		for _, v := range values {
			x, err = memoryMul(x, v)
			if err != nil {
				return err
			}
		}
		*total, err = memoryAdd(*total, x)
		return err
	}
	// Both DPs run ONCE/query, not once per probe/EF/concurrency coordinate.
	if err := addProduct(&work, 2, int64(queries), 2*(int64(domains)+int64(k)+1), states); err != nil {
		return 0, 0, err
	}
	// Masks/home sorting, no-coarsening best reduction and route sorting.
	if err := addProduct(&work, int64(queries), 2*int64(domains)*int64(domains)+2*int64(k)*int64(domains)+int64(packs)); err != nil {
		return 0, 0, err
	}
	if err := addProduct(&work, int64(queries), int64(cells), 32*int64(domains)+16*int64(k)); err != nil {
		return 0, 0, err
	}
	if err := addProduct(&mem, 2, states, 8); err != nil {
		return 0, 0, err
	}
	cacheRow := int64(unsafe.Sizeof(m8QualityCachedQueryV1{})) + 14*int64(domains) + 16*int64(k+1) + 8 + 128
	if err := addProduct(&mem, int64(queries), cacheRow); err != nil {
		return 0, 0, err
	}
	// Price full domain-sized route buffers and owned small observations. Curves
	// are immutable shared cache values; no source vectors are copied.
	outputRow := int64(unsafe.Sizeof(m8QualityQueryV1{})) + 12*int64(domains) + 128
	if err := addProduct(&mem, int64(queries), int64(cells), outputRow); err != nil {
		return 0, 0, err
	}
	if err := addProduct(&mem, int64(packs), int64(unsafe.Sizeof(m8ExactPackBestV1{}))+documentIDStorageBytes+16); err != nil {
		return 0, 0, err
	}
	if traceQueries > 0 {
		// One pack trace is live at a time. Actual pack structure is checked
		// against this cap before the existing trace allocates any event arrays.
		if err := addProduct(&mem, m8QualityTraceMaxBytesV1); err != nil {
			return 0, 0, err
		}
		if err := addProduct(&work, int64(traceQueries), int64(cells), int64(packs), m8QualityTraceMaxEventsV1*16); err != nil {
			return 0, 0, err
		}
	}
	if work > limits.WorkUnits || mem > limits.Bytes {
		return work, mem, fmt.Errorf("quality diagnostics exceed resource envelope: work=%d bytes=%d", work, mem)
	}
	return work, mem, nil
}

func m8PlanQualityDiagnosticsV1(cfg config, m fixtureManifest, domainCounts []int, capUnits, capBytes int64) (int64, int64, error) {
	if !cfg.m8QualityDiagnostics {
		if cfg.m8QualityTraceQueries != 0 {
			return 0, 0, errors.New("quality trace requires selected diagnostics")
		}
		return 0, 0, nil
	}
	cells, err := memoryMul(int64(len(cfg.probes)), int64(len(cfg.efSearch)))
	if err != nil || cells < 1 {
		return 0, 0, errors.New("invalid quality matrix cells")
	}
	var totalWork, peakBytes int64
	for _, d := range domainCounts {
		w, b, err := m8QualityOwnedBoundsV1(cfg.topK, d, cfg.partitions, m.Queries, int(cells), cfg.m8QualityTraceQueries, m8CoverageLimitsV1{WorkUnits: capUnits, Bytes: capBytes})
		if err != nil {
			return 0, 0, err
		}
		totalWork, err = memoryAdd(totalWork, w)
		if err != nil {
			return 0, 0, err
		}
		// The cache owns one diagnostic per probe/EF cell. Each measured
		// concurrency row additionally clones its small query records when
		// attaching coordinator masks; JSON serialization also retains bytes.
		rowBytes, err := m8QualityDiagnosticRowBytesV1(cfg, m, d, cells)
		if err != nil {
			return 0, 0, err
		}
		b, err = memoryAdd(b, rowBytes)
		if err != nil {
			return 0, 0, err
		}
		rowCopies, err := memoryMul(int64(m.Queries), cells, int64(len(cfg.concurrency)), int64(max(1, cfg.m8MeasuredRepetitions)))
		if err != nil {
			return 0, 0, err
		}
		rowWork, err := memoryMul(rowCopies, int64(cfg.topK)*int64(cfg.topK)+int64(cfg.topK)*8)
		if err != nil {
			return 0, 0, err
		}
		totalWork, err = memoryAdd(totalWork, rowWork)
		if err != nil {
			return 0, 0, err
		}
		peakBytes = max(peakBytes, b)
	}
	if cfg.m8QualityTraceQueries > 0 {
		// Bound document-ID storage by the general per-vector membership cap,
		// not the nominal selected overlap percentage.
		idBytes, err := memoryMul(int64(m.Vectors), int64(maxM8QualityMembershipsV1), documentIDStorageBytes+int64(unsafe.Sizeof("")))
		if err != nil {
			return 0, 0, err
		}
		peakBytes, err = memoryAdd(peakBytes, idBytes)
		if err != nil {
			return 0, 0, err
		}
	}
	if totalWork > capUnits || peakBytes > capBytes {
		return totalWork, peakBytes, errors.New("quality matrix exceeds work/memory cap")
	}
	return totalWork, peakBytes, nil
}
