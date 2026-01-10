package slab

import "log"

const defaultCompressionMetricsWindowBytes = 4 << 20

type compressionMetrics struct {
	enabled          bool
	logWindows       bool
	windowBytes      uint64
	slabID           uint32
	pauseThreshold   float64
	pauseBytes       uint64
	minRecords       uint64
	windowRaw        uint64
	windowStored     uint64
	windowRecords    uint64
	windowCompressed uint64
	windowFull       uint64
	totalRaw         uint64
	totalStored      uint64
	totalRecords     uint64
	totalCompressed  uint64
	totalFull        uint64
	totalDegraded    uint64
}

func newCompressionMetrics(opts Options) compressionMetrics {
	enabled := opts.CompressionMetrics || opts.CompressionAdaptiveRatio > 0
	if !enabled {
		return compressionMetrics{}
	}
	window := opts.CompressionMetricsWindowBytes
	if window <= 0 {
		window = defaultCompressionMetricsWindowBytes
	}
	minRecords := opts.CompressionAdaptiveMinRecords
	if minRecords <= 0 {
		minRecords = 1
	}
	pauseBytes := opts.CompressionAdaptivePauseBytes
	if pauseBytes <= 0 {
		pauseBytes = window
	}
	return compressionMetrics{
		enabled:        true,
		logWindows:     opts.CompressionMetrics,
		windowBytes:    uint64(window),
		pauseThreshold: opts.CompressionAdaptiveRatio,
		pauseBytes:     uint64(pauseBytes),
		minRecords:     uint64(minRecords),
	}
}

func (m *compressionMetrics) setSlab(id uint32) {
	if !m.enabled {
		return
	}
	m.slabID = id
}

func (m *compressionMetrics) add(slabID uint32, rawBytes, storedBytes, records, compressedCount, fullCount int) uint64 {
	if !m.enabled || rawBytes <= 0 || records <= 0 {
		return 0
	}
	if m.slabID == 0 {
		m.slabID = slabID
	}
	if m.slabID != slabID {
		m.finish("slab-switch")
		m.reset(slabID)
	}
	m.windowRaw += uint64(rawBytes)
	m.windowStored += uint64(storedBytes)
	m.windowRecords += uint64(records)
	m.windowCompressed += uint64(compressedCount)
	m.windowFull += uint64(fullCount)
	m.totalRaw += uint64(rawBytes)
	m.totalStored += uint64(storedBytes)
	m.totalRecords += uint64(records)
	m.totalCompressed += uint64(compressedCount)
	m.totalFull += uint64(fullCount)

	if m.windowRaw >= m.windowBytes {
		pauseBytes := m.logWindow()
		m.windowRaw = 0
		m.windowStored = 0
		m.windowRecords = 0
		m.windowCompressed = 0
		m.windowFull = 0
		return pauseBytes
	}
	return 0
}

func (m *compressionMetrics) finish(reason string) {
	if !m.enabled || m.slabID == 0 || m.totalRaw == 0 {
		return
	}
	if !m.logWindows {
		return
	}
	ratio := float64(m.totalStored) / float64(m.totalRaw)
	log.Printf("treedb: slab compression summary slab=%d raw=%d stored=%d ratio=%.3f records=%d compressed=%d full=%d reason=%s",
		m.slabID,
		m.totalRaw,
		m.totalStored,
		ratio,
		m.totalRecords,
		m.totalCompressed,
		m.totalFull,
		reason,
	)
}

func (m *compressionMetrics) reset(nextSlabID uint32) {
	m.slabID = nextSlabID
	m.windowRaw = 0
	m.windowStored = 0
	m.windowRecords = 0
	m.windowCompressed = 0
	m.windowFull = 0
	m.totalRaw = 0
	m.totalStored = 0
	m.totalRecords = 0
	m.totalCompressed = 0
	m.totalFull = 0
}

func (m *compressionMetrics) logWindow() uint64 {
	if m.windowRaw == 0 {
		return 0
	}
	ratio := float64(m.windowStored) / float64(m.windowRaw)
	if m.logWindows {
		log.Printf("treedb: slab compression window slab=%d raw=%d stored=%d ratio=%.3f records=%d",
			m.slabID,
			m.windowRaw,
			m.windowStored,
			ratio,
			m.windowRecords,
		)
	}
	if m.pauseThreshold > 0 &&
		ratio >= m.pauseThreshold &&
		m.windowRecords >= m.minRecords {
		if m.logWindows && m.totalDegraded == 0 {
			log.Printf("treedb: slab compression degraded slab=%d ratio=%.3f raw=%d stored=%d records=%d",
				m.slabID,
				ratio,
				m.windowRaw,
				m.windowStored,
				m.windowRecords,
			)
		}
		m.totalDegraded++
		return m.pauseBytes
	}
	return 0
}
