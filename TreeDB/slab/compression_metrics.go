package slab

import "log"

const defaultCompressionMetricsWindowBytes = 4 << 20

type compressionMetrics struct {
	enabled         bool
	windowBytes     uint64
	slabID          uint32
	windowRaw       uint64
	windowStored    uint64
	windowRecords   uint64
	totalRaw        uint64
	totalStored     uint64
	totalRecords    uint64
	totalCompressed uint64
	totalFull       uint64
}

func newCompressionMetrics(opts Options) compressionMetrics {
	if !opts.CompressionMetrics {
		return compressionMetrics{}
	}
	window := opts.CompressionMetricsWindowBytes
	if window <= 0 {
		window = defaultCompressionMetricsWindowBytes
	}
	return compressionMetrics{
		enabled:     true,
		windowBytes: uint64(window),
	}
}

func (m *compressionMetrics) setSlab(id uint32) {
	if !m.enabled {
		return
	}
	m.slabID = id
}

func (m *compressionMetrics) add(slabID uint32, rawBytes, storedBytes, records, compressedCount, fullCount int) {
	if !m.enabled || rawBytes <= 0 || records <= 0 {
		return
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
	m.totalRaw += uint64(rawBytes)
	m.totalStored += uint64(storedBytes)
	m.totalRecords += uint64(records)
	m.totalCompressed += uint64(compressedCount)
	m.totalFull += uint64(fullCount)

	if m.windowRaw >= m.windowBytes {
		m.logWindow()
		m.windowRaw = 0
		m.windowStored = 0
		m.windowRecords = 0
	}
}

func (m *compressionMetrics) finish(reason string) {
	if !m.enabled || m.slabID == 0 || m.totalRaw == 0 {
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
	m.totalRaw = 0
	m.totalStored = 0
	m.totalRecords = 0
	m.totalCompressed = 0
	m.totalFull = 0
}

func (m *compressionMetrics) logWindow() {
	if m.windowRaw == 0 {
		return
	}
	ratio := float64(m.windowStored) / float64(m.windowRaw)
	log.Printf("treedb: slab compression window slab=%d raw=%d stored=%d ratio=%.3f records=%d",
		m.slabID,
		m.windowRaw,
		m.windowStored,
		ratio,
		m.windowRecords,
	)
}
