package compression

import (
	"log"
	"sync"
)

const DefaultMetricsWindowBytes = 4 << 20

type Metrics struct {
	mu sync.Mutex

	Enabled          bool
	LogWindows       bool
	WindowBytes      uint64
	SlabID           uint32
	PauseThreshold   float64
	PauseBytes       uint64
	MinRecords       uint64
	WindowRaw        uint64
	WindowStored     uint64
	WindowRecords    uint64
	WindowCompressed uint64
	WindowFull       uint64
	TotalRaw         uint64
	TotalStored      uint64
	TotalRecords     uint64
	TotalCompressed  uint64
	TotalFull        uint64
	TotalDegraded    uint64
}

type MetricsOptions struct {
	MetricsEnabled bool
	AdaptiveRatio  float64
	WindowBytes    int
	MinRecords     int
	PauseBytes     int
}

func NewMetrics(opts MetricsOptions) Metrics {
	enabled := opts.MetricsEnabled || opts.AdaptiveRatio > 0
	if !enabled {
		return Metrics{}
	}
	window := opts.WindowBytes
	if window <= 0 {
		window = DefaultMetricsWindowBytes
	}
	minRecords := opts.MinRecords
	if minRecords <= 0 {
		minRecords = 1
	}
	pauseBytes := opts.PauseBytes
	if pauseBytes <= 0 {
		pauseBytes = window
	}
	return Metrics{
		Enabled:        true,
		LogWindows:     opts.MetricsEnabled,
		WindowBytes:    uint64(window),
		PauseThreshold: opts.AdaptiveRatio,
		PauseBytes:     uint64(pauseBytes),
		MinRecords:     uint64(minRecords),
	}
}

func (m *Metrics) SetSlab(id uint32) {
	if !m.Enabled {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SlabID = id
}

func (m *Metrics) Add(slabID uint32, rawBytes, storedBytes, records, compressedCount, fullCount int) uint64 {
	if !m.Enabled || rawBytes <= 0 || records <= 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SlabID == 0 {
		m.SlabID = slabID
	}
	if m.SlabID != slabID {
		m.finishLocked("slab-switch")
		m.resetLocked(slabID)
	}
	m.WindowRaw += uint64(rawBytes)
	m.WindowStored += uint64(storedBytes)
	m.WindowRecords += uint64(records)
	m.WindowCompressed += uint64(compressedCount)
	m.WindowFull += uint64(fullCount)
	m.TotalRaw += uint64(rawBytes)
	m.TotalStored += uint64(storedBytes)
	m.TotalRecords += uint64(records)
	m.TotalCompressed += uint64(compressedCount)
	m.TotalFull += uint64(fullCount)

	if m.WindowRaw >= m.WindowBytes {
		pauseBytes := m.logWindowLocked()
		m.WindowRaw = 0
		m.WindowStored = 0
		m.WindowRecords = 0
		m.WindowCompressed = 0
		m.WindowFull = 0
		return pauseBytes
	}
	return 0
}

func (m *Metrics) Finish(reason string) {
	if !m.Enabled {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finishLocked(reason)
}

func (m *Metrics) finishLocked(reason string) {
	if m.SlabID == 0 || m.TotalRaw == 0 {
		return
	}
	if !m.LogWindows {
		return
	}
	ratio := float64(m.TotalStored) / float64(m.TotalRaw)
	log.Printf("treedb: slab compression summary slab=%d raw=%d stored=%d ratio=%.3f records=%d compressed=%d full=%d reason=%s",
		m.SlabID,
		m.TotalRaw,
		m.TotalStored,
		ratio,
		m.TotalRecords,
		m.TotalCompressed,
		m.TotalFull,
		reason,
	)
}

func (m *Metrics) Reset(nextSlabID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetLocked(nextSlabID)
}

func (m *Metrics) resetLocked(nextSlabID uint32) {
	m.SlabID = nextSlabID
	m.WindowRaw = 0
	m.WindowStored = 0
	m.WindowRecords = 0
	m.WindowCompressed = 0
	m.WindowFull = 0
	m.TotalRaw = 0
	m.TotalStored = 0
	m.TotalRecords = 0
	m.TotalCompressed = 0
	m.TotalFull = 0
}

func (m *Metrics) LogWindow() uint64 {
	if !m.Enabled {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logWindowLocked()
}

func (m *Metrics) logWindowLocked() uint64 {
	if m.WindowRaw == 0 {
		return 0
	}
	ratio := float64(m.WindowStored) / float64(m.WindowRaw)
	if m.LogWindows {
		log.Printf("treedb: slab compression window slab=%d raw=%d stored=%d ratio=%.3f records=%d",
			m.SlabID,
			m.WindowRaw,
			m.WindowStored,
			ratio,
			m.WindowRecords,
		)
	}
	if m.PauseThreshold > 0 &&
		ratio >= m.PauseThreshold &&
		m.WindowRecords >= m.MinRecords {
		if m.LogWindows && m.TotalDegraded == 0 {
			log.Printf("treedb: slab compression degraded slab=%d ratio=%.3f raw=%d stored=%d records=%d",
				m.SlabID,
				ratio,
				m.WindowRaw,
				m.WindowStored,
				m.WindowRecords,
			)
		}
		m.TotalDegraded++
		return m.PauseBytes
	}
	return 0
}
