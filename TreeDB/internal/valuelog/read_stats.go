package valuelog

// ReadStats captures value-log read-path counters.
type ReadStats struct {
	// RecordCRCChecks counts value-log record CRC32 computations performed while
	// checksum verification is enabled. A grouped-frame read should increment this
	// once per verified grouped record read, not once per subvalue served from a
	// reused verified frame.
	RecordCRCChecks uint64
}

func (st *ReadStats) add(other ReadStats) {
	st.RecordCRCChecks += other.RecordCRCChecks
}

func (f *File) noteRecordCRCCheck() {
	if f == nil {
		return
	}
	f.readRecordCRCChecks.Add(1)
}

// ReadStats returns this file's read-path counters.
func (f *File) ReadStats() ReadStats {
	if f == nil {
		return ReadStats{}
	}
	return ReadStats{RecordCRCChecks: f.readRecordCRCChecks.Load()}
}

// ReadStats returns aggregate read-path counters for this snapshot set.
func (s *Set) ReadStats() ReadStats {
	if s == nil {
		return ReadStats{}
	}
	var stats ReadStats
	for _, f := range s.Files {
		stats.add(f.ReadStats())
	}
	return stats
}

// ReadStats returns aggregate read-path counters for currently tracked files.
func (m *Manager) ReadStats() ReadStats {
	if m == nil {
		return ReadStats{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var stats ReadStats
	for _, f := range m.files {
		stats.add(f.ReadStats())
	}
	return stats
}
