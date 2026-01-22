package valuelog

// VirtualSink advances a clock based on written bytes to simulate IO wall time.
// Intended for deterministic tests/benchmarks.
type VirtualSink struct {
	Clock          interface{ Advance(int64) }
	NsPerByte      int64
	SyncPenaltyNs  int64
	FlushPenaltyNs int64
}

func (s *VirtualSink) Write(p []byte) (int, error) {
	if s == nil {
		return 0, nil
	}
	if s.Clock != nil && s.NsPerByte > 0 {
		s.Clock.Advance(int64(len(p)) * s.NsPerByte)
	}
	return len(p), nil
}

func (s *VirtualSink) Sync() error {
	if s == nil {
		return nil
	}
	if s.Clock != nil && s.SyncPenaltyNs > 0 {
		s.Clock.Advance(s.SyncPenaltyNs)
	}
	return nil
}

func (s *VirtualSink) Flush() error {
	if s == nil {
		return nil
	}
	if s.Clock != nil && s.FlushPenaltyNs > 0 {
		s.Clock.Advance(s.FlushPenaltyNs)
	}
	return nil
}
