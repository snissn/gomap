package orderedmemnative

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

type allocationProfile struct {
	Boundary           string `json:"boundary"`
	Profile            string `json:"profile"`
	Path               string `json:"path"`
	SHA256             string `json:"sha256"`
	Bytes              int64  `json:"bytes"`
	SamplingRate       int    `json:"sampling_rate"`
	BoundaryGCCalls    int    `json:"boundary_gc_calls"`
	NumGCAfterBoundary uint32 `json:"num_gc_after_boundary"`
}

// These are process-wide cumulative profiles, including engine background work
// and the live fixture/oracle. Two GCs flush delayed memory-profile records and
// establish the in-use boundary. They are outside foreground measurement, and
// their initial-heap/GC effects make this a distinct diagnostic campaign.
func captureAllocationProfiles(c config, p *packet, boundary string) error {
	if c.AllocationProfileRate <= 0 || runtime.MemProfileRate != c.AllocationProfileRate {
		return fmt.Errorf("allocation sampling rate mismatch: configured=%d runtime=%d", c.AllocationProfileRate, runtime.MemProfileRate)
	}
	if err := os.MkdirAll(c.ProfileDir, 0o755); err != nil {
		return err
	}
	runtime.GC()
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	for _, name := range []string{"allocs", "heap"} {
		path := filepath.Join(c.ProfileDir, boundary+"."+name+".pprof")
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		writeErr := pprof.Lookup(name).WriteTo(file, 0)
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		p.AllocationProfiles = append(p.AllocationProfiles, allocationProfile{
			Boundary: boundary, Profile: name, Path: path,
			SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Bytes: int64(len(data)),
			SamplingRate: c.AllocationProfileRate, BoundaryGCCalls: 2, NumGCAfterBoundary: memory.NumGC,
		})
	}
	return nil
}
