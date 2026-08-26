package documentservice

import (
	"encoding/json"
	"net/http"

	"github.com/snissn/gomap/TreeDB/collections"
)

// DiagnosticsSnapshot is the bounded, operator-only service snapshot exposed
// by the optional diagnostics listener. It contains no document or vector data.
type DiagnosticsSnapshot struct {
	ContractVersion string                `json:"contract_version"`
	ServiceClosed   bool                  `json:"service_closed"`
	Database        map[string]string     `json:"database,omitempty"`
	Collections     map[string]string     `json:"collections,omitempty"`
	LastOpened      *LastOpenedIndexStats `json:"last_opened_index,omitempty"`
}

// LastOpenedIndexStats identifies the most recently opened service index and its
// last completed insert. Build subphase counters are deliberately owned by O2.
type LastOpenedIndexStats struct {
	Name       string                            `json:"name"`
	Generation uint64                            `json:"generation"`
	Insert     collections.CollectionInsertStats `json:"insert"`
}

type diagnosticsActiveIndex struct {
	name   string
	info   IndexInfo
	insert collections.CollectionInsertStats
}

// DiagnosticsSnapshot copies existing stats without taking the service write
// lock or touching collection mutation/persistence paths.
func (s *Service) DiagnosticsSnapshot(databaseStats func() map[string]string) DiagnosticsSnapshot {
	out := DiagnosticsSnapshot{ContractVersion: ContractVersion}
	if s == nil {
		return out
	}
	s.benchmarkSearchCacheMu.RLock()
	out.ServiceClosed = s.closed
	s.benchmarkSearchCacheMu.RUnlock()
	if databaseStats != nil {
		out.Database = cloneDiagnosticsStats(databaseStats())
	}
	if s.manager != nil {
		out.Collections = cloneDiagnosticsStats(s.manager.Stats())
	}
	if active := s.diagnosticsActive.Load(); active != nil {
		out.LastOpened = &LastOpenedIndexStats{Name: active.name, Generation: active.info.Generation, Insert: cloneDiagnosticsInsertStats(active.insert)}
	}
	return out
}

func cloneDiagnosticsInsertStats(stats collections.CollectionInsertStats) collections.CollectionInsertStats {
	if len(stats.SecondaryRuns) > 0 {
		stats.SecondaryRuns = append([]collections.CollectionSecondaryRunStats(nil), stats.SecondaryRuns...)
	}
	return stats
}

func cloneDiagnosticsStats(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// DiagnosticsHandler serves the snapshot only at its explicit operator path.
func (s *Service) DiagnosticsHandler(databaseStats func() map[string]string) http.Handler {
	if s != nil {
		s.diagnosticsEnabled.Store(true)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/debug/treedb/stats" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.DiagnosticsSnapshot(databaseStats))
	})
}
