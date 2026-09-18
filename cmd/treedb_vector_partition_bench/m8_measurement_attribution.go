package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Diagnostic attribution is computed outside the timed serving boundary. Keep
// the measurement transcript bounded by binding it, not duplicating every
// per-query diagnostic into that file. The full report still owns attribution;
// qualification must independently reconstruct it from the retained assets.
// This is an opt-in transcript version only. Ordinary writes and historical
// version-5 reads preserve their complete-row encoding and interpretation.
func m8MeasurementRowsWithAttributionBindingsV1(report m8ProductionReportV1) ([]m8ProductionRowV1, []string, error) {
	if !report.Config.QualityDiagnostics {
		return report.Rows, nil, nil
	}
	rows := append([]m8ProductionRowV1(nil), report.Rows...)
	digests := make([]string, len(rows))
	for i := range rows {
		// One row's encoding at a time, charged by the diagnostic serialization
		// preflight. Do not retain these buffers or clone the source corpus.
		raw, err := json.Marshal(rows[i].Attribution)
		if err != nil {
			return nil, nil, err
		}
		h := sha256.New()
		_, _ = h.Write([]byte("m8_offline_attribution_binding_v1\x00"))
		_, _ = h.Write(raw)
		digests[i] = hex.EncodeToString(h.Sum(nil))
		rows[i].Attribution = m8ProductionAttributionV1{}
	}
	return rows, digests, nil
}
