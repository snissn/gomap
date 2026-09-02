package caching

import (
	"math"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type vlogAutotuneProfile struct {
	DictHash         uint64
	HistoryBytes     int
	K                int
	TotalRatio       float64
	EncodeNsEstimate int64
	AvgSampleBytes   int
}

func (db *DB) valueLogAutotuneSafetyMargin() float64 {
	if db == nil {
		return valuelog.DefaultKeepSafetyMargin
	}
	switch db.valueLogAutotuneOptions.Mode {
	case valuelog.AutotuneAggressive:
		return 0.02
	default:
		return valuelog.DefaultKeepSafetyMargin
	}
}

func (db *DB) valueLogAutotuneCandidate(profile *compression.ActiveProfile, k int) *vlogAutotuneProfile {
	if profile == nil {
		return nil
	}
	return &vlogAutotuneProfile{
		DictHash:         profile.DictHash,
		HistoryBytes:     profile.HistoryBytes,
		K:                k,
		TotalRatio:       profile.TotalRatio,
		EncodeNsEstimate: profile.EncodeNsEstimate,
		AvgSampleBytes:   profile.AvgSampleBytes,
	}
}

func (db *DB) valueLogAutotuneScore(profile *vlogAutotuneProfile, ioNsPerStoredByte float64) float64 {
	if profile == nil || profile.TotalRatio <= 0 {
		return 0
	}
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoSize {
		return 1.0 / profile.TotalRatio
	}
	if ioNsPerStoredByte <= 0 {
		return 0
	}
	encodeNsPerRaw := 0.0
	if profile.EncodeNsEstimate > 0 && profile.AvgSampleBytes > 0 {
		encodeNsPerRaw = float64(profile.EncodeNsEstimate) / float64(profile.AvgSampleBytes)
	}
	cost := encodeNsPerRaw + ioNsPerStoredByte*profile.TotalRatio
	if cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return 0
	}
	return 1.0 / cost
}

func (db *DB) valueLogAutotuneShouldSwitch(candidate *vlogAutotuneProfile, ioNsPerStoredByte float64) bool {
	if db == nil || candidate == nil {
		return false
	}
	opts := db.valueLogAutotuneOptions
	policy := normalizeVlogAutoPolicy(db.valueLogAutoPolicy)
	if opts.Mode == valuelog.AutotuneOff {
		return true
	}
	if opts.MinDwellFrames > 0 {
		last := db.valueLogAutotuneLastSwitchFrames.Load()
		if last > 0 {
			total := db.valueLogDictFrames.total.Load()
			if total > last && total-last < opts.MinDwellFrames {
				return false
			}
		}
	}
	if opts.MinGainToSwitch > 0 && (policy == vlogAutoSize || ioNsPerStoredByte > 0) {
		current, _ := db.valueLogAutotuneLastProfile.Load().(*vlogAutotuneProfile)
		currentScore := db.valueLogAutotuneScore(current, ioNsPerStoredByte)
		candidateScore := db.valueLogAutotuneScore(candidate, ioNsPerStoredByte)
		if currentScore > 0 && candidateScore > 0 {
			if candidateScore < currentScore*(1+opts.MinGainToSwitch) {
				return false
			}
		}
	}
	return true
}

func (db *DB) valueLogAutotuneRecordSwitch(candidate *vlogAutotuneProfile) {
	if db == nil || candidate == nil {
		return
	}
	db.valueLogAutotuneLastProfile.Store(candidate)
	db.valueLogAutotuneLastSwitchFrames.Store(db.valueLogDictFrames.total.Load())
}
