package treedb

import "github.com/snissn/gomap/TreeDB/caching"

// EstimateMVCCWriteWork returns a conservative composable physical-work charge
// for one logical MVCC key/value, including worst-case key escaping and producer
// backing/canonical preparation. Pass the actual stored value length (including
// any application envelope); Delete can use zero. Invalid/overflowing input
// saturates at MaxUint64. Sum with overflow saturation, including replacement
// attempts whose old arena storage remains owned until transaction completion.
//
// This is a producer shaping bound, not a reservation or a guarantee of success:
// retained cuts/history and producer-discovered resources still use the original
// canonical admission. It cannot authorize splitting or retrying an assigned
// command. The scope is the ordered logical MVCC path only.
func (db *DB) EstimateMVCCWriteWork(keyBytes, valueBytes int) uint64 {
	return db.NewMVCCWriteWorkEstimator().Estimate(keyBytes, valueBytes)
}

// MVCCWriteWorkEstimator is an immutable scalar cost model. It owns no DB,
// budget, root or lifecycle pointer and remains safe to use after DB.Close.
// Its zero value reports saturated estimates (unsupported).
type MVCCWriteWorkEstimator struct {
	inner caching.MVCCWriteWorkEstimator
}

// NewMVCCWriteWorkEstimator captures producer constants once under lifecycle
// protection. Cache this value at Open for lock-free per-entry shaping.
func (db *DB) NewMVCCWriteWorkEstimator() MVCCWriteWorkEstimator {
	if db == nil {
		return MVCCWriteWorkEstimator{}
	}
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.cached == nil {
		return MVCCWriteWorkEstimator{}
	}
	return MVCCWriteWorkEstimator{inner: db.cached.NewMVCCWriteWorkEstimator()}
}

func (e MVCCWriteWorkEstimator) Estimate(keyBytes, valueBytes int) uint64 {
	return e.inner.Estimate(keyBytes, valueBytes)
}

// MVCCWriteWorkLimit is the configured work envelope per aggregate when a
// consumer retains at most two submitted aggregate records. It is derived from
// the engine's finite producer/maintenance partition. Both each transaction and
// the complete combined cohort must fit this same bound before submission.
// Zero exclusively means unsupported. A supported configuration with no shaping
// headroom returns 1, below every valid row estimate, so consumers must refuse.
func (db *DB) MVCCWriteWorkLimit() uint64 {
	if db == nil {
		return 0
	}
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.cached == nil {
		return 0
	}
	return db.cached.MVCCWriteWorkLimit()
}
