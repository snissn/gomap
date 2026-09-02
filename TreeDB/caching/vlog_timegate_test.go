package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestNeedsVlogAutotuneTiming(t *testing.T) {
	db := &DB{}
	prevMetricsEnabled := vlogAutotuneMetricsEnabled.Load()
	defer vlogAutotuneMetricsEnabled.Store(prevMetricsEnabled)

	db.valueLogAutotuneOptions = valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff}
	vlogAutotuneMetricsEnabled.Store(false)
	if db.needsVlogAutotuneTiming() {
		t.Fatalf("expected needsVlogAutotuneTiming to be false when autotune=off and metrics disabled")
	}

	vlogAutotuneMetricsEnabled.Store(true)
	if !db.needsVlogAutotuneTiming() {
		t.Fatalf("expected needsVlogAutotuneTiming to be true when env metrics are enabled")
	}

	vlogAutotuneMetricsEnabled.Store(false)
	db.valueLogAutotuneOptions = valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium}
	if !db.needsVlogAutotuneTiming() {
		t.Fatalf("expected needsVlogAutotuneTiming to be true when autotune is enabled")
	}
}
