package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Diagnostic test-only overlay. Frozen from the separately retained prior
// actual zero-injection measurement, never tuned after these new controls.
const m0PairedControlDelayNS int64 = 53219865

func m0PairedControlDelayForExecutable(executable string, legacy bool) (time.Duration, error) {
	if m0PairedControlDelayNS < 1 || m0PairedControlDelayNS > int64(5*time.Second) {
		return 0, fmt.Errorf("fixed diagnostic delay outside bounds")
	}
	if legacy {
		return 0, nil
	}
	switch filepath.Base(executable) {
	case "baseline-db.test":
		return 0, nil
	case "candidate-db.test":
		return time.Duration(m0PairedControlDelayNS), nil
	default:
		return 0, fmt.Errorf("diagnostic control requires exact saved paired executable name")
	}
}

func m0PairedControlDelay(tb testing.TB, legacy bool) time.Duration {
	tb.Helper()
	executable, err := os.Executable()
	if err != nil {
		tb.Fatal(err)
	}
	delay, err := m0PairedControlDelayForExecutable(executable, legacy)
	if err != nil {
		tb.Fatal(err)
	}
	return delay
}

func TestM0PairedControlSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		want   time.Duration
		reject bool
	}{
		{"/private/baseline-db.test", false, 0, false},
		{"/private/candidate-db.test", false, time.Duration(m0PairedControlDelayNS), false},
		{"/private/candidate-db.test", true, 0, false},
		{"/private/unexpected.test", false, 0, true},
	} {
		got, err := m0PairedControlDelayForExecutable(tc.name, tc.legacy)
		if (err != nil) != tc.reject || got != tc.want {
			t.Fatalf("%+v got %v err %v", tc, got, err)
		}
	}
}

func TestM0PairedControlRuntimeFDIdentity(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(executable)
	if name != "baseline-db.test" && name != "candidate-db.test" {
		t.Skip("runtime check requires saved paired executable basename")
	}
	want := time.Duration(0)
	if name == "candidate-db.test" {
		want = time.Duration(m0PairedControlDelayNS)
	}
	if got := m0PairedControlDelay(t, false); got != want {
		t.Fatalf("runtime executable=%s delay=%v want=%v", executable, got, want)
	}
}
