package valuelog

import "testing"

func TestShouldKeepCompressed(t *testing.T) {
	tests := []struct {
		name         string
		rawBytes     int
		encodedBytes int
		encodeNs     int64
		ioNsPerByte  float64
		safetyMargin float64
		wantKeep     bool
	}{
		{
			name:         "encoded_ge_raw_never_keep",
			rawBytes:     100,
			encodedBytes: 100,
			encodeNs:     1,
			ioNsPerByte:  10,
			safetyMargin: 0.10,
			wantKeep:     false,
		},
		{
			name:         "savings_below_threshold",
			rawBytes:     1000,
			encodedBytes: 900,
			encodeNs:     1000,
			ioNsPerByte:  10, // savings=100*10=1000, cost=1100
			safetyMargin: 0.10,
			wantKeep:     false,
		},
		{
			name:         "savings_above_threshold",
			rawBytes:     1000,
			encodedBytes: 800,
			encodeNs:     1000,
			ioNsPerByte:  10, // savings=2000, cost=1100
			safetyMargin: 0.10,
			wantKeep:     true,
		},
		{
			name:         "safety_margin_sensitivity_aggressive",
			rawBytes:     1000,
			encodedBytes: 900,
			encodeNs:     950, // savings=1000, cost=969
			ioNsPerByte:  10,
			safetyMargin: 0.02,
			wantKeep:     true,
		},
		{
			name:         "safety_margin_sensitivity_medium",
			rawBytes:     1000,
			encodedBytes: 900,
			encodeNs:     950, // savings=1000, cost=1045
			ioNsPerByte:  10,
			safetyMargin: 0.10,
			wantKeep:     false,
		},
		{
			name:         "fallback_when_encode_ns_missing",
			rawBytes:     1000,
			encodedBytes: 900,
			encodeNs:     0,
			ioNsPerByte:  10,
			safetyMargin: 0.10,
			wantKeep:     true,
		},
		{
			name:         "fallback_when_io_cost_missing",
			rawBytes:     1000,
			encodedBytes: 900,
			encodeNs:     1000,
			ioNsPerByte:  0,
			safetyMargin: 0.10,
			wantKeep:     true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldKeepCompressed(tc.rawBytes, tc.encodedBytes, tc.encodeNs, tc.ioNsPerByte, tc.safetyMargin)
			if got != tc.wantKeep {
				t.Fatalf("got %v want %v", got, tc.wantKeep)
			}
		})
	}
}
