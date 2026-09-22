package collectionwal

import "testing"

func TestDurableAckCapabilityNames(t *testing.T) {
	tests := map[DurableAckCapability]string{
		DurableAckDisabled:             "disabled",
		DurableAckNoIndexRowInsertOnly: "NoIndexRowInsertOnly",
	}
	for capability, want := range tests {
		if got := capability.String(); got != want {
			t.Fatalf("capability %d String()=%q want %q", capability, got, want)
		}
	}
}

func TestDurableAckCapabilityEnabled(t *testing.T) {
	if DurableAckDisabled.Enabled() {
		t.Fatal("disabled capability must not be enabled")
	}
	if !DurableAckNoIndexRowInsertOnly.Enabled() {
		t.Fatal("NoIndexRowInsertOnly capability must be enabled")
	}
}
