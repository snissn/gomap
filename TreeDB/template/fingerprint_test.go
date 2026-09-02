package template

import "testing"

func TestFingerprintsDeterministic(t *testing.T) {
	cfg := Config{FingerprintK: 4, FingerprintW: 4, MaxFingerprints: 16}
	v := []byte("abcdefghijklmnop")
	fp1 := Fingerprints(v, cfg)
	fp2 := Fingerprints(v, cfg)
	if len(fp1) != len(fp2) {
		t.Fatalf("fingerprint length mismatch")
	}
	for i := range fp1 {
		if fp1[i] != fp2[i] {
			t.Fatalf("fingerprint mismatch at %d", i)
		}
	}
}
