package caching

import "testing"

func TestAllowAutoDictSampling_BootstrapIgnoresMissingSelector(t *testing.T) {
	db := &DB{}
	db.valueLogDictLastAppliedDictID.Store(0)

	if !db.allowAutoDictSampling(nil, vlogWriteBlock, 4096) {
		t.Fatal("expected bootstrap auto mode to keep sampling without a selector")
	}
}

func TestAllowAutoDictSampling_PostBootstrapDefaultsToSamplingWithoutSelector(t *testing.T) {
	db := &DB{}
	db.valueLogDictLastAppliedDictID.Store(7)

	if !db.allowAutoDictSampling(nil, vlogWriteBlock, 4096) {
		t.Fatal("expected post-bootstrap auto mode to keep sampling when selector is nil")
	}
}

func TestAllowAutoDictSampling_PostBootstrapSkipsTinyBlockSamples(t *testing.T) {
	db := &DB{}
	db.valueLogDictLastAppliedDictID.Store(7)

	if db.allowAutoDictSampling(nil, vlogWriteBlock, 64) {
		t.Fatal("expected tiny post-bootstrap block payloads to skip sampling")
	}
}
