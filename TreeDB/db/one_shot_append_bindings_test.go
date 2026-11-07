package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type appendBindingsBackingReader struct {
	value    []byte
	checksum bool
	err      error
	reads    int
}

func (r *appendBindingsBackingReader) Read(page.ValuePtr) ([]byte, error) {
	r.reads++
	return r.value, r.err
}
func (r *appendBindingsBackingReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return r.Read(ptr)
}
func (r *appendBindingsBackingReader) ReadChecksumEnabled() bool { return r.checksum }

func TestOneShotAppendBindingsUseCurrentOwner(t *testing.T) {
	first, second := &valueReader{}, &valueReader{}
	for _, r := range []*valueReader{first, second} {
		b := r.appendReadBindings()
		if b.Reader != r || b.Appender != r || b.Batcher != r || b.ToReader != r ||
			b.LeafToReader != r || b.LeafToState != r || b.LeafView != r ||
			b.LeafViewState != r || b.VerifyMarker != r {
			t.Fatal("private constructor mixed or retained another reader")
		}
	}
	// This is a limitation witness, not an authentication/refusal control.
	// Only the private constructor supplies production bundles. The Tree API
	// cannot prove arbitrary public-field bundles have one owner.
	mixed := first.appendReadBindings()
	mixed.Appender = second
	if mixed.Reader != first || mixed.Appender != second {
		t.Fatal("mixed-owner limitation witness did not retain distinct readers")
	}
}

func TestOneShotAppendBindingsRetainMutablePolicy(t *testing.T) {
	first := &appendBindingsBackingReader{value: []byte("first"), checksum: true}
	second := &appendBindingsBackingReader{value: []byte("second"), checksum: false}
	r := &valueReader{}
	r.reconfigure(first, nil)
	b := r.appendReadBindings()
	policy := b.Reader.(readChecksumCapability)
	dst := make([]byte, 0, 32)
	if !policy.ReadChecksumEnabled() {
		t.Fatal("initial backing checksum policy lost")
	}
	got, err := b.Appender.ReadUnsafeAppend(page.ValuePtr{}, dst)
	if err != nil || !bytes.Equal(got, first.value) || first.reads != 1 {
		t.Fatalf("first current backing read: %q %v calls=%d", got, err, first.reads)
	}
	r.reconfigure(second, nil)
	if policy.ReadChecksumEnabled() {
		t.Fatal("same concrete reader cached old backing policy")
	}
	got, err = b.Appender.ReadUnsafeAppend(page.ValuePtr{}, dst)
	if err != nil || !bytes.Equal(got, second.value) || second.reads != 1 || first.reads != 1 {
		t.Fatalf("reconfigured backing read: %q %v first=%d second=%d", got, err, first.reads, second.reads)
	}
	failure := errors.New("actual current backing failure")
	second.err = failure
	if _, err = b.Appender.ReadUnsafeAppend(page.ValuePtr{}, dst); !errors.Is(err, failure) {
		t.Fatal("current backing error changed")
	}
	before := second.reads
	r.reconfigure(nil, nil)
	if _, err = b.Reader.Read(page.ValuePtr{}); err == nil || second.reads != before {
		t.Fatal("detached reader retained old backing access")
	}
}
