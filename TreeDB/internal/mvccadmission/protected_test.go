package mvccadmission

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"testing"
)

func TestProtectedFloorIndependentSerialAndExactDeletion(t *testing.T) {
	var a Authority
	c := a.Issue()
	p, err := testDrainedFloor(c, 20)
	if err != nil {
		t.Fatal(err)
	}
	other, err := testDrainedFloor(c, 20)
	if err != nil {
		t.Fatal(err)
	}
	key, err := mvcckey.Append(nil, []byte("key"), 10)
	if err != nil {
		t.Fatal(err)
	}
	w := func() *iterator.OrdinalScanWork {
		return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	}
	v, ready, err := p.BindVersion(key, 7, w())
	if !ready || err != nil {
		t.Fatalf("bind %v %v", ready, err)
	}
	future, err := mvcckey.Append(nil, []byte("key"), 30)
	if err != nil {
		t.Fatal(err)
	}
	cut := a.Begin()
	cut.Observe(future, c.Input(future, 30, 20))
	cut.Commit()
	if _, err := p.ValidateWithWork(w()); err != nil {
		t.Fatal("above-floor mutation changed protected serial", err)
	}
	cut = a.Begin()
	if err := cut.ObserveProtectedDeletion(v, key, 8); !errors.Is(err, iterator.ErrOrdinalScanStale) {
		t.Fatal("changed revision matched")
	}
	cut.Abort()
	cut = a.Begin()
	if err := cut.ObserveProtectedDeletion(v, key, 7); err != nil {
		cut.Abort()
		t.Fatal(err)
	}
	cut.Commit()
	if _, err := p.ValidateWithWork(w()); err != nil {
		t.Fatal("own Accepted deletion did not advance proof", err)
	}
	if _, err := other.ValidateWithWork(w()); !errors.Is(err, iterator.ErrOrdinalScanStale) {
		t.Fatal("other private proof survived protected deletion")
	}
	if err := other.Request().Validate(); err != nil {
		t.Fatal("protected serial mutation invalidated original Request", err)
	}
	cut = a.Begin()
	cut.Observe(key, Input{})
	cut.Commit()
	if !errors.Is(p.Request().Validate(), iterator.ErrOrdinalScanStale) {
		t.Fatal("raw mutation did not invalidate original Request")
	}
}

func testDrainedFloor(c *Capability, floor uint64) (*Protection, error) {
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	d, _, err := c.CaptureFloorDrain(floor, w)
	if err != nil {
		return nil, err
	}
	p, _, err := c.ConfirmDrainedFloor(d, w)
	return p, err
}
func TestProtectedFloorDrainRejectsChangedSerialAndOwner(t *testing.T) {
	for _, mutation := range []string{"foreign", "protected", "owner"} {
		t.Run(mutation, func(t *testing.T) {
			var a Authority
			c := a.Issue()
			w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			p, err := testDrainedFloor(c, 20)
			if err != nil {
				t.Fatal(err)
			}
			d, ready, err := c.CaptureFloorDrain(20, w)
			if !ready || err != nil {
				t.Fatal(ready, err)
			}
			switch mutation {
			case "foreign":
				cut := a.Begin()
				cut.Refuse()
				cut.Commit()
			case "protected":
				key, _ := mvcckey.Append(nil, []byte("key"), 10)
				v, _, err := p.BindVersion(key, 7, w)
				if err != nil {
					t.Fatal(err)
				}
				cut := a.Begin()
				if err := cut.ObserveProtectedDeletion(v, key, 7); err != nil {
					cut.Abort()
					t.Fatal(err)
				}
				cut.Commit()
			case "owner":
				a.Issue()
			}
			if p, ready, err := c.ConfirmDrainedFloor(d, w); !ready || p != nil || !errors.Is(err, iterator.ErrOrdinalScanStale) {
				t.Fatal(p, ready, err)
			}
		})
	}
}
