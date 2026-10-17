package db

import (
	"errors"
	"fmt"
	"testing"
)

func TestConditionalTxnOpeningDrainsPriorProducerBeforeCapture(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(fmt.Sprintf("command_wal_%t", wal), func(t *testing.T) {
			dir := t.TempDir()
			if wal {
				enableCommandWALFormat(t, dir)
			}
			d, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			sentinel := errors.New("producer preflight failed")
			fail, published, calls := true, false, 0
			unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
				calls++
				if fail {
					return sentinel
				}
				if published {
					return nil
				}
				b := d.NewPhysicalBatch()
				defer b.Close()
				if err := b.Set([]byte("prior"), []byte("visible")); err != nil {
					return err
				}
				if err := b.Write(); err != nil {
					return err
				}
				published = true
				return nil
			})
			defer unregister()
			next := d.CommandWALNextLSN()
			if tx, err := d.NewConditionalTxn(); tx != nil || !errors.Is(err, sentinel) {
				t.Fatalf("failed opening=%v %v", tx, err)
			}
			if d.conditionalActiveTxnCount.Load() != 0 || d.CommandWALNextLSN() != next {
				t.Fatal("failed opening retained transaction or assigned a command")
			}
			fail = false
			tx, err := d.NewConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if got, err := tx.Get([]byte("prior")); err != nil || string(got) != "visible" {
				t.Fatalf("opening snapshot missed prior producer: %q %v", got, err)
			}
			beforeCommit := calls
			if err := tx.Set([]byte("later"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := tx.CommitSync(); err != nil {
				t.Fatal(err)
			}
			if calls <= beforeCommit {
				t.Fatal("opening preflight replaced commit-time drain")
			}
		})
	}
}
