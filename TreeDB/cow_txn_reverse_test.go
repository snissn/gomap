package treedb

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestPublicCOWTxnAllFFPrefixUnboundedPredicate(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
		for _, key := range [][]byte{{0xff}, {0xff, 0x00}, {0xff, 0xff}} {
			if err := db.Set(key, []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		tx := cowTxnOpen(t, db)
		open := tx.Iterator
		if reverse {
			open = tx.ReverseIterator
		}
		it, err := open([]byte{0xff}, nil)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for ; it.Valid(); it.Next() {
			if !bytes.HasPrefix(it.Key(), []byte{0xff}) {
				t.Fatalf("prefix=%x", it.Key())
			}
			count++
		}
		if count != 3 || it.Error() != nil {
			t.Fatalf("prefix count=%d err=%v", count, it.Error())
		}
		it.Seek([]byte{0xff, 0x00})
		_ = it.Close()
		if err := db.Set([]byte{0xff, 0xfe}, []byte("phantom")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Set([]byte("result"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
			t.Fatalf("prefix commit=%v", err)
		}
	}
}

func TestPublicCOWTxnIteratorOwnsOriginalBounds(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
		for _, key := range []string{"b", "c", "d", "e"} {
			if err := db.Set([]byte(key), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		tx := cowTxnOpen(t, db)
		open := tx.Iterator
		if reverse {
			open = tx.ReverseIterator
		}
		start, end := []byte("b"), []byte("e")
		it, err := open(start, end)
		if err != nil {
			t.Fatal(err)
		}
		start[0], end[0] = 'x', 'y'
		it.Seek(nil)
		var keys []string
		for ; it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
		}
		want := []string{"b", "c", "d"}
		if reverse {
			want = []string{"d", "c", "b"}
		}
		if !reflect.DeepEqual(keys, want) || it.Error() != nil {
			t.Fatalf("owned bounds=%v want%v err%v", keys, want, it.Error())
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		narrow, err := open([]byte("c"), []byte("d"))
		if err != nil {
			t.Fatal(err)
		}
		if !narrow.Valid() || string(narrow.Key()) != "c" {
			t.Fatalf("coalesced predicate broadened narrow iterator: %q", narrow.Key())
		}
		narrow.Next()
		if narrow.Valid() || narrow.Error() != nil {
			t.Fatalf("narrow iterator escaped domain: %v", narrow.Error())
		}
		if err := narrow.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicCOWTxnReverseOverlaySeekAndLifetime(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, checkpoint := range []bool{false, true} {
			t.Run(string(profile)+map[bool]string{false: "/dirty", true: "/disk"}[checkpoint], func(t *testing.T) {
				db, _, _ := cowPublicContractOpen(t, profile)
				for _, k := range []string{"a", "b", "c", "d", "e"} {
					if err := db.Set([]byte(k), []byte("base-"+k)); err != nil {
						t.Fatal(err)
					}
				}
				if checkpoint {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				tx := cowTxnOpen(t, db)
				if err := tx.Set([]byte("d"), []byte("staged-d")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Delete([]byte("c")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Set([]byte("bb"), []byte("staged-bb")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Set([]byte("bb"), []byte("last-bb")); err != nil {
					t.Fatal(err)
				}
				it, err := tx.Snapshot().ReverseIterator([]byte("b"), []byte("e"))
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for ; it.Valid(); it.Next() {
					got = append(got, string(it.Key())+"="+string(it.Value()))
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				if want := []string{"d=staged-d", "bb=last-bb", "b=base-b"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("reverse=%v want%v", got, want)
				}
				for _, tc := range []struct {
					seek []byte
					want string
				}{{nil, "d"}, {[]byte("zz"), "d"}, {[]byte("c"), "bb"}, {[]byte("bb"), "bb"}, {[]byte("b"), "b"}, {[]byte("a"), ""}} {
					it.Seek(tc.seek)
					if tc.want == "" {
						if it.Valid() {
							t.Fatalf("seek%q=%q", tc.seek, it.Key())
						}
					} else if !it.Valid() || string(it.Key()) != tc.want {
						t.Fatalf("seek%q=%q valid%v want%s", tc.seek, it.Key(), it.Valid(), tc.want)
					}
				}
				if err := tx.Set([]byte("z"), []byte("v")); !errors.Is(err, ErrTxnIteratorOpen) {
					t.Fatalf("open reverse mutation=%v", err)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				child, err := tx.ReverseIterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if child.Valid() || !errors.Is(child.Error(), ErrConditionalTxnClosed) {
					t.Fatalf("child survived transaction: %v", child.Error())
				}
				if err := child.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestPublicCOWTxnReverseFullPredicateAfterSeekAndEarlyClose(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			for _, k := range []string{"b", "d"} {
				if err := db.Set([]byte(k), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			tx := cowTxnOpen(t, db)
			it, err := tx.ReverseIterator([]byte("b"), []byte("f"))
			if err != nil {
				t.Fatal(err)
			}
			it.Seek([]byte("b"))
			if !it.Valid() || string(it.Key()) != "b" {
				t.Fatal("bounded seek")
			}
			_ = it.Close()
			if err := db.Set([]byte("e"), []byte("phantom beyond seek")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Set([]byte("z"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			before := db.backend.CommandWALNextLSN()
			if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
				t.Fatalf("reverse predicate commit=%v", err)
			}
			if after := db.backend.CommandWALNextLSN(); after != before {
				t.Fatalf("conflict appended frame: %d -> %d", before, after)
			}
			// An exclusive concrete empty end is empty, rather than unbounded.
			tx = cowTxnOpen(t, db)
			it, err = tx.ReverseIterator(nil, []byte{})
			if err != nil {
				t.Fatal(err)
			}
			if it.Valid() {
				t.Fatalf("empty reverse domain=%q", it.Key())
			}
			_ = it.Close()
			if err := db.Set([]byte("unrelated"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Set([]byte("result"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
