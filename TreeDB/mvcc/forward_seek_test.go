package mvcc

import (
	"bytes"
	"errors"
	"testing"
)

type forwardVersionSource struct {
	*nextKeySource
	forwards int
}

func (s *forwardVersionSource) SeekForward(key []byte) {
	s.forwards++
	s.nextKeySource.Seek(key)
}

func TestVersionIteratorForwardSeekDeepFutureParity(t *testing.T) {
	makeSource := func() *nextKeySource {
		s := new(nextKeySource)
		s.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
		for _, key := range [][]byte{{'a', 0}, {'b'}, {255}} {
			for ts := uint64(65); ts > 0; ts-- {
				value := []byte{recordValueV1, byte(ts)}
				if key[0] == 'b' && ts == 1 {
					value = []byte{recordTombstoneV1}
				}
				s.add(t, key, ts, value)
			}
		}
		return s
	}
	plain := makeSource()
	forward := &forwardVersionSource{nextKeySource: makeSource()}
	a := &VersionIterator{raw: plain, options: VersionIteratorOptions{ReadTimestamp: 1}}
	b := &VersionIterator{raw: forward, options: a.options}
	a.advance()
	b.advance()
	owned := b.Entry()
	for i := 0; ; i++ {
		if a.Valid() != b.Valid() || !errors.Is(a.Error(), b.Error()) || a.Stats() != b.Stats() {
			t.Fatalf("step %d validity/error/stats differ: %v/%v %+v/%+v", i, a.Error(), b.Error(), a.Stats(), b.Stats())
		}
		if !a.Valid() {
			break
		}
		x, y := a.EntryView(), b.EntryView()
		if !bytes.Equal(x.Key, y.Key) || x.Timestamp != y.Timestamp || x.State != y.State || !bytes.Equal(x.Value, y.Value) {
			t.Fatalf("step %d records differ: %+v/%+v", i, x, y)
		}
		a.NextKey()
		b.NextKey()
	}
	if forward.forwards != 3 || forward.seeks != 3 || forward.nexts != 4 || forward.reads != 4 || plain.nexts != forward.nexts {
		t.Fatalf("deep future history walked or eagerly read: forward=%d seek=%d next=%d value=%d", forward.forwards, forward.seeks, forward.nexts, forward.reads)
	}
	if string(owned.Key) != "a" || owned.Timestamp != 1 || string(owned.Value) != "a" {
		t.Fatalf("owned result changed: %+v", owned)
	}
}

func TestVersionIteratorForwardSeekValidationAndErrors(t *testing.T) {
	failure := errors.New("forward storage failure")
	for _, malformed := range []bool{false, true} {
		s := &forwardVersionSource{nextKeySource: new(nextKeySource)}
		s.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
		if malformed {
			s.keys = append(s.keys, []byte{'b', 0, 0, 1})
			s.values = append(s.values, nil)
		} else {
			s.add(t, []byte("b"), 65, []byte{recordValueV1, 'n'})
			s.seekErr = failure
		}
		it := &VersionIterator{raw: s, options: VersionIteratorOptions{ReadTimestamp: 1}}
		it.advance()
		it.NextKey()
		if malformed {
			if !errors.Is(it.Error(), ErrMalformedRecord) || s.forwards != 0 {
				t.Fatalf("malformed key bypassed validation: forwards=%d err=%v", s.forwards, it.Error())
			}
		} else if !errors.Is(it.Error(), failure) || s.forwards != 1 || s.seeks != 1 {
			t.Fatalf("movement error retried/hidden: forward=%d seek=%d err=%v", s.forwards, s.seeks, it.Error())
		}
		if s.reads != 1 {
			t.Fatalf("rejected payload read %d times", s.reads)
		}
	}
	// Ordinary iteration and caller-directed seeks retain their original paths.
	s := &forwardVersionSource{nextKeySource: new(nextKeySource)}
	s.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
	s.add(t, []byte("b"), 9, []byte{recordValueV1, 'n'})
	s.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
	it := &VersionIterator{raw: s, options: VersionIteratorOptions{ReadTimestamp: 1}}
	it.advance()
	it.Next()
	it.Seek([]byte("a"), 1)
	if s.forwards != 0 || !it.Valid() || string(it.EntryView().Key) != "a" {
		t.Fatalf("ordinary movement dispatched forward seek: forwards=%d err=%v", s.forwards, it.Error())
	}
}
