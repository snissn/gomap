package caching

import "testing"

type unsafeForwardTestIterator struct {
	key   []byte
	value []byte
	valid bool

	keyCalls       int
	valueCalls     int
	keyCopyCalls   int
	valueCopyCalls int
}

func (it *unsafeForwardTestIterator) Next() {
	it.valid = false
}

func (it *unsafeForwardTestIterator) Valid() bool { return it.valid }

func (it *unsafeForwardTestIterator) Key() []byte {
	it.keyCalls++
	return append([]byte(nil), it.key...)
}

func (it *unsafeForwardTestIterator) Value() []byte {
	it.valueCalls++
	return append([]byte(nil), it.value...)
}

func (it *unsafeForwardTestIterator) KeyCopy(dst []byte) []byte {
	it.keyCopyCalls++
	return append(dst[:0], it.key...)
}

func (it *unsafeForwardTestIterator) ValueCopy(dst []byte) []byte {
	it.valueCopyCalls++
	return append(dst[:0], it.value...)
}

func (it *unsafeForwardTestIterator) Close() error { return nil }

func (it *unsafeForwardTestIterator) Error() error { return nil }

func (it *unsafeForwardTestIterator) Domain() ([]byte, []byte) { return nil, nil }

func (it *unsafeForwardTestIterator) UnsafeKey() []byte { return it.key }

func (it *unsafeForwardTestIterator) UnsafeValue() []byte { return it.value }

func TestIteratorWrappersForwardUnsafeViews(t *testing.T) {
	base := &unsafeForwardTestIterator{
		key:   []byte("key"),
		value: []byte("value"),
		valid: true,
	}

	for name, view := range map[string]unsafeIteratorView{
		"debug":      &debugIterator{Iterator: base},
		"leased":     &leasedMergingIterator{Iterator: base},
		"foreground": (&DB{}).wrapForegroundIterator(base).(unsafeIteratorView),
	} {
		key := view.UnsafeKey()
		if len(key) == 0 || &key[0] != &base.key[0] {
			t.Fatalf("%s UnsafeKey did not forward the backing key view", name)
		}
		value := view.UnsafeValue()
		if len(value) == 0 || &value[0] != &base.value[0] {
			t.Fatalf("%s UnsafeValue did not forward the backing value view", name)
		}
	}

	if base.keyCalls != 0 || base.valueCalls != 0 || base.keyCopyCalls != 0 || base.valueCopyCalls != 0 {
		t.Fatalf(
			"safe iterator fallback called: key=%d value=%d keyCopy=%d valueCopy=%d",
			base.keyCalls,
			base.valueCalls,
			base.keyCopyCalls,
			base.valueCopyCalls,
		)
	}
}
