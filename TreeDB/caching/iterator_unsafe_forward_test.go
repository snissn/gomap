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

type safeFallbackTestIterator struct {
	key   []byte
	value []byte
	valid bool

	keyCalls       int
	valueCalls     int
	keyCopyCalls   int
	valueCopyCalls int
}

func (it *safeFallbackTestIterator) Next() {
	it.valid = false
}

func (it *safeFallbackTestIterator) Valid() bool { return it.valid }

func (it *safeFallbackTestIterator) Key() []byte {
	it.keyCalls++
	return append([]byte(nil), it.key...)
}

func (it *safeFallbackTestIterator) Value() []byte {
	it.valueCalls++
	return append([]byte(nil), it.value...)
}

func (it *safeFallbackTestIterator) KeyCopy(dst []byte) []byte {
	it.keyCopyCalls++
	return append(dst[:0], it.key...)
}

func (it *safeFallbackTestIterator) ValueCopy(dst []byte) []byte {
	it.valueCopyCalls++
	return append(dst[:0], it.value...)
}

func (it *safeFallbackTestIterator) Close() error { return nil }

func (it *safeFallbackTestIterator) Error() error { return nil }

func (it *safeFallbackTestIterator) Domain() ([]byte, []byte) { return nil, nil }

func TestIteratorWrappersForwardUnsafeViews(t *testing.T) {
	base := &unsafeForwardTestIterator{
		key:   []byte("key"),
		value: []byte("value"),
		valid: true,
	}
	foreground, ok := (&DB{}).wrapForegroundIterator(base).(unsafeIteratorView)
	if !ok {
		t.Fatalf("foreground iterator type=%T does not implement unsafeIteratorView", foreground)
	}

	for _, tc := range []struct {
		name string
		view unsafeIteratorView
	}{
		{name: "debug", view: &debugIterator{Iterator: base}},
		{name: "leased", view: &leasedMergingIterator{Iterator: base}},
		{name: "foreground", view: foreground},
	} {
		key := tc.view.UnsafeKey()
		if len(key) == 0 || &key[0] != &base.key[0] {
			t.Fatalf("%s UnsafeKey did not forward the backing key view", tc.name)
		}
		value := tc.view.UnsafeValue()
		if len(value) == 0 || &value[0] != &base.value[0] {
			t.Fatalf("%s UnsafeValue did not forward the backing value view", tc.name)
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

func TestIteratorWrappersFallbackToSafeCopiesWithoutUnsafeViews(t *testing.T) {
	base := &safeFallbackTestIterator{
		key:   []byte("key"),
		value: []byte("value"),
		valid: true,
	}
	foreground, ok := (&DB{}).wrapForegroundIterator(base).(unsafeIteratorView)
	if !ok {
		t.Fatalf("foreground iterator type=%T does not implement unsafeIteratorView", foreground)
	}

	for _, tc := range []struct {
		name string
		view unsafeIteratorView
	}{
		{name: "debug", view: &debugIterator{Iterator: base}},
		{name: "leased", view: &leasedMergingIterator{Iterator: base}},
		{name: "foreground", view: foreground},
	} {
		key := tc.view.UnsafeKey()
		if string(key) != "key" {
			t.Fatalf("%s UnsafeKey fallback=%q want key", tc.name, key)
		}
		if len(key) != 0 && &key[0] == &base.key[0] {
			t.Fatalf("%s UnsafeKey fallback returned backing key view", tc.name)
		}
		value := tc.view.UnsafeValue()
		if string(value) != "value" {
			t.Fatalf("%s UnsafeValue fallback=%q want value", tc.name, value)
		}
		if len(value) != 0 && &value[0] == &base.value[0] {
			t.Fatalf("%s UnsafeValue fallback returned backing value view", tc.name)
		}
	}

	if base.keyCalls != 3 || base.valueCalls != 3 || base.keyCopyCalls != 0 || base.valueCopyCalls != 0 {
		t.Fatalf(
			"safe iterator fallback calls: key=%d value=%d keyCopy=%d valueCopy=%d",
			base.keyCalls,
			base.valueCalls,
			base.keyCopyCalls,
			base.valueCopyCalls,
		)
	}
}

func TestConcatUnsafeIteratorPanicsWhenInvalid(t *testing.T) {
	it := &concatUnsafeIterator{}
	for name, fn := range map[string]func(){
		"UnsafeKey":   func() { _ = it.UnsafeKey() },
		"UnsafeValue": func() { _ = it.UnsafeValue() },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}
