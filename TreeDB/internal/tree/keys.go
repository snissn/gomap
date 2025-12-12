package tree

// The spec and test-spec diverge on whether user keys are stored raw in the
// user tree or prefixed. test-spec 1.7 expects internal encoding for user keys
// (`0x01|userKey`) and stripping on public iteration. To maintain Cosmos
// compatibility we follow the test-spec here.

const (
	userKeyPrefix byte = 0x01
)

type Kind uint8

const (
	KindUser Kind = iota
	KindSystem
)

func encodeUserKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	enc := make([]byte, 1+len(key))
	enc[0] = userKeyPrefix
	copy(enc[1:], key)
	return enc
}

func decodeUserKey(enc []byte) []byte {
	if len(enc) == 0 {
		return nil
	}
	if enc[0] != userKeyPrefix {
		return append([]byte(nil), enc...)
	}
	return append([]byte(nil), enc[1:]...)
}

func encodeSystemKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	return append([]byte(nil), key...)
}

func decodeSystemKey(enc []byte) []byte {
	if enc == nil {
		return nil
	}
	return append([]byte(nil), enc...)
}

