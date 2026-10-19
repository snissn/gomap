package collections

import (
	"encoding/binary"
	"errors"
)

// Native catalog identities are system-tree records. A deleted name loses its
// identity record; its number is never reused because the separate next value
// advances in the SAME logical and physical publication as creation. Renaming
// moves this record without changing its value. This format is deliberately
// distinct from physical collection root IDs and command-WAL LSNs.
const (
	nativeCatalogNextIncarnationKey = "collections/native/v1/next-incarnation"
	nativeCatalogIncarnationPrefix  = "collections/native/v1/incarnation/"
	nativeCatalogIdentityBytes      = 16
)

var errNativeCatalogIdentity = errors.New("collections: invalid native catalog identity")

// The kind byte prevents a next-counter value being accepted as a collection
// incarnation. Reserved bytes reject unknown extensions rather than silently
// inventing logical identity from physical metadata on reopen.
func nativeCatalogIdentityRecord(value uint64, counter bool) ([nativeCatalogIdentityBytes]byte, error) {
	var record [nativeCatalogIdentityBytes]byte
	if value == 0 {
		return record, errNativeCatalogIdentity
	}
	copy(record[:4], "NCI1")
	if counter {
		record[4] = 1
	}
	binary.BigEndian.PutUint64(record[8:], value)
	return record, nil
}

func nativeCatalogIdentityValue(record []byte, counter bool) (uint64, error) {
	wantKind := byte(0)
	if counter {
		wantKind = 1
	}
	if len(record) != nativeCatalogIdentityBytes || string(record[:4]) != "NCI1" || record[4] != wantKind || record[5] != 0 || record[6] != 0 || record[7] != 0 {
		return 0, errNativeCatalogIdentity
	}
	value := binary.BigEndian.Uint64(record[8:])
	if value == 0 {
		return 0, errNativeCatalogIdentity
	}
	return value, nil
}
