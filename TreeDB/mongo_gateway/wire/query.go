package wire

import (
	"encoding/binary"
	"fmt"
)

type Query struct {
	Flags                int32
	FullCollectionName   string
	NumberToSkip         int32
	NumberToReturn       int32
	Query                Document
	ReturnFieldsSelector Document
}

func ParseQuery(body []byte) (Query, error) {
	if len(body) < 4 {
		return Query{}, ErrMessageTooShort
	}
	q := Query{
		Flags: int32(binary.LittleEndian.Uint32(body[:4])),
	}
	rem := body[4:]
	var err error
	q.FullCollectionName, rem, err = readCString(rem)
	if err != nil {
		return Query{}, err
	}
	if len(rem) < 8 {
		return Query{}, ErrMessageTooShort
	}
	q.NumberToSkip = int32(binary.LittleEndian.Uint32(rem[:4]))
	q.NumberToReturn = int32(binary.LittleEndian.Uint32(rem[4:8]))
	rem = rem[8:]
	q.Query, rem, err = readDocument(rem)
	if err != nil {
		return Query{}, err
	}
	if err := validateParsedDocument(q.Query); err != nil {
		return Query{}, err
	}
	if len(rem) == 0 {
		return q, nil
	}
	q.ReturnFieldsSelector, rem, err = readDocument(rem)
	if err != nil {
		return Query{}, err
	}
	if err := validateParsedDocument(q.ReturnFieldsSelector); err != nil {
		return Query{}, err
	}
	if len(rem) != 0 {
		return Query{}, fmt.Errorf("%w: trailing bytes after OP_QUERY", ErrMalformed)
	}
	return q, nil
}

func AppendQueryBody(dst []byte, flags int32, fullCollectionName string, numberToSkip, numberToReturn int32, query Document, returnFieldsSelector Document) ([]byte, error) {
	if err := validateCString(fullCollectionName); err != nil {
		return nil, err
	}
	if err := ValidateDocument(query); err != nil {
		return nil, err
	}
	if returnFieldsSelector != nil {
		if err := ValidateDocument(returnFieldsSelector); err != nil {
			return nil, err
		}
	}
	dst = appendInt32(dst, flags)
	dst = appendCString(dst, fullCollectionName)
	dst = appendInt32(dst, numberToSkip)
	dst = appendInt32(dst, numberToReturn)
	dst = append(dst, query...)
	if returnFieldsSelector != nil {
		dst = append(dst, returnFieldsSelector...)
	}
	return dst, nil
}

func AppendQueryMessage(dst []byte, requestID, responseTo int32, flags int32, fullCollectionName string, numberToSkip, numberToReturn int32, query Document, returnFieldsSelector Document) ([]byte, error) {
	body, err := AppendQueryBody(nil, flags, fullCollectionName, numberToSkip, numberToReturn, query, returnFieldsSelector)
	if err != nil {
		return nil, err
	}
	return AppendMessage(dst, requestID, responseTo, OpQuery, body)
}
