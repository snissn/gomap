package wire

import (
	"encoding/binary"
	"fmt"
)

const (
	minBSONDocumentSize     = 5
	initialReplyDocumentCap = 16
)

type Reply struct {
	ResponseFlags int32
	CursorID      int64
	StartingFrom  int32
	Documents     []Document
}

func ParseReply(body []byte) (Reply, error) {
	if len(body) < 20 {
		return Reply{}, ErrMessageTooShort
	}
	r := Reply{
		ResponseFlags: int32(binary.LittleEndian.Uint32(body[:4])),
		CursorID:      int64(binary.LittleEndian.Uint64(body[4:12])),
		StartingFrom:  int32(binary.LittleEndian.Uint32(body[12:16])),
	}
	numberReturned := int(int32(binary.LittleEndian.Uint32(body[16:20])))
	if numberReturned < 0 {
		return Reply{}, fmt.Errorf("%w: negative OP_REPLY document count", ErrMalformed)
	}
	rem := body[20:]
	if numberReturned > len(rem)/minBSONDocumentSize {
		return Reply{}, fmt.Errorf("%w: OP_REPLY document count exceeds remaining body size", ErrMalformed)
	}
	r.Documents = make([]Document, 0, min(numberReturned, initialReplyDocumentCap))
	for i := 0; i < numberReturned; i++ {
		doc, next, err := readDocument(rem)
		if err != nil {
			return Reply{}, err
		}
		if err := validateParsedDocument(doc); err != nil {
			return Reply{}, err
		}
		r.Documents = append(r.Documents, doc)
		rem = next
	}
	if len(rem) != 0 {
		return Reply{}, fmt.Errorf("%w: trailing bytes after OP_REPLY", ErrMalformed)
	}
	return r, nil
}

func AppendReplyBody(dst []byte, responseFlags int32, cursorID int64, startingFrom int32, docs ...Document) ([]byte, error) {
	if int64(len(docs)) > maxInt32 {
		return nil, fmt.Errorf("%w: OP_REPLY document count exceeds int32 max", ErrMessageTooLarge)
	}
	for _, doc := range docs {
		if err := ValidateDocument(doc); err != nil {
			return nil, err
		}
	}
	dst = appendInt32(dst, responseFlags)
	dst = appendInt64(dst, cursorID)
	dst = appendInt32(dst, startingFrom)
	dst = appendInt32(dst, int32(len(docs)))
	for _, doc := range docs {
		dst = append(dst, doc...)
	}
	return dst, nil
}

func AppendReplyMessage(dst []byte, requestID, responseTo int32, responseFlags int32, cursorID int64, startingFrom int32, docs ...Document) ([]byte, error) {
	body, err := AppendReplyBody(nil, responseFlags, cursorID, startingFrom, docs...)
	if err != nil {
		return nil, err
	}
	return AppendMessage(dst, requestID, responseTo, OpReply, body)
}
