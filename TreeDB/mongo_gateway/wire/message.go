package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	HeaderLen               = 16
	DefaultMaxMessageLength = 48_000_000
	maxInt32                = int64(1<<31 - 1)
)

type OpCode int32

const (
	OpReply      OpCode = 1
	OpUpdate     OpCode = 2001
	OpInsert     OpCode = 2002
	OpQuery      OpCode = 2004
	OpGetMore    OpCode = 2005
	OpDelete     OpCode = 2006
	OpKillCursor OpCode = 2007
	OpCompressed OpCode = 2012
	OpMsg        OpCode = 2013
)

var (
	ErrMessageTooShort = errors.New("mongo wire: message too short")
	ErrMessageTooLarge = errors.New("mongo wire: message too large")
	ErrMalformed       = errors.New("mongo wire: malformed message")
	ErrUnsupported     = errors.New("mongo wire: unsupported message")
)

type Header struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        OpCode
}

type Document = bson.Raw

func ParseHeader(src []byte) (Header, error) {
	if len(src) < HeaderLen {
		return Header{}, ErrMessageTooShort
	}
	h := Header{
		MessageLength: int32(binary.LittleEndian.Uint32(src[0:4])),
		RequestID:     int32(binary.LittleEndian.Uint32(src[4:8])),
		ResponseTo:    int32(binary.LittleEndian.Uint32(src[8:12])),
		OpCode:        OpCode(int32(binary.LittleEndian.Uint32(src[12:16]))),
	}
	if h.MessageLength < HeaderLen {
		return Header{}, fmt.Errorf("%w: message length %d below header length", ErrMalformed, h.MessageLength)
	}
	return h, nil
}

func AppendHeader(dst []byte, h Header) []byte {
	dst = appendInt32(dst, h.MessageLength)
	dst = appendInt32(dst, h.RequestID)
	dst = appendInt32(dst, h.ResponseTo)
	dst = appendInt32(dst, int32(h.OpCode))
	return dst
}

func AppendMessage(dst []byte, requestID, responseTo int32, opCode OpCode, body []byte) ([]byte, error) {
	messageLength := HeaderLen + len(body)
	if int64(messageLength) > maxInt32 {
		return nil, fmt.Errorf("%w: length=%d exceeds int32 max", ErrMessageTooLarge, messageLength)
	}
	if messageLength > DefaultMaxMessageLength {
		return nil, fmt.Errorf("%w: length=%d max=%d", ErrMessageTooLarge, messageLength, DefaultMaxMessageLength)
	}
	h := Header{
		MessageLength: int32(messageLength),
		RequestID:     requestID,
		ResponseTo:    responseTo,
		OpCode:        opCode,
	}
	dst = AppendHeader(dst, h)
	return append(dst, body...), nil
}

func ReadMessage(r io.Reader, maxMessageLength int32) (Header, []byte, error) {
	return ReadMessageInto(r, nil, maxMessageLength)
}

// ReadMessageInto reads one wire message and uses dst as reusable storage for
// the returned body when dst has enough capacity. Callers that pass a reusable
// dst must treat the returned body as borrowed: it may alias dst and remains
// valid only until dst is reused or modified. Clone the body before retaining it
// across subsequent reads.
func ReadMessageInto(r io.Reader, dst []byte, maxMessageLength int32) (Header, []byte, error) {
	if maxMessageLength <= 0 {
		maxMessageLength = DefaultMaxMessageLength
	}
	var headerBuf [HeaderLen]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return Header{}, nil, err
	}
	h, err := ParseHeader(headerBuf[:])
	if err != nil {
		return Header{}, nil, err
	}
	if h.MessageLength > maxMessageLength {
		return Header{}, nil, fmt.Errorf("%w: length=%d max=%d", ErrMessageTooLarge, h.MessageLength, maxMessageLength)
	}
	bodyLen := int(h.MessageLength) - HeaderLen
	if cap(dst) < bodyLen {
		dst = make([]byte, bodyLen)
	} else {
		dst = dst[:bodyLen]
	}
	if _, err := io.ReadFull(r, dst); err != nil {
		return Header{}, nil, err
	}
	return h, dst, nil
}

func ValidateDocument(doc Document) error {
	parsed, rem, err := readDocument(doc)
	if err != nil {
		return err
	}
	if len(parsed) != len(doc) || len(rem) != 0 {
		return fmt.Errorf("%w: trailing bytes after document", ErrMalformed)
	}
	return validateParsedDocument(doc)
}

func validateParsedDocument(doc Document) error {
	if err := doc.Validate(); err != nil {
		return fmt.Errorf("%w: invalid BSON document: %v", ErrMalformed, err)
	}
	return nil
}

func CommandName(doc Document) (string, error) {
	if err := ValidateDocument(doc); err != nil {
		return "", err
	}
	return CommandNameFromValidatedDocument(doc)
}

// CommandNameFromValidatedDocument extracts the command name from a document
// that has already passed ValidateDocument, such as a ParseMsg or ParseQuery
// result.
func CommandNameFromValidatedDocument(doc Document) (string, error) {
	elem, err := doc.IndexErr(0)
	if err != nil {
		return "", err
	}
	key, err := elem.KeyErr()
	if err != nil {
		return "", fmt.Errorf("%w: invalid command key: %v", ErrMalformed, err)
	}
	if key == "" {
		return "", fmt.Errorf("%w: empty command name", ErrMalformed)
	}
	return key, nil
}

func readDocument(src []byte) (doc Document, rem []byte, err error) {
	if len(src) < 5 {
		return nil, nil, ErrMessageTooShort
	}
	n := int(int32(binary.LittleEndian.Uint32(src[0:4])))
	if n < 5 {
		return nil, nil, fmt.Errorf("%w: invalid BSON document length %d", ErrMalformed, n)
	}
	if n > len(src) {
		return nil, nil, fmt.Errorf("%w: truncated BSON document length=%d available=%d", ErrMessageTooShort, n, len(src))
	}
	if src[n-1] != 0 {
		return nil, nil, fmt.Errorf("%w: BSON document missing terminator", ErrMalformed)
	}
	return Document(src[:n]), src[n:], nil
}

func readElementHeader(src []byte) (typ byte, key string, rem []byte, err error) {
	if len(src) == 0 {
		return 0, "", nil, ErrMessageTooShort
	}
	typ = src[0]
	key, rem, err = readCString(src[1:])
	return typ, key, rem, err
}

func readCString(src []byte) (string, []byte, error) {
	idx := bytes.IndexByte(src, 0)
	if idx < 0 {
		return "", nil, fmt.Errorf("%w: missing cstring terminator", ErrMalformed)
	}
	return string(src[:idx]), src[idx+1:], nil
}

func validateCString(value string) error {
	if strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: cstring contains NUL", ErrMalformed)
	}
	return nil
}

func appendCString(dst []byte, value string) []byte {
	dst = append(dst, value...)
	return append(dst, 0)
}

func appendInt32(dst []byte, value int32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	return append(dst, buf[:]...)
}

func appendInt64(dst []byte, value int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	return append(dst, buf[:]...)
}
