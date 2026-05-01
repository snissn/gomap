package wire

import (
	"encoding/binary"
	"fmt"
)

type MsgFlag uint32

const (
	MsgFlagChecksumPresent MsgFlag = 1 << 0
	MsgFlagMoreToCome      MsgFlag = 1 << 1
	MsgFlagExhaustAllowed  MsgFlag = 1 << 16
)

const (
	MsgSectionBody             byte = 0
	MsgSectionDocumentSequence byte = 1
)

type Msg struct {
	Flags     MsgFlag
	Body      Document
	Sequences []DocumentSequence
}

type DocumentSequence struct {
	Identifier string
	Documents  []Document
}

func ParseMsg(body []byte) (Msg, error) {
	if len(body) < 5 {
		return Msg{}, ErrMessageTooShort
	}
	flags := MsgFlag(binary.LittleEndian.Uint32(body[:4]))
	if err := validateMsgFlags(flags); err != nil {
		return Msg{}, err
	}
	rem := body[4:]
	msg := Msg{Flags: flags}
	for len(rem) > 0 {
		kind := rem[0]
		rem = rem[1:]
		switch kind {
		case MsgSectionBody:
			if msg.Body != nil {
				return Msg{}, fmt.Errorf("%w: duplicate OP_MSG body section", ErrMalformed)
			}
			doc, next, err := readDocument(rem)
			if err != nil {
				return Msg{}, err
			}
			if err := validateParsedDocument(doc); err != nil {
				return Msg{}, err
			}
			msg.Body = doc
			rem = next
		case MsgSectionDocumentSequence:
			seq, next, err := readDocumentSequence(rem)
			if err != nil {
				return Msg{}, err
			}
			msg.Sequences = append(msg.Sequences, seq)
			rem = next
		default:
			return Msg{}, fmt.Errorf("%w: unknown OP_MSG section kind %d", ErrUnsupported, kind)
		}
	}
	if msg.Body == nil {
		return Msg{}, fmt.Errorf("%w: OP_MSG missing kind 0 body section", ErrMalformed)
	}
	return msg, nil
}

func AppendMsgBody(dst []byte, flags MsgFlag, doc Document) ([]byte, error) {
	return AppendMsgBodyWithSequences(dst, flags, doc, nil)
}

func AppendMsgBodyWithSequences(dst []byte, flags MsgFlag, doc Document, sequences []DocumentSequence) ([]byte, error) {
	if err := validateMsgFlags(flags); err != nil {
		return nil, err
	}
	if err := ValidateDocument(doc); err != nil {
		return nil, err
	}
	dst = appendInt32(dst, int32(flags))
	dst = append(dst, MsgSectionBody)
	dst = append(dst, doc...)
	for _, seq := range sequences {
		if err := validateCString(seq.Identifier); err != nil {
			return nil, err
		}
		if seq.Identifier == "" {
			return nil, fmt.Errorf("%w: OP_MSG document sequence identifier cannot be empty", ErrMalformed)
		}
		dst = append(dst, MsgSectionDocumentSequence)
		sizeOffset := len(dst)
		dst = appendInt32(dst, 0)
		dst = appendCString(dst, seq.Identifier)
		for _, doc := range seq.Documents {
			if err := ValidateDocument(doc); err != nil {
				return nil, err
			}
			dst = append(dst, doc...)
		}
		binary.LittleEndian.PutUint32(dst[sizeOffset:sizeOffset+4], uint32(len(dst)-sizeOffset))
	}
	return dst, nil
}

func AppendMsgMessage(dst []byte, requestID, responseTo int32, flags MsgFlag, doc Document) ([]byte, error) {
	return AppendMsgMessageWithSequences(dst, requestID, responseTo, flags, doc, nil)
}

func AppendMsgMessageWithSequences(dst []byte, requestID, responseTo int32, flags MsgFlag, doc Document, sequences []DocumentSequence) ([]byte, error) {
	base := len(dst)
	dst = appendInt32(dst, 0)
	dst = appendInt32(dst, requestID)
	dst = appendInt32(dst, responseTo)
	dst = appendInt32(dst, int32(OpMsg))
	dst, err := AppendMsgBodyWithSequences(dst, flags, doc, sequences)
	if err != nil {
		return dst[:base], err
	}
	messageLength := len(dst) - base
	if int64(messageLength) > maxInt32 {
		return dst[:base], fmt.Errorf("%w: length=%d exceeds int32 max", ErrMessageTooLarge, messageLength)
	}
	if messageLength > DefaultMaxMessageLength {
		return dst[:base], fmt.Errorf("%w: length=%d max=%d", ErrMessageTooLarge, messageLength, DefaultMaxMessageLength)
	}
	binary.LittleEndian.PutUint32(dst[base:base+4], uint32(messageLength))
	return dst, nil
}

func readDocumentSequence(src []byte) (DocumentSequence, []byte, error) {
	if len(src) < 4 {
		return DocumentSequence{}, nil, ErrMessageTooShort
	}
	size := int(int32(binary.LittleEndian.Uint32(src[:4])))
	if size < 5 {
		return DocumentSequence{}, nil, fmt.Errorf("%w: invalid OP_MSG document sequence size %d", ErrMalformed, size)
	}
	if size > len(src) {
		return DocumentSequence{}, nil, fmt.Errorf("%w: truncated OP_MSG document sequence size=%d available=%d", ErrMessageTooShort, size, len(src))
	}
	section := src[4:size]
	identifier, rem, err := readCString(section)
	if err != nil {
		return DocumentSequence{}, nil, err
	}
	if identifier == "" {
		return DocumentSequence{}, nil, fmt.Errorf("%w: OP_MSG document sequence identifier cannot be empty", ErrMalformed)
	}
	seq := DocumentSequence{
		Identifier: identifier,
		Documents:  make([]Document, 0),
	}
	for len(rem) > 0 {
		doc, next, err := readDocument(rem)
		if err != nil {
			return DocumentSequence{}, nil, err
		}
		if err := validateParsedDocument(doc); err != nil {
			return DocumentSequence{}, nil, err
		}
		seq.Documents = append(seq.Documents, doc)
		rem = next
	}
	return seq, src[size:], nil
}

func validateMsgFlags(flags MsgFlag) error {
	knownRequired := MsgFlagChecksumPresent | MsgFlagMoreToCome
	if unknownRequired := flags & 0xffff &^ knownRequired; unknownRequired != 0 {
		return fmt.Errorf("%w: unknown required OP_MSG flags %#x", ErrMalformed, uint32(unknownRequired))
	}
	if flags&MsgFlagChecksumPresent != 0 {
		return fmt.Errorf("%w: OP_MSG checksum", ErrUnsupported)
	}
	return nil
}
