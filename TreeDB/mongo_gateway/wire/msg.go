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
	MsgSectionBody byte = 0
)

type Msg struct {
	Flags MsgFlag
	Body  Document
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
	if rem[0] != MsgSectionBody {
		return Msg{}, fmt.Errorf("%w: OP_MSG MVP only supports kind 0 body sections", ErrUnsupported)
	}
	doc, rem, err := readDocument(rem[1:])
	if err != nil {
		return Msg{}, err
	}
	if err := ValidateDocument(doc); err != nil {
		return Msg{}, err
	}
	if len(rem) != 0 {
		return Msg{}, fmt.Errorf("%w: OP_MSG MVP supports exactly one kind 0 section", ErrUnsupported)
	}
	return Msg{Flags: flags, Body: doc}, nil
}

func AppendMsgBody(dst []byte, flags MsgFlag, doc Document) ([]byte, error) {
	if err := validateMsgFlags(flags); err != nil {
		return nil, err
	}
	if err := ValidateDocument(doc); err != nil {
		return nil, err
	}
	dst = appendInt32(dst, int32(flags))
	dst = append(dst, MsgSectionBody)
	dst = append(dst, doc...)
	return dst, nil
}

func AppendMsgMessage(dst []byte, requestID, responseTo int32, flags MsgFlag, doc Document) ([]byte, error) {
	body, err := AppendMsgBody(nil, flags, doc)
	if err != nil {
		return nil, err
	}
	return AppendMessage(dst, requestID, responseTo, OpMsg, body), nil
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
