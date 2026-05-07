package nativewire

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func readFrame(r io.Reader, limits iwire.Limits) (iwire.Header, []byte, error) {
	var headerBuf [iwire.FrameHeaderLenV1]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return iwire.Header{}, nil, err
	}
	header, err := iwire.DecodeHeader(headerBuf[:], limits)
	if err != nil {
		return iwire.Header{}, nil, err
	}
	if header.BodyLen == 0 {
		return header, nil, nil
	}
	if header.BodyLen > uint64(maxInt) {
		return iwire.Header{}, nil, protocolError(iwire.ErrResourceExhausted, "frame body exceeds int capacity")
	}
	body := make([]byte, int(header.BodyLen))
	if _, err := io.ReadFull(r, body); err != nil {
		return iwire.Header{}, nil, err
	}
	return header, body, nil
}

func writeFrame(w io.Writer, header iwire.Header, body []byte) error {
	header.BodyLen = uint64(len(body))
	var headerBuf [iwire.FrameHeaderLenV1]byte
	frame, err := iwire.AppendHeader(headerBuf[:0], header)
	if err != nil {
		return err
	}
	if err := writeAll(w, frame); err != nil {
		return err
	}
	return writeAll(w, body)
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func appendCommandRequestBody(dst []byte, commandID iwire.CommandID, sections ...iwire.Section) ([]byte, error) {
	var commandHeader [16]byte
	headerSection := iwire.Section{
		ID:    iwire.SectionCommandHeader,
		Bytes: iwire.AppendCommandHeader(commandHeader[:0], iwire.CommandHeader{ID: commandID, Version: 1}),
	}
	total := iwire.SectionEncodedLen(headerSection)
	for _, section := range sections {
		total += iwire.SectionEncodedLen(section)
	}
	if cap(dst)-len(dst) < total {
		next := make([]byte, len(dst), len(dst)+total)
		copy(next, dst)
		dst = next
	}
	body, err := iwire.AppendSection(dst, iwire.Section{
		ID:    headerSection.ID,
		Bytes: headerSection.Bytes,
	})
	if err != nil {
		return nil, err
	}
	for _, section := range sections {
		body, err = iwire.AppendSection(body, section)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func readString(src []byte, off *int) (string, error) {
	if off == nil || *off > len(src) {
		return "", protocolError(iwire.ErrMalformedFrame, "invalid string offset")
	}
	n, read := binary.Uvarint(src[*off:])
	if read <= 0 {
		return "", protocolError(iwire.ErrMalformedFrame, "invalid string length")
	}
	*off += read
	if n > uint64(len(src)-*off) {
		return "", protocolError(iwire.ErrMalformedFrame, "string length exceeds remaining payload")
	}
	out := string(src[*off : *off+int(n)])
	*off += int(n)
	return out, nil
}

func appendStringMap(dst []byte, values map[string]string) []byte {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	dst = binary.AppendUvarint(dst, uint64(len(keys)))
	for _, key := range keys {
		dst = appendString(dst, key)
		dst = appendString(dst, values[key])
	}
	return dst
}

func decodeStringMap(src []byte) (map[string]string, error) {
	count, off, err := readUvarint(src)
	if err != nil {
		return nil, err
	}
	if count > uint64(maxInt) {
		return nil, protocolError(iwire.ErrResourceExhausted, "string map count exceeds int capacity")
	}
	out := make(map[string]string, int(count))
	for i := uint64(0); i < count; i++ {
		key, err := readString(src, &off)
		if err != nil {
			return nil, err
		}
		value, err := readString(src, &off)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	if off != len(src) {
		return nil, protocolError(iwire.ErrMalformedFrame, "string map has %d trailing bytes", len(src)-off)
	}
	return out, nil
}

func readUvarint(src []byte) (uint64, int, error) {
	value, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, 0, protocolError(iwire.ErrMalformedFrame, "invalid uvarint")
	}
	return value, n, nil
}

func appendErrorPayload(dst []byte, code iwire.ErrorCode, retryable bool, message string) []byte {
	dst = binary.AppendUvarint(dst, uint64(code))
	if retryable {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return appendString(dst, message)
}

func decodeErrorPayload(src []byte) (iwire.ErrorCode, bool, string, error) {
	code, off, err := readUvarint(src)
	if err != nil {
		return 0, false, "", err
	}
	if off >= len(src) {
		return 0, false, "", protocolError(iwire.ErrMalformedFrame, "missing error retryable flag")
	}
	retryable := src[off] != 0
	off++
	message, err := readString(src, &off)
	if err != nil {
		return 0, false, "", err
	}
	if off != len(src) {
		return 0, false, "", protocolError(iwire.ErrMalformedFrame, "error payload has %d trailing bytes", len(src)-off)
	}
	return iwire.ErrorCode(code), retryable, message, nil
}

func sectionsByID(sections []iwire.Section, id iwire.SectionID) []iwire.Section {
	var out []iwire.Section
	for _, section := range sections {
		if section.ID == id {
			out = append(out, section)
		}
	}
	return out
}

func singletonSection(sections []iwire.Section, id iwire.SectionID) ([]byte, bool, error) {
	var out []byte
	found := false
	for _, section := range sections {
		if section.ID != id {
			continue
		}
		if found {
			return nil, false, protocolError(iwire.ErrInvalidCommand, "duplicate section %d", id)
		}
		out = section.Bytes
		found = true
	}
	return out, found, nil
}

func statsString(key string, value uint64) (string, string) {
	return key, strconv.FormatUint(value, 10)
}

func protocolError(code iwire.ErrorCode, format string, args ...any) error {
	return &iwire.ProtocolError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

const maxInt = int(^uint(0) >> 1)
