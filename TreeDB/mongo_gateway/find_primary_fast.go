package mongogateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	fastPrimaryFindDocBufInitCap = 1024
	fastPrimaryFindDocBufMaxCap  = 64 * 1024
)

var fastPrimaryFindDocBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, fastPrimaryFindDocBufInitCap)
		return &buf
	},
}

type fastPrimaryFindRequest struct {
	collection string
	database   string
	idValue    bson.RawValue
}

// parseFastPrimaryEqualityFind parses simple unprojected primary _id equality
// find commands in a single pass without heap allocations. Any command with
// extra options (projection, sort, hint, skip, readConcern, transactions,
// operators, etc.) returns ok=false so the caller runs the general planner.
func parseFastPrimaryEqualityFind(command wire.Document) (fastPrimaryFindRequest, bool) {
	length, rem, ok := bsoncore.ReadLength(bsoncore.Document(command))
	if !ok || length < 5 || int(length) != len(command) {
		return fastPrimaryFindRequest{}, false
	}
	rem = rem[:int(length)-4]
	if len(rem) == 0 || rem[len(rem)-1] != 0x00 {
		return fastPrimaryFindRequest{}, false
	}
	rem = rem[:len(rem)-1]

	var (
		colName    string
		dbName     string
		filterDoc  bsoncore.Document
		hasFind    bool
		hasDB      bool
		hasFilter  bool
		hasLimit   bool
		hasBatch   bool
		hasSingleB bool
	)

	for len(rem) > 0 {
		elem, next, ok := bsoncore.ReadElement(rem)
		if !ok || len(elem) < 2 {
			return fastPrimaryFindRequest{}, false
		}
		nul := bytes.IndexByte(elem[1:], 0)
		if nul < 0 {
			return fastPrimaryFindRequest{}, false
		}
		keyBytes := elem[1 : 1+nul]
		valType := bsoncore.Type(elem[0])
		valBytes := elem[2+nul:]

		switch string(keyBytes) {
		case "find":
			if hasFind || valType != bsoncore.TypeString {
				return fastPrimaryFindRequest{}, false
			}
			s, _, ok := bsoncore.ReadString(valBytes)
			if !ok || s == "" {
				return fastPrimaryFindRequest{}, false
			}
			colName = s
			hasFind = true
		case "$db":
			if hasDB || valType != bsoncore.TypeString {
				return fastPrimaryFindRequest{}, false
			}
			s, _, ok := bsoncore.ReadString(valBytes)
			if !ok || s == "" {
				return fastPrimaryFindRequest{}, false
			}
			dbName = s
			hasDB = true
		case "filter":
			if hasFilter || valType != bsoncore.TypeEmbeddedDocument {
				return fastPrimaryFindRequest{}, false
			}
			doc, _, ok := bsoncore.ReadDocument(valBytes)
			if !ok {
				return fastPrimaryFindRequest{}, false
			}
			filterDoc = doc
			hasFilter = true
		case "limit":
			if hasLimit {
				return fastPrimaryFindRequest{}, false
			}
			switch valType {
			case bsoncore.TypeInt32:
				v, _, ok := bsoncore.ReadInt32(valBytes)
				if !ok || v < 0 {
					return fastPrimaryFindRequest{}, false
				}
			case bsoncore.TypeInt64:
				v, _, ok := bsoncore.ReadInt64(valBytes)
				if !ok || v < 0 {
					return fastPrimaryFindRequest{}, false
				}
			default:
				return fastPrimaryFindRequest{}, false
			}
			hasLimit = true
		case "batchSize":
			if hasBatch {
				return fastPrimaryFindRequest{}, false
			}
			switch valType {
			case bsoncore.TypeInt32:
				v, _, ok := bsoncore.ReadInt32(valBytes)
				if !ok || v < 1 {
					return fastPrimaryFindRequest{}, false
				}
			case bsoncore.TypeInt64:
				v, _, ok := bsoncore.ReadInt64(valBytes)
				if !ok || v < 1 {
					return fastPrimaryFindRequest{}, false
				}
			default:
				return fastPrimaryFindRequest{}, false
			}
			hasBatch = true
		case "singleBatch":
			if hasSingleB || valType != bsoncore.TypeBoolean {
				return fastPrimaryFindRequest{}, false
			}
			if _, _, ok := bsoncore.ReadBoolean(valBytes); !ok {
				return fastPrimaryFindRequest{}, false
			}
			hasSingleB = true
		default:
			return fastPrimaryFindRequest{}, false
		}
		rem = next
	}

	if !hasFind || !hasDB || !hasFilter {
		return fastPrimaryFindRequest{}, false
	}

	idVal, ok := parseSinglePrimaryIDFilter(filterDoc)
	if !ok {
		return fastPrimaryFindRequest{}, false
	}

	return fastPrimaryFindRequest{
		collection: colName,
		database:   dbName,
		idValue:    idVal,
	}, true
}

func parseSinglePrimaryIDFilter(filterDoc bsoncore.Document) (bson.RawValue, bool) {
	length, rem, ok := bsoncore.ReadLength(filterDoc)
	if !ok || length < 5 || int(length) != len(filterDoc) {
		return bson.RawValue{}, false
	}
	rem = rem[:int(length)-4]
	if len(rem) == 0 || rem[len(rem)-1] != 0x00 {
		return bson.RawValue{}, false
	}
	rem = rem[:len(rem)-1]
	if len(rem) == 0 {
		return bson.RawValue{}, false
	}
	elem, next, ok := bsoncore.ReadElement(rem)
	if !ok || len(next) != 0 || len(elem) < 2 {
		return bson.RawValue{}, false
	}
	nul := bytes.IndexByte(elem[1:], 0)
	if nul != 3 || elem[1] != '_' || elem[2] != 'i' || elem[3] != 'd' {
		return bson.RawValue{}, false
	}
	valType := bson.Type(elem[0])
	switch valType {
	case bson.TypeString, bson.TypeObjectID, bson.TypeBoolean, bson.TypeBinary, bson.TypeDateTime, bson.TypeTimestamp:
	default:
		// Conservatively fall back for numeric types (cross-type equality),
		// embedded documents (operators / subdocs), arrays, regex, null, etc.
		return bson.RawValue{}, false
	}
	return bson.RawValue{
		Type:  valType,
		Value: elem[2+nul:],
	}, true
}

// tryFastPrimaryEqualityFindMsgResponseInto serves simple unprojected primary
// _id equality find commands directly into dst without opening a
// CollectionReadView, copying CollectionMeta/IndexDefinitions, or allocating
// intermediate planner/batch structures.
func (s *Server) tryFastPrimaryEqualityFindMsgResponseInto(_ context.Context, dst []byte, command wire.Document, requestID, responseTo int32) ([]byte, bool, error) {
	if s == nil || s.Collections == nil || s.clusterRouteProviderConfigured() {
		return dst, false, nil
	}
	req, ok := parseFastPrimaryEqualityFind(command)
	if !ok {
		return dst, false, nil
	}
	ns, err := gatewayCollectionName(req.database, req.collection)
	if err != nil {
		return dst, false, nil
	}
	col, err := s.openCollectionCached(ns)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		msg, encErr := appendSingleDocCursorMsgResponseInto(dst, requestID, responseTo, ns, nil, false, int(s.maxMessageLength()))
		return msg, true, encErr
	}
	if err != nil {
		return dst, false, nil
	}

	var keyStack [128]byte
	key, err := encodePrimaryKeyInto(keyStack[:0], req.idValue)
	if err != nil {
		return dst, false, nil
	}

	bufPtr := fastPrimaryFindDocBufPool.Get().(*[]byte)
	docBuf := (*bufPtr)[:0]
	stored, format, found, err := col.GetIntoWithFormat(key, docBuf)
	if err != nil {
		if cap(stored) <= fastPrimaryFindDocBufMaxCap {
			*bufPtr = stored[:0]
			fastPrimaryFindDocBufPool.Put(bufPtr)
		}
		if errors.Is(err, collections.ErrCollectionNotFound) {
			msg, encErr := appendSingleDocCursorMsgResponseInto(dst, requestID, responseTo, ns, nil, false, int(s.maxMessageLength()))
			return msg, true, encErr
		}
		return dst, false, nil
	}
	if !found {
		if cap(stored) <= fastPrimaryFindDocBufMaxCap {
			*bufPtr = stored[:0]
			fastPrimaryFindDocBufPool.Put(bufPtr)
		}
		msg, encErr := appendSingleDocCursorMsgResponseInto(dst, requestID, responseTo, ns, nil, false, int(s.maxMessageLength()))
		return msg, true, encErr
	}
	if format != collections.DocumentFormatBSON {
		if cap(stored) <= fastPrimaryFindDocBufMaxCap {
			*bufPtr = stored[:0]
			fastPrimaryFindDocBufPool.Put(bufPtr)
		}
		return dst, false, nil
	}
	if findBatchOverheadBytes+findBatchDocumentBytes(wire.Document(stored), 0) > s.maxFindBatchBytes() {
		if cap(stored) <= fastPrimaryFindDocBufMaxCap {
			*bufPtr = stored[:0]
			fastPrimaryFindDocBufPool.Put(bufPtr)
		}
		return dst, false, nil
	}

	msg, encErr := appendSingleDocCursorMsgResponseInto(dst, requestID, responseTo, ns, stored, true, int(s.maxMessageLength()))
	if cap(stored) <= fastPrimaryFindDocBufMaxCap {
		*bufPtr = stored[:0]
		fastPrimaryFindDocBufPool.Put(bufPtr)
	}
	return msg, true, encErr
}

func appendSingleDocCursorMsgResponseInto(dst []byte, requestID, responseTo int32, ns string, doc []byte, hasDoc bool, maxMessageLength int) ([]byte, error) {
	if maxMessageLength <= 0 || maxMessageLength > wire.DefaultMaxMessageLength {
		maxMessageLength = wire.DefaultMaxMessageLength
	}
	batchBytes := findBatchOverheadBytes
	if hasDoc {
		batchBytes += findBatchDocumentBytes(wire.Document(doc), 0)
	}
	need := cursorDocumentsMsgResponseLength(ns, "firstBatch", batchBytes)
	if int64(need) > maxWireMessageLengthInt32Limit {
		return nil, fmt.Errorf("%w: length=%d", wire.ErrMessageTooLarge, need)
	}
	if need > maxMessageLength {
		return nil, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, need, maxMessageLength)
	}
	dst = ensureWireAppendCapacity(dst, need)
	msg := dst
	base := len(msg)
	msg = appendWireInt32(msg, 0)
	msg = appendWireInt32(msg, requestID)
	msg = appendWireInt32(msg, responseTo)
	msg = appendWireInt32(msg, int32(wire.OpMsg))
	msg = appendWireInt32(msg, 0)
	msg = append(msg, wire.MsgSectionBody)
	docIdx, msg := bsoncore.AppendDocumentStart(msg)
	cursorIdx, msg := bsoncore.AppendDocumentElementStart(msg, "cursor")
	msg = bsoncore.AppendInt64Element(msg, "id", 0)
	msg = bsoncore.AppendStringElement(msg, "ns", ns)
	batchIdx, msg := bsoncore.AppendArrayElementStart(msg, "firstBatch")
	if hasDoc {
		msg = bsoncore.AppendDocumentElement(msg, "0", doc)
	}
	var err error
	msg, err = bsoncore.AppendArrayEnd(msg, batchIdx)
	if err != nil {
		return nil, err
	}
	msg, err = bsoncore.AppendDocumentEnd(msg, cursorIdx)
	if err != nil {
		return nil, err
	}
	msg = bsoncore.AppendDoubleElement(msg, "ok", 1.0)
	msg, err = bsoncore.AppendDocumentEnd(msg, docIdx)
	if err != nil {
		return nil, err
	}
	messageLength := len(msg) - base
	if int64(messageLength) > maxWireMessageLengthInt32Limit {
		return nil, fmt.Errorf("%w: length=%d", wire.ErrMessageTooLarge, messageLength)
	}
	if messageLength > maxMessageLength {
		return nil, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, messageLength, maxMessageLength)
	}
	binary.LittleEndian.PutUint32(msg[base:base+4], uint32(messageLength))
	return msg, nil
}
