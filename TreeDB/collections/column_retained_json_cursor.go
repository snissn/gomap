package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/buger/jsonparser"
)

// The cursor owns only block-local parser scratch. A document beyond this
// structural depth is handed to the established collector by the preparation
// seam, preserving current behavior without retaining an unbounded parse tree.
const columnRetainedSemanticStreamV1JSONCursorMaxDepth = 1024

// Nodes and members describe structure, not payload bytes. Capping them keeps
// the reusable cursor arena bounded even when a document has an adversarially
// wide object; the prepare seam falls back before retained emission.
const columnRetainedSemanticStreamV1JSONCursorMaxDescriptors = 4096

// Escaped keys require temporary decoded bytes for trie lookup. Keep a normal
// key's scratch reusable, but do not pin an unusually large escaped key for
// the rest of a multi-document preparation block.
const columnRetainedSemanticStreamV1JSONCursorMaxRetainedUnescapeScratch = 64 << 10

var (
	errColumnRetainedSemanticStreamV1JSONCursorDepth   = errors.New("collections: semantic-stream-v1 JSON cursor depth exceeds block-local bound")
	errColumnRetainedSemanticStreamV1JSONCursorScratch = errors.New("collections: semantic-stream-v1 JSON cursor descriptor budget exceeds block-local bound")
)

// columnRetainedSemanticStreamV1JSONCursor is a private structural cursor for
// the generic semantic-stream-v1 retained path. It records source ranges and
// object-key slices in one validation walk, then emits retained and declared
// values from that immutable in-memory structure. Keys are decoded only when
// their object is semantically visited, matching the established collector's
// behavior for terminal skipped subtrees.
//
// Its slices are reset between documents and retained for one preparation
// block, avoiding per-document parser allocation while keeping all ownership
// local to the prepare worker.
type columnRetainedSemanticStreamV1JSONCursor struct {
	document        []byte
	pos             int
	nodeStack       [64]columnRetainedSemanticStreamV1JSONCursorNode
	nodes           []columnRetainedSemanticStreamV1JSONCursorNode
	memberStack     [64]columnRetainedSemanticStreamV1JSONCursorMember
	members         []columnRetainedSemanticStreamV1JSONCursorMember
	unescapeScratch []byte
	pathInterner    *columnRetainedSemanticStreamV1PathSegmentInterner
}

type columnRetainedSemanticStreamV1JSONCursorNode struct {
	valueType   jsonparser.ValueType
	rawStart    int
	rawEnd      int
	valueStart  int
	valueEnd    int
	firstMember int
}

type columnRetainedSemanticStreamV1JSONCursorMember struct {
	keyStart int
	keyEnd   int
	node     int
	next     int
}

func (c *columnRetainedSemanticStreamV1JSONCursor) reset(document []byte, pathInterner *columnRetainedSemanticStreamV1PathSegmentInterner) {
	c.document = document
	c.pos = 0
	if cap(c.nodes) < len(c.nodeStack) {
		c.nodes = c.nodeStack[:0]
	} else {
		c.nodes = c.nodes[:0]
	}
	if cap(c.members) < len(c.memberStack) {
		c.members = c.memberStack[:0]
	} else {
		c.members = c.members[:0]
	}
	if cap(c.unescapeScratch) > columnRetainedSemanticStreamV1JSONCursorMaxRetainedUnescapeScratch {
		c.unescapeScratch = nil
	} else {
		c.unescapeScratch = c.unescapeScratch[:0]
	}
	c.pathInterner = pathInterner
}

func (c *columnRetainedSemanticStreamV1JSONCursor) parseDocument(document []byte, pathInterner *columnRetainedSemanticStreamV1PathSegmentInterner) (int, error) {
	c.reset(document, pathInterner)
	c.skipSpace()
	root, err := c.parseValue(0, true)
	if err != nil {
		return 0, err
	}
	c.skipSpace()
	if c.pos != len(c.document) {
		return 0, errors.New("trailing JSON bytes")
	}
	if c.nodes[root].valueType != jsonparser.Object {
		return 0, errors.New("column retained payload root must be a JSON object")
	}
	return root, nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) parseValue(depth int, retainObject bool) (int, error) {
	if depth > columnRetainedSemanticStreamV1JSONCursorMaxDepth {
		return 0, errColumnRetainedSemanticStreamV1JSONCursorDepth
	}
	c.skipSpace()
	if c.pos >= len(c.document) {
		return 0, errors.New("unexpected end of JSON value")
	}
	start := c.pos
	switch c.document[c.pos] {
	case '{':
		if !retainObject {
			if err := c.skipObject(depth); err != nil {
				return 0, err
			}
			return -1, nil
		}
		return c.parseObject(depth)
	case '[':
		if err := c.skipArray(depth); err != nil {
			return 0, err
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.Array, start, c.pos, start, c.pos, -1)
	case '"':
		valueStart, valueEnd, err := c.scanString()
		if err != nil {
			return 0, err
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.String, start, c.pos, valueStart, valueEnd, -1)
	case 't':
		if !c.consumeLiteral("true") {
			return 0, errors.New("invalid JSON literal")
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.Boolean, start, c.pos, start, c.pos, -1)
	case 'f':
		if !c.consumeLiteral("false") {
			return 0, errors.New("invalid JSON literal")
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.Boolean, start, c.pos, start, c.pos, -1)
	case 'n':
		if !c.consumeLiteral("null") {
			return 0, errors.New("invalid JSON literal")
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.Null, start, c.pos, start, c.pos, -1)
	default:
		if err := c.scanNumber(); err != nil {
			return 0, err
		}
		if !retainObject {
			return -1, nil
		}
		return c.appendNode(jsonparser.Number, start, c.pos, start, c.pos, -1)
	}
}

func (c *columnRetainedSemanticStreamV1JSONCursor) parseObject(depth int) (int, error) {
	start := c.pos
	c.pos++
	nodeIdx, err := c.appendNode(jsonparser.Object, start, 0, start, 0, -1)
	if err != nil {
		return 0, err
	}
	c.skipSpace()
	if c.consumeByte('}') {
		c.nodes[nodeIdx].rawEnd = c.pos
		c.nodes[nodeIdx].valueEnd = c.pos
		return nodeIdx, nil
	}
	lastMember := -1
	for {
		c.skipSpace()
		if c.pos >= len(c.document) || c.document[c.pos] != '"' {
			return 0, errors.New("JSON object key must be a string")
		}
		keyStart, keyEnd, err := c.scanString()
		if err != nil {
			return 0, err
		}
		c.skipSpace()
		if !c.consumeByte(':') {
			return 0, errors.New("missing JSON object colon")
		}
		child, err := c.parseValue(depth+1, true)
		if err != nil {
			return 0, err
		}
		if len(c.members) >= columnRetainedSemanticStreamV1JSONCursorMaxDescriptors {
			return 0, errColumnRetainedSemanticStreamV1JSONCursorScratch
		}
		memberIdx := len(c.members)
		c.members = append(c.members, columnRetainedSemanticStreamV1JSONCursorMember{keyStart: keyStart, keyEnd: keyEnd, node: child, next: -1})
		if lastMember < 0 {
			c.nodes[nodeIdx].firstMember = memberIdx
		} else {
			c.members[lastMember].next = memberIdx
		}
		lastMember = memberIdx
		c.skipSpace()
		if c.consumeByte('}') {
			c.nodes[nodeIdx].rawEnd = c.pos
			c.nodes[nodeIdx].valueEnd = c.pos
			return nodeIdx, nil
		}
		if !c.consumeByte(',') {
			return 0, errors.New("missing JSON object comma")
		}
	}
}

func (c *columnRetainedSemanticStreamV1JSONCursor) skipObject(depth int) error {
	if !c.consumeByte('{') {
		return errors.New("missing JSON object start")
	}
	c.skipSpace()
	if c.consumeByte('}') {
		return nil
	}
	for {
		c.skipSpace()
		if c.pos >= len(c.document) || c.document[c.pos] != '"' {
			return errors.New("JSON object key must be a string")
		}
		if _, _, err := c.scanString(); err != nil {
			return err
		}
		c.skipSpace()
		if !c.consumeByte(':') {
			return errors.New("missing JSON object colon")
		}
		if _, err := c.parseValue(depth+1, false); err != nil {
			return err
		}
		c.skipSpace()
		if c.consumeByte('}') {
			return nil
		}
		if !c.consumeByte(',') {
			return errors.New("missing JSON object comma")
		}
	}
}

func (c *columnRetainedSemanticStreamV1JSONCursor) skipArray(depth int) error {
	if !c.consumeByte('[') {
		return errors.New("missing JSON array start")
	}
	c.skipSpace()
	if c.consumeByte(']') {
		return nil
	}
	for {
		if _, err := c.parseValue(depth+1, false); err != nil {
			return err
		}
		c.skipSpace()
		if c.consumeByte(']') {
			return nil
		}
		if !c.consumeByte(',') {
			return errors.New("missing JSON array comma")
		}
	}
}

func (c *columnRetainedSemanticStreamV1JSONCursor) scanString() (int, int, error) {
	if !c.consumeByte('"') {
		return 0, 0, errors.New("missing JSON string quote")
	}
	start := c.pos
	for c.pos < len(c.document) {
		ch := c.document[c.pos]
		switch {
		case ch == '"':
			end := c.pos
			c.pos++
			return start, end, nil
		case ch < 0x20:
			return 0, 0, errors.New("invalid control character in JSON string")
		case ch != '\\':
			c.pos++
		default:
			c.pos++
			if c.pos >= len(c.document) {
				return 0, 0, errors.New("truncated JSON string escape")
			}
			escape := c.document[c.pos]
			c.pos++
			switch escape {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if c.pos+4 > len(c.document) {
					return 0, 0, errors.New("truncated JSON unicode escape")
				}
				for _, hex := range c.document[c.pos : c.pos+4] {
					if !columnRetainedSemanticStreamV1JSONCursorHex(hex) {
						return 0, 0, errors.New("invalid JSON unicode escape")
					}
				}
				c.pos += 4
			default:
				return 0, 0, errors.New("invalid JSON string escape")
			}
		}
	}
	return 0, 0, errors.New("unterminated JSON string")
}

func (c *columnRetainedSemanticStreamV1JSONCursor) scanNumber() error {
	start := c.pos
	if c.consumeByte('-') && c.pos >= len(c.document) {
		return errors.New("truncated JSON number")
	}
	if c.pos >= len(c.document) {
		return errors.New("missing JSON number")
	}
	if c.document[c.pos] == '0' {
		c.pos++
		if c.pos < len(c.document) && c.document[c.pos] >= '0' && c.document[c.pos] <= '9' {
			return errors.New("invalid leading zero in JSON number")
		}
	} else {
		if c.document[c.pos] < '1' || c.document[c.pos] > '9' {
			return errors.New("invalid JSON value")
		}
		for c.pos < len(c.document) && c.document[c.pos] >= '0' && c.document[c.pos] <= '9' {
			c.pos++
		}
	}
	if c.consumeByte('.') {
		fractionStart := c.pos
		for c.pos < len(c.document) && c.document[c.pos] >= '0' && c.document[c.pos] <= '9' {
			c.pos++
		}
		if c.pos == fractionStart {
			return errors.New("missing JSON number fraction")
		}
	}
	if c.pos < len(c.document) && (c.document[c.pos] == 'e' || c.document[c.pos] == 'E') {
		c.pos++
		if c.pos < len(c.document) && (c.document[c.pos] == '+' || c.document[c.pos] == '-') {
			c.pos++
		}
		exponentStart := c.pos
		for c.pos < len(c.document) && c.document[c.pos] >= '0' && c.document[c.pos] <= '9' {
			c.pos++
		}
		if c.pos == exponentStart {
			return errors.New("missing JSON number exponent")
		}
	}
	if c.pos == start {
		return errors.New("missing JSON number")
	}
	return nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) appendNode(valueType jsonparser.ValueType, rawStart, rawEnd, valueStart, valueEnd, firstMember int) (int, error) {
	if len(c.nodes) >= columnRetainedSemanticStreamV1JSONCursorMaxDescriptors {
		return 0, errColumnRetainedSemanticStreamV1JSONCursorScratch
	}
	idx := len(c.nodes)
	c.nodes = append(c.nodes, columnRetainedSemanticStreamV1JSONCursorNode{
		valueType:   valueType,
		rawStart:    rawStart,
		rawEnd:      rawEnd,
		valueStart:  valueStart,
		valueEnd:    valueEnd,
		firstMember: firstMember,
	})
	return idx, nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) skipSpace() {
	for c.pos < len(c.document) {
		switch c.document[c.pos] {
		case ' ', '\n', '\r', '\t':
			c.pos++
		default:
			return
		}
	}
}

func (c *columnRetainedSemanticStreamV1JSONCursor) consumeByte(want byte) bool {
	if c.pos >= len(c.document) || c.document[c.pos] != want {
		return false
	}
	c.pos++
	return true
}

func (c *columnRetainedSemanticStreamV1JSONCursor) consumeLiteral(want string) bool {
	if len(c.document)-c.pos < len(want) {
		return false
	}
	for i := range len(want) {
		if c.document[c.pos+i] != want[i] {
			return false
		}
	}
	c.pos += len(want)
	return true
}

func columnRetainedSemanticStreamV1JSONCursorHex(ch byte) bool {
	return ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F'
}

func collectColumnRetainedSemanticStreamV1JSONCursorDocument(cfg ColumnStoreConfig, skip *columnRetainedSemanticStreamV1RetainedSkipTrie, document []byte, row uint64, streamEntryCapacity int, streams *columnRetainedSemanticStreamStreams, pathInterner *columnRetainedSemanticStreamV1PathSegmentInterner, declared *columnRetainedSemanticStreamV1DeclaredPathTrie, declaredValues []columnDeclaredValue, declaredStringInterner *columnDeclaredStringInterner, cursor *columnRetainedSemanticStreamV1JSONCursor) ([]columnDeclaredValue, error) {
	if cfg.RetainedPayload != ColumnRetainedPayloadNonColumn {
		return nil, fmt.Errorf("collections: retained payload policy %q cannot produce object payload", cfg.RetainedPayload)
	}
	if cursor == nil {
		cursor = &columnRetainedSemanticStreamV1JSONCursor{}
	}
	root, err := cursor.parseDocument(document, pathInterner)
	if err != nil {
		return nil, fmt.Errorf("collections: invalid JSON document for column retained payload: %w", err)
	}
	var valuesRaw []jsonParserIndexValue
	if declared != nil {
		var stackValues [8]jsonParserIndexValue
		valuesRaw = stackValues[:]
		if len(cfg.Columns) > len(stackValues) {
			valuesRaw = make([]jsonParserIndexValue, len(cfg.Columns))
		} else {
			valuesRaw = valuesRaw[:len(cfg.Columns)]
		}
	}
	if err := cursor.collectObject(root, nil, row, streamEntryCapacity, skip, streams, declared, valuesRaw); err != nil {
		return nil, err
	}
	if declared == nil {
		return nil, nil
	}
	values := declaredValues
	if values == nil && len(cfg.Columns) == 0 {
		values = make([]columnDeclaredValue, 0)
	}
	if len(values) != len(cfg.Columns) {
		values = make([]columnDeclaredValue, len(cfg.Columns))
	}
	var scratch []byte
	for colIdx, col := range cfg.Columns {
		var stringInterner *columnDeclaredStringInterner
		if col.ValueType == ColumnStoreValueString && col.Dictionary {
			stringInterner = declaredStringInterner
		}
		value, err := convertColumnDeclaredJSONParserValueWithStringInterner(col, valuesRaw[colIdx], &scratch, stringInterner)
		if err != nil {
			return nil, fmt.Errorf("%w: column[%d] %q: %v", ErrColumnDeclaredValueUnsupported, colIdx, col.Name, err)
		}
		values[colIdx] = value
	}
	return values, nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) collectObject(nodeIdx int, path []string, row uint64, streamEntryCapacity int, skip *columnRetainedSemanticStreamV1RetainedSkipTrie, streams *columnRetainedSemanticStreamStreams, declared *columnRetainedSemanticStreamV1DeclaredPathTrie, declaredValues []jsonParserIndexValue) error {
	type retainedObjectValue struct {
		path         string
		node         int
		skip         *columnRetainedSemanticStreamV1RetainedSkipTrie
		skipTerminal bool
		declared     *columnRetainedSemanticStreamV1DeclaredPathTrie
	}
	var stack [8]retainedObjectValue
	values := stack[:0]
	for memberIdx := c.nodes[nodeIdx].firstMember; memberIdx >= 0; memberIdx = c.members[memberIdx].next {
		member := c.members[memberIdx]
		key, err := c.memberKey(member)
		if err != nil {
			return err
		}
		node := c.nodes[member.node]
		var childSkip *columnRetainedSemanticStreamV1RetainedSkipTrie
		if skip != nil {
			childSkip = skip.children[key]
		}
		var childDeclared *columnRetainedSemanticStreamV1DeclaredPathTrie
		if declared != nil {
			childDeclared = declared.children[key]
		}
		if childSkip != nil && childSkip.terminal && childDeclared == nil {
			continue
		}
		if childSkip != nil && childSkip.terminal && childDeclared != nil && len(childDeclared.children) == 0 {
			raw := c.declaredRaw(node)
			for _, colIdx := range childDeclared.columnIndexes {
				declaredValues[colIdx] = jsonParserIndexValue{raw: raw, valueType: node.valueType}
			}
			continue
		}
		next := retainedObjectValue{
			path:         key,
			node:         member.node,
			declared:     childDeclared,
			skipTerminal: childSkip != nil && childSkip.terminal,
		}
		if childSkip != nil && len(childSkip.children) > 0 && node.valueType == jsonparser.Object {
			next.skip = childSkip
		}
		replaced := false
		for i := range values {
			if values[i].path == key {
				values[i] = next
				replaced = true
				break
			}
		}
		if !replaced {
			values = append(values, next)
		}
	}
	if len(values) == 0 {
		if len(path) > 0 {
			appendColumnRetainedSemanticStreamValueNoCopy(path, row, []byte("{}"), streamEntryCapacity, streams)
		}
		return nil
	}
	slices.SortFunc(values, func(a, b retainedObjectValue) int { return strings.Compare(a.path, b.path) })
	var pathStack [8]string
	retainedAny := false
	for _, value := range values {
		node := c.nodes[value.node]
		var nextPath []string
		if len(path) == 0 && cap(path) == 0 {
			nextPath = append(pathStack[:0], value.path)
		} else {
			nextPath = append(path, value.path)
		}
		if value.declared != nil {
			if len(value.declared.columnIndexes) > 0 {
				raw := c.declaredRaw(node)
				for _, colIdx := range value.declared.columnIndexes {
					declaredValues[colIdx] = jsonParserIndexValue{raw: raw, valueType: node.valueType}
				}
			}
			if value.skipTerminal {
				if len(value.declared.children) > 0 && node.valueType == jsonparser.Object {
					if err := c.collectDeclared(value.node, value.declared, declaredValues); err != nil {
						return err
					}
				}
				continue
			}
		}
		if value.skip != nil {
			retainedAny = true
			if err := c.collectObject(value.node, nextPath, row, streamEntryCapacity, value.skip, streams, value.declared, declaredValues); err != nil {
				return err
			}
			continue
		}
		if value.declared != nil && len(value.declared.children) > 0 && node.valueType == jsonparser.Object {
			retainedAny = true
			if err := c.collectObject(value.node, nextPath, row, streamEntryCapacity, nil, streams, value.declared, declaredValues); err != nil {
				return err
			}
			continue
		}
		retainedAny = true
		if node.valueType == jsonparser.Object {
			if err := c.collectObject(value.node, nextPath, row, streamEntryCapacity, nil, streams, nil, declaredValues); err != nil {
				return err
			}
			continue
		}
		appendColumnRetainedSemanticStreamValueNoCopy(nextPath, row, c.raw(node), streamEntryCapacity, streams)
	}
	if !retainedAny && len(path) > 0 {
		appendColumnRetainedSemanticStreamValueNoCopy(path, row, []byte("{}"), streamEntryCapacity, streams)
	}
	return nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) collectDeclared(nodeIdx int, declared *columnRetainedSemanticStreamV1DeclaredPathTrie, declaredValues []jsonParserIndexValue) error {
	if declared == nil || len(declared.children) == 0 {
		return nil
	}
	type declaredValue struct {
		node     int
		declared *columnRetainedSemanticStreamV1DeclaredPathTrie
	}
	var stack [8]declaredValue
	values := stack[:0]
	for memberIdx := c.nodes[nodeIdx].firstMember; memberIdx >= 0; memberIdx = c.members[memberIdx].next {
		member := c.members[memberIdx]
		key, err := c.memberKey(member)
		if err != nil {
			return err
		}
		child := declared.children[key]
		if child == nil {
			continue
		}
		next := declaredValue{node: member.node, declared: child}
		replaced := false
		for i := range values {
			if values[i].declared == child {
				values[i] = next
				replaced = true
				break
			}
		}
		if !replaced {
			values = append(values, next)
		}
	}
	for _, value := range values {
		node := c.nodes[value.node]
		if len(value.declared.columnIndexes) > 0 {
			raw := c.declaredRaw(node)
			for _, colIdx := range value.declared.columnIndexes {
				declaredValues[colIdx] = jsonParserIndexValue{raw: raw, valueType: node.valueType}
			}
		}
		if len(value.declared.children) > 0 && node.valueType == jsonparser.Object {
			if err := c.collectDeclared(value.node, value.declared, declaredValues); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *columnRetainedSemanticStreamV1JSONCursor) raw(node columnRetainedSemanticStreamV1JSONCursorNode) []byte {
	return c.document[node.rawStart:node.rawEnd]
}

func (c *columnRetainedSemanticStreamV1JSONCursor) declaredRaw(node columnRetainedSemanticStreamV1JSONCursorNode) []byte {
	return c.document[node.valueStart:node.valueEnd]
}

func (c *columnRetainedSemanticStreamV1JSONCursor) memberKey(member columnRetainedSemanticStreamV1JSONCursorMember) (string, error) {
	key := c.document[member.keyStart:member.keyEnd]
	if bytes.IndexByte(key, '\\') >= 0 {
		decoded, err := jsonparser.Unescape(key, c.unescapeScratch[:0])
		if err != nil {
			return "", err
		}
		key = decoded
		c.unescapeScratch = decoded[:0]
	}
	return c.pathInterner.intern(key), nil
}
