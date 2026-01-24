package template

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
)

// Format:
// [0]=magic0, [1]=magic1, [2]=version, [3]=flags, [4:12]=tplID (uint64), [12:]=tail (mid section)
const (
	magic0              = 'T'
	magic1              = 'M'
	formatVersion       = 1
	flagTemplateEncoded = 1 << 0
	headerSize          = 12
)

var ErrCorrupt = errors.New("template: corrupt payload")

type Template struct {
	ID     uint64
	Prefix []byte
	Suffix []byte
}

// Engine maintains templates and can encode/decode values.
// It is workload-agnostic and uses byte-level similarity only.
type Engine struct {
	mu      sync.RWMutex
	cfg     Config
	history [][]byte
	entries int
	byID    map[uint64]*Template
	list    []*Template
	onNew   func(Template)
}

// NewEngine creates a template engine with normalized config.
func NewEngine(cfg Config) *Engine {
	cfg = NormalizeConfig(cfg)
	return &Engine{
		cfg:  cfg,
		byID: make(map[uint64]*Template),
		list: make([]*Template, 0, 16),
	}
}

// SetOnNewTemplate sets a callback for newly created templates.
func (e *Engine) SetOnNewTemplate(fn func(Template)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onNew = fn
}

// Encode returns the encoded payload, whether a template was used,
// and the template ID (0 if not used).
func (e *Engine) Encode(value []byte) ([]byte, bool, uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(value) == 0 {
		return value, false, 0
	}

	// Try existing templates.
	bestID := uint64(0)
	bestTail := []byte(nil)
	bestSavings := 0
	for _, tpl := range e.list {
		if tpl == nil {
			continue
		}
		tail, ok := matchTemplate(tpl, value)
		if !ok {
			continue
		}
		savings := len(value) - (headerSize + len(tail))
		if savings > bestSavings {
			bestSavings = savings
			bestID = tpl.ID
			bestTail = tail
		}
	}

	if bestID != 0 && bestSavings >= e.cfg.MinSavingsBytes {
		encoded := make([]byte, headerSize+len(bestTail))
		encoded[0] = magic0
		encoded[1] = magic1
		encoded[2] = formatVersion
		encoded[3] = flagTemplateEncoded
		binary.LittleEndian.PutUint64(encoded[4:12], bestID)
		copy(encoded[12:], bestTail)
		e.addHistory(value)
		return encoded, true, bestID
	}

	// No usable template; consider creating a new one from history.
	e.maybeCreateTemplate(value)
	e.addHistory(value)
	return value, false, 0
}

// Decode decodes a template-encoded value if it was encoded.
// If the value is raw, it is returned as-is.
func (e *Engine) Decode(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return value, nil
	}
	if len(value) < headerSize || value[0] != magic0 || value[1] != magic1 || value[2] != formatVersion || value[3]&flagTemplateEncoded == 0 {
		return value, nil
	}
	id := binary.LittleEndian.Uint64(value[4:12])
	tail := value[12:]
	e.mu.RLock()
	tpl := e.byID[id]
	e.mu.RUnlock()
	if tpl == nil {
		return nil, ErrCorrupt
	}
	out := make([]byte, 0, len(tpl.Prefix)+len(tail)+len(tpl.Suffix))
	out = append(out, tpl.Prefix...)
	out = append(out, tail...)
	out = append(out, tpl.Suffix...)
	return out, nil
}

// ExportTemplates returns a snapshot of known templates.
func (e *Engine) ExportTemplates() []Template {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Template, 0, len(e.list))
	for _, tpl := range e.list {
		if tpl == nil {
			continue
		}
		cp := Template{ID: tpl.ID}
		cp.Prefix = append([]byte(nil), tpl.Prefix...)
		cp.Suffix = append([]byte(nil), tpl.Suffix...)
		out = append(out, cp)
	}
	return out
}

func (e *Engine) addHistory(value []byte) {
	if e.cfg.HistoryEntries <= 0 {
		return
	}
	cp := append([]byte(nil), value...)
	if e.entries < e.cfg.HistoryEntries {
		e.history = append(e.history, cp)
		e.entries++
		return
	}
	idx := e.entries % e.cfg.HistoryEntries
	if idx < len(e.history) {
		e.history[idx] = cp
	} else {
		e.history = append(e.history, cp)
	}
	e.entries++
}

func (e *Engine) maybeCreateTemplate(value []byte) {
	if len(e.history) == 0 {
		return
	}
	// Find best candidate from history.
	var bestPrefix []byte
	var bestSuffix []byte
	bestTotal := 0
	for _, prev := range e.history {
		if len(prev) == 0 {
			continue
		}
		p := commonPrefix(value, prev)
		s := commonSuffix(value, prev)
		if len(p) == 0 && len(s) == 0 {
			continue
		}
		if len(p)+len(s) > bestTotal {
			bestTotal = len(p) + len(s)
			bestPrefix = p
			bestSuffix = s
		}
	}
	if bestTotal < e.cfg.MinTotalBytes {
		return
	}
	if len(bestPrefix) < e.cfg.MinPrefixBytes || len(bestSuffix) < e.cfg.MinSuffixBytes {
		return
	}
	if bestTotal > e.cfg.MaxTemplateBytes {
		// Trim to max bytes; preserve prefix+suffix shape.
		over := bestTotal - e.cfg.MaxTemplateBytes
		for over > 0 && len(bestSuffix) > e.cfg.MinSuffixBytes {
			bestSuffix = bestSuffix[:len(bestSuffix)-1]
			over--
		}
		for over > 0 && len(bestPrefix) > e.cfg.MinPrefixBytes {
			bestPrefix = bestPrefix[:len(bestPrefix)-1]
			over--
		}
	}
	if len(bestPrefix)+len(bestSuffix) < e.cfg.MinTotalBytes {
		return
	}
	e.addTemplate(bestPrefix, bestSuffix)
}

func (e *Engine) addTemplate(prefix, suffix []byte) {
	id := templateID(prefix, suffix)
	if id == 0 {
		return
	}
	if existing := e.byID[id]; existing != nil {
		if equalBytes(existing.Prefix, prefix) && equalBytes(existing.Suffix, suffix) {
			return
		}
		// Hash collision; skip to avoid corrupting prior templates.
		return
	}
	tpl := &Template{
		ID:     id,
		Prefix: append([]byte(nil), prefix...),
		Suffix: append([]byte(nil), suffix...),
	}
	e.byID[id] = tpl
	e.list = append(e.list, tpl)
	if e.onNew != nil {
		e.onNew(*tpl)
	}
}

func matchTemplate(tpl *Template, value []byte) ([]byte, bool) {
	if tpl == nil {
		return nil, false
	}
	if len(value) < len(tpl.Prefix)+len(tpl.Suffix) {
		return nil, false
	}
	if len(tpl.Prefix) > 0 && !hasPrefix(value, tpl.Prefix) {
		return nil, false
	}
	if len(tpl.Suffix) > 0 && !hasSuffix(value, tpl.Suffix) {
		return nil, false
	}
	tailStart := len(tpl.Prefix)
	tailEnd := len(value) - len(tpl.Suffix)
	if tailEnd < tailStart {
		return nil, false
	}
	return value[tailStart:tailEnd], true
}

// IsEncodedPayload reports whether value looks like a template-encoded payload.
func IsEncodedPayload(value []byte) bool {
	return len(value) >= headerSize &&
		value[0] == magic0 &&
		value[1] == magic1 &&
		value[2] == formatVersion &&
		value[3]&flagTemplateEncoded != 0
}

// DecodePayload decodes a template-encoded payload using a lookup function.
// If payload is not template-encoded, it is returned as-is.
func DecodePayload(payload []byte, lookup func(id uint64) (prefix, suffix []byte, err error)) ([]byte, error) {
	if len(payload) == 0 || !IsEncodedPayload(payload) {
		return payload, nil
	}
	if lookup == nil {
		return nil, ErrCorrupt
	}
	id := binary.LittleEndian.Uint64(payload[4:12])
	tail := payload[12:]
	prefix, suffix, err := lookup(id)
	if err != nil {
		return nil, err
	}
	if len(prefix) == 0 && len(suffix) == 0 {
		return nil, ErrCorrupt
	}
	out := make([]byte, 0, len(prefix)+len(tail)+len(suffix))
	out = append(out, prefix...)
	out = append(out, tail...)
	out = append(out, suffix...)
	return out, nil
}

func templateID(prefix, suffix []byte) uint64 {
	h := sha256.New()
	_, _ = h.Write(prefix)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(suffix)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func commonPrefix(a, b []byte) []byte {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	if i == 0 {
		return nil
	}
	return a[:i]
}

func commonSuffix(a, b []byte) []byte {
	na := len(a)
	nb := len(b)
	if na == 0 || nb == 0 {
		return nil
	}
	i := 0
	for i < na && i < nb && a[na-1-i] == b[nb-1-i] {
		i++
	}
	if i == 0 {
		return nil
	}
	return a[na-i:]
}

func hasPrefix(b, prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	if len(prefix) > len(b) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

func hasSuffix(b, suffix []byte) bool {
	if len(suffix) == 0 {
		return true
	}
	if len(suffix) > len(b) {
		return false
	}
	offset := len(b) - len(suffix)
	for i := range suffix {
		if b[offset+i] != suffix[i] {
			return false
		}
	}
	return true
}
