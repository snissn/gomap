package slab

import (
	"errors"
	"sync"
)

var errDictUnavailable = errors.New("slab: dictionary unavailable")

func (s *SlabFile) ensureHeaderLoaded() error {
	if s == nil || s.File == nil {
		return nil
	}
	s.headerMu.Lock()
	defer s.headerMu.Unlock()
	if s.headerOK {
		return nil
	}
	header, hasHeader, err := readSlabHeader(s.File, s.Size)
	if err != nil {
		return err
	}
	if hasHeader {
		if s.Size < slabV2DataStart {
			return errSlabHeaderCorrupt
		}
		s.version = slabVersionV2
		s.dataStart = slabV2DataStart
		s.header = header
		s.dictID = header.DictID
		s.dictRaw = header.Flags&slabFlagDictRaw != 0
		if header.Flags&slabFlagDictReady != 0 {
			if err := s.loadDict(); err != nil {
				return err
			}
		}
	}
	s.headerOK = true
	return nil
}

func (s *SlabFile) loadDict() error {
	if s == nil || s.File == nil {
		return errDictUnavailable
	}
	dict := make([]byte, slabV2DictSize)
	if _, err := s.File.ReadAt(dict, slabV2HeaderSize); err != nil {
		return err
	}
	s.dict = dict
	s.dictReady.Store(true)
	return nil
}

func (s *SlabFile) dictPools(cfg *compressionConfig) (*sync.Pool, *sync.Pool) {
	if s == nil || !s.dictReady.Load() || s.dict == nil {
		return nil, nil
	}
	if cfg == nil || cfg.kind == CompressionNone {
		return nil, nil
	}
	s.dictPoolMu.Lock()
	defer s.dictPoolMu.Unlock()
	if s.dictEnc == nil {
		s.dictEnc = cfg.newDictEncoderPool(s.dict, s.dictID, s.dictRaw)
	}
	if s.dictDec == nil {
		s.dictDec = cfg.newDictDecoderPool(s.dict, s.dictID, s.dictRaw)
	}
	return s.dictEnc, s.dictDec
}

func (s *SlabFile) initDictFromSample(cfg *compressionConfig, sample []byte) error {
	if s == nil || s.dictReady.Load() {
		return nil
	}
	if s.File == nil {
		return errDictUnavailable
	}
	if cfg == nil || cfg.kind == CompressionNone {
		return nil
	}
	dict := buildRawDict(sample)
	if len(dict) != slabV2DictSize {
		return errDictUnavailable
	}
	if _, err := s.File.WriteAt(dict, slabV2HeaderSize); err != nil {
		return err
	}
	s.dict = dict
	s.dictRaw = true
	s.dictID = 0
	s.header.Flags |= slabFlagDictReady | slabFlagDictRaw
	s.header.DictID = s.dictID
	if err := writeSlabHeader(s.File, s.header); err != nil {
		return err
	}
	s.dictEnc = cfg.newDictEncoderPool(s.dict, s.dictID, s.dictRaw)
	s.dictDec = cfg.newDictDecoderPool(s.dict, s.dictID, s.dictRaw)
	s.dictReady.Store(true)
	return nil
}

func buildRawDict(sample []byte) []byte {
	if len(sample) == 0 {
		return make([]byte, slabV2DictSize)
	}
	if len(sample) >= slabV2DictSize {
		out := make([]byte, slabV2DictSize)
		copy(out, sample[len(sample)-slabV2DictSize:])
		return out
	}
	out := make([]byte, slabV2DictSize)
	for i := 0; i < slabV2DictSize; {
		n := copy(out[i:], sample)
		i += n
		if n == 0 {
			break
		}
	}
	return out
}
