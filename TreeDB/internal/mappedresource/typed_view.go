package mappedresource

import (
	"errors"
	"fmt"
	"unsafe"
)

var nativeLittleEndian = func() bool {
	var value uint16 = 1
	return *(*byte)(unsafe.Pointer(&value)) == 1
}()

func NativeLittleEndian() bool { return nativeLittleEndian }

var (
	ErrDirectViewNilHandle      = errors.New("mappedresource: nil handle")
	ErrDirectViewReleasedHandle = errors.New("mappedresource: handle is released")
	ErrDirectViewWrongEndian    = errors.New("mappedresource: direct view wrong endian")
	ErrDirectViewLengthMultiple = errors.New("mappedresource: direct view length multiple mismatch")
	ErrDirectViewUnaligned      = errors.New("mappedresource: direct view unaligned")
)

// DirectViewOptions controls validation before raw bytes are reinterpreted as a
// fixed-width typed slice.
type DirectViewOptions struct {
	ElementSize         uintptr
	Alignment           uintptr
	TypeName            string
	RequireLittleEndian bool
}

// ValidateDirectView validates length, alignment, and endian eligibility for a
// zero-copy typed view. It returns the element count.
func ValidateDirectView(data []byte, opts DirectViewOptions) (int, error) {
	if opts.ElementSize == 0 {
		return 0, fmt.Errorf("mappedresource: direct view element size is zero")
	}
	if opts.Alignment == 0 {
		opts.Alignment = opts.ElementSize
	}
	if opts.TypeName == "" {
		opts.TypeName = fmt.Sprintf("%d-byte", opts.ElementSize)
	}
	if opts.RequireLittleEndian && !nativeLittleEndian {
		return 0, fmt.Errorf("%w: %s direct view requires little-endian host", ErrDirectViewWrongEndian, opts.TypeName)
	}
	if uintptr(len(data))%opts.ElementSize != 0 {
		return 0, fmt.Errorf("%w: %s direct view length=%d is not multiple of element size=%d", ErrDirectViewLengthMultiple, opts.TypeName, len(data), opts.ElementSize)
	}
	if len(data) == 0 {
		return 0, nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	if addr%opts.Alignment != 0 {
		return 0, fmt.Errorf("%w: %s direct view address=%#x is not %d-byte aligned", ErrDirectViewUnaligned, opts.TypeName, addr, opts.Alignment)
	}
	return int(uintptr(len(data)) / opts.ElementSize), nil
}

// Uint16View exposes bytes as []uint16 after validation.
func Uint16View(data []byte) ([]uint16, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(uint16(0)), Alignment: unsafe.Alignof(uint16(0)), TypeName: "uint16", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*uint16)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Int64View exposes bytes as []int64 after validation.
func Int64View(data []byte) ([]int64, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(int64(0)), Alignment: unsafe.Alignof(int64(0)), TypeName: "int64", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Float32View exposes bytes as []float32 after validation.
func Float32View(data []byte) ([]float32, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(float32(0)), Alignment: unsafe.Alignof(float32(0)), TypeName: "float32", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Float64View exposes bytes as []float64 after validation.
func Float64View(data []byte) ([]float64, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(float64(0)), Alignment: unsafe.Alignof(float64(0)), TypeName: "float64", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Uint32View exposes bytes as []uint32 after validation.
func Uint32View(data []byte) ([]uint32, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(uint32(0)), Alignment: unsafe.Alignof(uint32(0)), TypeName: "uint32", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Uint64View exposes bytes as []uint64 after validation.
func Uint64View(data []byte) ([]uint64, error) {
	count, err := ValidateDirectView(data, DirectViewOptions{ElementSize: unsafe.Sizeof(uint64(0)), Alignment: unsafe.Alignof(uint64(0)), TypeName: "uint64", RequireLittleEndian: true})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data))), count), nil
}

// Uint16View records direct-view success/failure in addition to validating.
func (m *Manager) Uint16View(h *Handle) ([]uint16, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Uint16View(data)
	m.recordDirectView(err)
	return view, err
}

// Int64View records direct-view success/failure in addition to validating.
func (m *Manager) Int64View(h *Handle) ([]int64, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Int64View(data)
	m.recordDirectView(err)
	return view, err
}

// Float32View records direct-view success/failure in addition to validating.
func (m *Manager) Float32View(h *Handle) ([]float32, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Float32View(data)
	m.recordDirectView(err)
	return view, err
}

// Float64View records direct-view success/failure in addition to validating.
func (m *Manager) Float64View(h *Handle) ([]float64, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Float64View(data)
	m.recordDirectView(err)
	return view, err
}

// Uint32View records direct-view success/failure in addition to validating.
func (m *Manager) Uint32View(h *Handle) ([]uint32, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Uint32View(data)
	m.recordDirectView(err)
	return view, err
}

// Uint64View records direct-view success/failure in addition to validating.
func (m *Manager) Uint64View(h *Handle) ([]uint64, error) {
	data, err := liveHandleBytes(h)
	if err != nil {
		m.recordDirectView(err)
		return nil, err
	}
	view, err := Uint64View(data)
	m.recordDirectView(err)
	return view, err
}

func liveHandleBytes(h *Handle) ([]byte, error) {
	if h == nil {
		return nil, ErrDirectViewNilHandle
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return nil, ErrDirectViewReleasedHandle
	}
	return h.bytes, nil
}
