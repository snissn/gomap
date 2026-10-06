package valuelog

import (
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

// ProducedFrameObserver consumes producer-owned metadata only during a
// successful append. first is the first produced grouped pointer, and count
// is this frame's record count. No record/RID buffer or writer storage escapes.
// The observer must be bounded, perform no IO, and copy scalar metadata before
// returning. It is a visibility/dependency seam, not a durability authority.
type ProducedFrameObserver func(header FrameHeader, first page.ValuePtr, count int)

// SwapProducedFrameObserver requires the writer's existing exclusive append
// owner. Restore the returned observer before relinquishing that owner, on
// every success and error path. Writers themselves are not concurrent objects.
func (w *Writer) SwapProducedFrameObserver(next ProducedFrameObserver) ProducedFrameObserver {
	old := w.producedFrameObserver
	w.producedFrameObserver = next
	return old
}

func producedFrameHeader(body []byte) FrameHeader {
	return FrameHeader{Version: body[0], Flags: body[1], K: body[2], Reserved: body[3], DictID: binary.LittleEndian.Uint64(body[4:12])}
}

func (w *Writer) observeProducedFrame(header FrameHeader, first page.ValuePtr, count int) {
	if w.producedFrameObserver != nil {
		w.producedFrameObserver(header, first, count)
	}
}

func (w *Writer) observeProducedPointer(header FrameHeader, ptr page.ValuePtr) page.ValuePtr {
	w.observeProducedFrame(header, ptr, 1)
	return ptr
}

func (w *Writer) observeProducedRawFrames(ptrs []page.ValuePtr) {
	if w.producedFrameObserver == nil {
		return
	}
	for start := 0; start < len(ptrs); {
		end := start + 1
		for end < len(ptrs) && ptrs[end].Offset == ptrs[start].Offset && ptrs[end].FileID == ptrs[start].FileID {
			end++
		}
		w.observeProducedFrame(FrameHeader{Version: FrameVersion, K: uint8(end - start)}, ptrs[start], end-start)
		start = end
	}
}
