package node

import "testing"

func TestBuilderReleaseScratchBoundsPooledByteArenas(t *testing.T) {
	tests := []struct {
		name    string
		release func(*Builder, *byteArenaHandle, []byte)
	}{
		{
			name: "columnar leaf",
			release: func(b *Builder, h *byteArenaHandle, buf []byte) {
				b.leafColumnarV2ArenaH, b.leafColumnarV2Arena = h, buf
				b.releaseLeafColumnarV2Scratch()
			},
		},
		{
			name: "prefix leaf",
			release: func(b *Builder, h *byteArenaHandle, buf []byte) {
				b.leafColumnarPrefixV2ValueArenaH, b.leafColumnarPrefixV2ValueArena = h, buf
				b.releaseLeafColumnarPrefixV2Scratch()
			},
		},
		{
			name: "internal base",
			release: func(b *Builder, h *byteArenaHandle, buf []byte) {
				b.internalBaseArenaH, b.internalBaseArena = h, buf
				b.releaseInternalBaseDeltaScratch()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, capacity := range []int{builderByteArenaPoolMaxCap, builderByteArenaPoolMaxCap + 1} {
				h := &byteArenaHandle{}
				tt.release(&Builder{}, h, make([]byte, 1, capacity))
				if got := cap(h.buf); got > builderByteArenaPoolMaxCap {
					t.Fatalf("released arena capacity = %d, max %d", got, builderByteArenaPoolMaxCap)
				}
				if capacity > builderByteArenaPoolMaxCap && h.buf != nil {
					t.Fatal("oversized backing remains reachable from pooled handle")
				}
				if capacity == builderByteArenaPoolMaxCap && cap(h.buf) != capacity {
					t.Fatalf("eligible arena capacity = %d, want %d", cap(h.buf), capacity)
				}
			}
		})
	}
}
