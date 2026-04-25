package caching

import (
	"math"
	"math/big"
	"testing"
)

func TestLeafGenerationPackReclaimPerByteCopiedPPM_OverflowSafe(t *testing.T) {
	reclaimed := int64(math.MaxInt64 / 2)
	copied := int64(math.MaxInt64 - 1)
	left := new(big.Int).Mul(big.NewInt(reclaimed), big.NewInt(1000000))
	want := new(big.Int).Div(left, big.NewInt(copied)).Int64()
	if got := leafGenerationPackReclaimPerByteCopiedPPM(reclaimed, copied); got != want {
		t.Fatalf("leafGenerationPackReclaimPerByteCopiedPPM(%d,%d)=%d, want %d", reclaimed, copied, got, want)
	}
}
