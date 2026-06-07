//go:build arm64 && !purego

package collections

func rabitqByteTableMismatchWeight(querySignBits []byte, code []byte, byteMismatchWeights []float64) float64 {
	if len(code) == 0 {
		return 0
	}
	return rabitqByteTableMismatchWeightARM64(querySignBits, code, byteMismatchWeights, len(code))
}

// rabitqByteTableMismatchWeightARM64 sums byteMismatchWeights[i*256 + (code[i]^querySignBits[i])].
// The Go caller must validate that querySignBits and code have equal length and
// that byteMismatchWeights has at least len(code)*256 entries.
//
//go:noescape
func rabitqByteTableMismatchWeightARM64(querySignBits []byte, code []byte, byteMismatchWeights []float64, n int) float64
