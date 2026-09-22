//go:build purego || !arm64

package collections

func rabitqByteTableMismatchWeight(querySignBits []byte, code []byte, byteMismatchWeights []float64) float64 {
	return rabitqByteTableMismatchWeightPortable(querySignBits, code, byteMismatchWeights)
}
