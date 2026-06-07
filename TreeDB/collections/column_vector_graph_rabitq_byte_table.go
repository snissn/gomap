package collections

import "github.com/snissn/gomap/TreeDB/internal/rabitq"

func rabitqByteTableMismatchWeightPortable(querySignBits []byte, code []byte, byteMismatchWeights []float64) float64 {
	var mismatchWeight float64
	for byteIdx, candidateByte := range code {
		xorMask := candidateByte ^ querySignBits[byteIdx]
		mismatchWeight += byteMismatchWeights[byteIdx*rabitq.ByteMismatchTableEntries+int(xorMask)]
	}
	return mismatchWeight
}
