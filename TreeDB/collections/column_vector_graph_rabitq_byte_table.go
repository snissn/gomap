package collections

import "github.com/snissn/gomap/TreeDB/internal/rabitq"

func rabitqByteTableMismatchWeightPortable(querySignBits []byte, code []byte, byteMismatchWeights []float64) float64 {
	const tableEntries = rabitq.ByteMismatchTableEntries
	n := len(code)
	if n == 0 {
		return 0
	}
	// The checked scorer validates these lengths before reaching the hot loop.
	// Keep the preloads here so portable Go builds have the same fail-fast
	// behavior while giving the compiler enough shape information to remove
	// per-iteration bounds checks in the unrolled loop below.
	_ = querySignBits[n-1]
	_ = byteMismatchWeights[n*tableEntries-1]

	var sum0, sum1, sum2, sum3 float64
	byteIdx := 0
	tableBase := 0
	for ; byteIdx+4 <= n; byteIdx += 4 {
		candidate0 := code[byteIdx]
		candidate1 := code[byteIdx+1]
		candidate2 := code[byteIdx+2]
		candidate3 := code[byteIdx+3]
		query0 := querySignBits[byteIdx]
		query1 := querySignBits[byteIdx+1]
		query2 := querySignBits[byteIdx+2]
		query3 := querySignBits[byteIdx+3]

		sum0 += byteMismatchWeights[tableBase+int(candidate0^query0)]
		sum1 += byteMismatchWeights[tableBase+tableEntries+int(candidate1^query1)]
		sum2 += byteMismatchWeights[tableBase+2*tableEntries+int(candidate2^query2)]
		sum3 += byteMismatchWeights[tableBase+3*tableEntries+int(candidate3^query3)]
		tableBase += 4 * tableEntries
	}
	for ; byteIdx < n; byteIdx++ {
		xorMask := code[byteIdx] ^ querySignBits[byteIdx]
		sum0 += byteMismatchWeights[tableBase+int(xorMask)]
		tableBase += tableEntries
	}
	return (sum0 + sum1) + (sum2 + sum3)
}
