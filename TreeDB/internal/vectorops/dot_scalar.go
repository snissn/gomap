package vectorops

// DotFloat32Scalar returns the float32 dot product over the shared prefix of
// left and right. It accepts already validated float32 slices and performs no
// byte reinterpretation or allocation.
func DotFloat32Scalar(left, right []float32) float32 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	var sum float32
	for i := 0; i < n; i++ {
		sum += left[i] * right[i]
	}
	return sum
}
