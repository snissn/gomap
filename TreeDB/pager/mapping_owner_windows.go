//go:build windows

package pager

func nativePartialMappingSupported() bool { return false }
func mapConstructor(fd uintptr, offset int64, length int, populate, readOnly bool) (mappingOwner, error) {
	var data []byte
	var e error
	if readOnly {
		data, e = mmapFileReadOnly(fd, offset, length, populate)
	} else {
		data, e = mmapFile(fd, offset, length, populate)
	}
	return mappingOwner{data: data}, e
}
func unmapConstructor(m *mappingOwner, amount uintptr) error { return ErrMappingAuthority }
