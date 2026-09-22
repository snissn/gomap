package typedcolumn

// SectionReader is the narrow byte-access seam that #1754 can adapt to #1736
// mappedresource-backed handles without changing the typed-column data-plane
// section directory or codecs.
type SectionReader interface {
	ReadSection(section ColumnPartImageSection) ([]byte, error)
}

// ImageSectionReader reads sections from an in-memory part image.
type ImageSectionReader struct {
	Image ColumnPartImage
}

func NewImageSectionReader(image ColumnPartImage) ImageSectionReader {
	return ImageSectionReader{Image: image}
}

func (r ImageSectionReader) ReadSection(section ColumnPartImageSection) ([]byte, error) {
	return r.Image.SectionBytes(section)
}

func (i ColumnPartImage) SectionBytes(section ColumnPartImageSection) ([]byte, error) {
	if err := validateImageSectionBounds(section, i.ManifestBytes, len(i.Bytes)); err != nil {
		return nil, err
	}
	return i.sectionBytes(section), nil
}
