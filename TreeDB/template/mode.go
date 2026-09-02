package template

// Mode selects template compression behavior.
type Mode uint8

const (
	TemplateOff Mode = iota
	TemplateOnly
	TemplatePrepass
)
