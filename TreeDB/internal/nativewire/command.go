package nativewire

type CommandHeader struct {
	ID      CommandID
	Version uint64
	Flags   uint64
}

const (
	// CommandFlagOmitResultIDs asks mutation responses to omit result ID vectors
	// when the command can otherwise report success through response_meta.
	CommandFlagOmitResultIDs uint64 = 1 << iota
	// CommandFlagOmitResponseMeta asks success responses to omit response_meta
	// when the client only needs success/error signaling.
	CommandFlagOmitResponseMeta
)

const commandResponseShapingFlagsMask = CommandFlagOmitResultIDs | CommandFlagOmitResponseMeta

// Deterministic entries intentionally include no command flags in R0c. Response
// shaping flags are transport-only until a future replicated command version
// explicitly defines deterministic flag semantics.
const deterministicCommandFlagsMask uint64 = 0

func DeterministicCommandFlags(flags uint64) uint64 {
	return flags & deterministicCommandFlagsMask
}

func UnsupportedDeterministicCommandFlags(flags uint64) uint64 {
	return flags &^ (commandResponseShapingFlagsMask | deterministicCommandFlagsMask)
}

func AppendCommandHeader(dst []byte, h CommandHeader) []byte {
	dst = appendUvarint(dst, uint64(h.ID))
	dst = appendUvarint(dst, h.Version)
	dst = appendUvarint(dst, h.Flags)
	return dst
}

func DecodeCommandHeader(src []byte) (CommandHeader, error) {
	id, off, err := readUvarint(src)
	if err != nil {
		return CommandHeader{}, err
	}
	version, n, err := readUvarint(src[off:])
	if err != nil {
		return CommandHeader{}, err
	}
	off += n
	flags, n, err := readUvarint(src[off:])
	if err != nil {
		return CommandHeader{}, err
	}
	off += n
	if off != len(src) {
		return CommandHeader{}, protocolError(ErrInvalidCommand, "command_header has %d trailing bytes", len(src)-off)
	}
	return CommandHeader{ID: CommandID(id), Version: version, Flags: flags}, nil
}
