package index

// chunk flag
const (
	// FlagChunkIgnorable indicates a chunk as ignorable
	FlagChunkIgnorable ChunkFlag = 0x01
)

// ChunkFlag represents a chunks bit flag set
type ChunkFlag uint8

// chunk types
const (
	// ChunkTypePayload indicates chunk contains file payload
	ChunkTypePayload = ChunkType('P')
	// ChunkTypeEOF indicates chunk is the eof marker for a file
	ChunkTypeEOF = ChunkType('E')
	// ChunkTypeUnknown indicates the chunk was a type that was unknown to xbstream
	ChunkTypeUnknown = ChunkType(0)
)

// ChunkType designates a given chunks type
type ChunkType uint8 // Type of Chunk

// chunk header
const (
	MagicStr           = "XBSTCK01"
	MagicLen           = len(MagicStr)
	FlagLen            = 1
	TypeLen            = 1
	PathLenBytesLen    = 4
	ChunkHeaderFixSize = MagicLen + FlagLen + TypeLen + PathLenBytesLen
	PayLenBytesLen     = 8
	PayOffsetBytesLen  = 8
	ChecksumBytesLen   = 4
	ChunkPayFixSize    = PayLenBytesLen + PayOffsetBytesLen + ChecksumBytesLen
)

var (
	chunkMagic = []uint8(MagicStr)
)

func validateChunkType(p ChunkType) ChunkType {
	switch p {
	case ChunkTypePayload:
		fallthrough
	case ChunkTypeEOF:
		return p
	default:
		return ChunkTypeUnknown
	}
}

// ChunkHeader contains the metadata regarding the payload that immediately follows within the archive
type ChunkHeader struct {
	Magic          [MagicLen]uint8
	Flags          [FlagLen]uint8
	Type           [TypeLen]uint8 // The type of Chunk, Note xbstream archives end with a specific EOF type
	PathLenBytes   [PathLenBytesLen]uint8
	PathBytes      []uint8
	PayLenBytes    [PayLenBytesLen]uint8
	PayOffsetBytes [PayOffsetBytesLen]uint8
	ChecksumBytes  [ChecksumBytesLen]uint8
	PathLen        uint32 // The length of PathBytes
	PayLen         uint64 // The length of PayLenBytes
	HeaderSize     uint64 // The Size of the chuck's header without payload
	ChunkSize      uint64 // The Size of the entire chunk = HeaderSize + PayLen
}

func (c *ChunkHeader) ResetSize() {
	c.PathLen = 0
	c.PayLen = 0
	c.HeaderSize = 0
	c.ChunkSize = 0
}
