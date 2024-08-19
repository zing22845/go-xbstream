package xbcrypt

import (
	"bytes"
	"errors"
	"math"
	"unsafe"
)

const (
	XBCryptHashLen         = 32
	AESBlockSize           = 16
	MaxChunkSize           = math.MaxInt32
	MagicLen               = 8
	ReservedBytesSize      = 8
	OriginalSizeBytesSize  = unsafe.Sizeof(ChunkHeader{}.OriginSize)
	EncryptedSizeBytesSize = unsafe.Sizeof(ChunkHeader{}.EncryptedSize)
	ChecksumBytesSize      = 4
	IVSizeBytesSize        = 8
	ChunkHeaderFixSize     = MagicLen + ReservedBytesSize + OriginalSizeBytesSize + EncryptedSizeBytesSize + ChecksumBytesSize
	MagicStr1              = "XBCRYP01"
	MagicStr2              = "XBCRYP02"
	MagicStr3              = "XBCRYP03"
)

// magics
var (
	ChunkMagic1 = []byte(MagicStr1)
	ChunkMagic2 = []byte(MagicStr2)
	ChunkMagic3 = []byte(MagicStr3)
)

// errors
var (
	ErrReadMagic         = errors.New("read magic failed")
	ErrReadHeaderFix     = errors.New("read header fix failed")
	ErrReadReserved      = errors.New("read reserved failed")
	ErrReadOriginalSize  = errors.New("read original size failed")
	ErrReadEncryptedSize = errors.New("read encrypted size failed")
	ErrReadChecksum      = errors.New("read checksum failed")
	ErrReadIVSize        = errors.New("read iv size failed")
	ErrReadIV            = errors.New("read iv failed")
	ErrReadEncryptedData = errors.New("read encrypted data failed")
	ErrReadHash          = errors.New("read hash failed")
	ErrHashNotMatch      = errors.New("hash not match")
	ErrDecrypt           = errors.New("decrypt failed")
	ErrExceedExtractSize = errors.New("exceed extract size")
)

type ChunkHeader struct {
	PrefixHeader  []uint8
	OriginSize    uint64
	EncryptedSize uint64
	Checksum      uint32
	IVSize        uint64
	IV            []byte
	Version       uint8
	HashAppended  bool
	HeaderSize    uint32
}

type ChunkEncrypted struct {
	PayloadSize uint64
	Hash        *bytes.Buffer
}

type Chunk struct {
	ChunkHeader
	ChunkEncrypted
}
