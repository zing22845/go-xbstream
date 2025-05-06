/*
 * Copyright (C) 2017 Sean McGrail
 * Copyright (C) 2011-2017 Percona LLC and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

package xbstream

import (
	"errors"
	"io"
	"unsafe"
)

// ChunkFlag represents a chunks bit flag set
type ChunkFlag uint8

// ChunkType designates a given chunks type
type ChunkType uint8 // Type of Chunk

const (
	// MinimumChunkSize represents the smallest chunk size that xbstream will attempt to fill before flushing to the stream
	MinimumChunkSize = 10 * 1024 * 1024
	// MaxPathLength is the largest file path that can exist within an xbstream archive
	MaxPathLength = 512
	// FlagChunkIgnorable indicates a chunk as ignorable
	FlagChunkIgnorable ChunkFlag = 0x01
	MagicStr                     = "XBSTCK01"
	MagicLen                     = len(MagicStr)
	FlagLen                      = unsafe.Sizeof(ChunkFlag(0))
	TypeLen                      = unsafe.Sizeof(ChunkType(0))
	PathLenBytesLen              = unsafe.Sizeof(ChunkHeader{}.PathLen)
	ChunkHeaderFixSize           = MagicLen + int(FlagLen+TypeLen+PathLenBytesLen)
	PayLenBytesLen               = unsafe.Sizeof(ChunkHeader{}.PayLen)
	PayOffsetBytesLen            = unsafe.Sizeof(ChunkHeader{}.PayOffset)
	ChecksumBytesLen             = unsafe.Sizeof(ChunkHeader{}.Checksum)
	ChunkPayFixSize              = int(PayLenBytesLen + PayOffsetBytesLen + ChecksumBytesLen)
)
const FieldSize = unsafe.Sizeof(ChunkHeader{}.PayOffset)

const (
	// ChunkTypePayload indicates chunk contains file payload
	ChunkTypePayload = ChunkType('P')
	// ChunkTypeEOF indicates chunk is the eof marker for a file
	ChunkTypeEOF = ChunkType('E')
	// ChunkTypeUnknown indicates the chunk was a type that was unknown to xbstream
	ChunkTypeUnknown = ChunkType(0)
)

var (
	ChunkMagic       = []uint8(MagicStr)
	ErrReadHeaderFix = errors.New("xbstream read header fix error")
	ErrStreamRead    = errors.New("xbstream read error")
	ErrReadPath      = errors.New("xbstream read path error")
	ErrReadPayFix    = errors.New("xbstream read pay fix error")
)

// Chunk encapsulates a ChunkHeader and provides a io.Reader interface for reading the payload described by the Header
type Chunk struct {
	ChunkHeader
	io.Reader
}

// ChunkHeader contains the metadata regarding the payload that immediately follows within the archive
type ChunkHeader struct {
	PrefixHeader []uint8
	Magic        []uint8
	Flags        ChunkFlag
	Type         ChunkType // The type of Chunk, Note xbstream archives end with a specific EOF type
	PathLen      uint32
	Path         []uint8
	PayFix       []uint8
	PayLen       uint64
	PayOffset    uint64
	Checksum     uint32
	HeaderSize   uint32
	ReadSize     int64
}
