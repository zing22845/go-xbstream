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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Reader provides sequential access to chunks from an xbstream. Each chunk returned represents a
// contiguous set of bytes for a file stored in the xbstream archive. The Next method advances the stream
// and returns the next chunk in the archive. Each archive then acts as a reader for its contiguous set of bytes
type Reader struct {
	reader io.Reader
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

// NewReader creates a new Reader by wrapping the provided reader
func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: reader}
}

func (r *Reader) NextHeader(header *ChunkHeader) (err error) {
	header.HeaderSize = 0
	header.PrefixHeader = make([]uint8, ChunkHeaderFixSize)

	// Read Prefix bytes
	if _, err = r.Read(header.PrefixHeader); err != nil {
		// We should gracefully bubble up EOF if we attempt to read a new Chunk and hit EOF
		if err != io.EOF {
			return ErrReadHeaderFix
		}
		return err
	}
	header.Magic = header.PrefixHeader[:MagicLen]

	if !bytes.Equal(header.Magic, chunkMagic) {
		return fmt.Errorf("wrong chunk magic: %s", header.Magic)
	}

	// Chunk Flags
	header.Flags = ChunkFlag(header.PrefixHeader[MagicLen])

	// Chunk Type
	header.Type = ChunkType(header.PrefixHeader[MagicLen+1])
	if header.Type = validateChunkType(header.Type); header.Type == ChunkTypeUnknown {
		if !(header.Flags&FlagChunkIgnorable == 1) {
			return fmt.Errorf("unknown chunk type: '%c'", header.Type)
		}
	}

	// Path Length
	header.PathLen = binary.LittleEndian.Uint32(header.PrefixHeader[MagicLen+2:])
	header.HeaderSize += uint32(ChunkHeaderFixSize)

	// Path
	if header.PathLen > 0 {
		header.Path = make([]uint8, header.PathLen)
		if _, err = r.Read(header.Path); err != nil {
			return ErrReadPath
		}
	}
	header.HeaderSize += header.PathLen

	if header.Type == ChunkTypeEOF {
		return nil
	}
	header.PayFix = make([]uint8, ChunkPayFixSize)
	if _, err = r.Read(header.PayFix); err != nil {
		return ErrReadPayFix
	}
	header.PayLen = binary.LittleEndian.Uint64(header.PayFix)
	header.PayOffset = binary.LittleEndian.Uint64(header.PayFix[PayLenBytesLen:])
	header.Checksum = binary.LittleEndian.Uint32(header.PayFix[PayLenBytesLen+PayOffsetBytesLen:])
	header.HeaderSize += uint32(ChunkPayFixSize)
	return nil
}

// Next advances the Reader and returns the next Chunk.
// Note: end of input is represented by a specific Chunk type.
func (r *Reader) Next() (*Chunk, error) {
	var (
		chunk = new(Chunk)
		err   error
	)

	err = r.NextHeader(&chunk.ChunkHeader)
	if err != nil {
		return nil, err
	}

	if chunk.Type == ChunkTypeEOF {
		return chunk, nil
	}

	if chunk.PayLen > 0 {
		buffer := bytes.NewBuffer(nil)
		if _, err := io.CopyN(buffer, r.reader, int64(chunk.PayLen)); err != nil {
			return nil, ErrStreamRead
		}
		chunk.Reader = buffer
	} else {
		chunk.Reader = bytes.NewReader(nil)
	}

	return chunk, nil
}

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
