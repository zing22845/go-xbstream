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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xbstream archive created by the standard xbstream binary
// archive contains two files, file1 and file2, that are each 5 bytes in length
var xbFile = []byte{
	0x58, 0x42, 0x53, 0x54, 0x43, 0x4b, 0x30, 0x31, 0x00, 0x50, 0x05, 0x00,
	0x00, 0x00, 0x66, 0x69, 0x6c, 0x65, 0x31, 0x05, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5d,
	0xfe, 0x31, 0x4b, 0x87, 0x19, 0x8b, 0xe0, 0x9a, 0x58, 0x42, 0x53, 0x54,
	0x43, 0x4b, 0x30, 0x31, 0x00, 0x45, 0x05, 0x00, 0x00, 0x00, 0x66, 0x69,
	0x6c, 0x65, 0x31, 0x58, 0x42, 0x53, 0x54, 0x43, 0x4b, 0x30, 0x31, 0x00,
	0x50, 0x05, 0x00, 0x00, 0x00, 0x66, 0x69, 0x6c, 0x65, 0x32, 0x05, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x89, 0x58, 0x8b, 0x97, 0x35, 0xbf, 0x06, 0x38, 0x97, 0x58,
	0x42, 0x53, 0x54, 0x43, 0x4b, 0x30, 0x31, 0x00, 0x45, 0x05, 0x00, 0x00,
	0x00, 0x66, 0x69, 0x6c, 0x65, 0x32,
}

func TestReader(t *testing.T) {
	xb := bytes.NewReader(xbFile)
	reader := NewReader(xb)

	// Test file1
	chunk, err := reader.Next()
	require.NoError(t, err)
	assert.Equal(t, ChunkTypePayload, chunk.Type)
	assert.Equal(t, []byte("file1"), chunk.Path)
	assert.Equal(t, uint64(5), chunk.PayLen)
	assert.Equal(t, uint64(0), chunk.PayOffset)

	payload := make([]byte, 5)
	n, err := io.ReadFull(chunk, payload)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte{0x87, 0x19, 0x8b, 0xe0, 0x9a}, payload)

	// Test file1 EOF
	chunk, err = reader.Next()
	require.NoError(t, err)
	assert.Equal(t, ChunkTypeEOF, chunk.Type)
	assert.Equal(t, []byte("file1"), chunk.Path)

	// Test file2
	chunk, err = reader.Next()
	require.NoError(t, err)
	assert.Equal(t, ChunkTypePayload, chunk.Type)
	assert.Equal(t, []byte("file2"), chunk.Path)
	assert.Equal(t, uint64(5), chunk.PayLen)
	assert.Equal(t, uint64(0), chunk.PayOffset)

	n, err = io.ReadFull(chunk, payload)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte{0x35, 0xbf, 0x06, 0x38, 0x97}, payload)

	// Test file2 EOF
	chunk, err = reader.Next()
	require.NoError(t, err)
	assert.Equal(t, ChunkTypeEOF, chunk.Type)
	assert.Equal(t, []byte("file2"), chunk.Path)

	// Test end of archive
	chunk, err = reader.Next()
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, uint64(0), chunk.PayLen)
}
