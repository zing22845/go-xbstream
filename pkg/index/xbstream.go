package index

import (
	"github.com/zing22845/go-xbstream/pkg/xbstream"
)

func DecodeChunkHeader(xr *xbstream.Reader) (header *xbstream.ChunkHeader, err error) {
	// read header
	header = &xbstream.ChunkHeader{}
	err = xr.NextHeader(header)
	if err != nil {
		return nil, err
	}
	return header, nil
}
