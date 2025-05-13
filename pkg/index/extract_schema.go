package index

import (
	"fmt"
	"io"

	"github.com/zing22845/go-xbstream/pkg/xbstream"
)

func ExtractSchemaByPayload(
	schemaChan chan *TableSchema,
	ci *ChunkIndex,
	r io.Reader,
	payLen int64,
) (n int64, err error) {
	var tableSchema *TableSchema
	if ci.PayOffset == 0 {
		tableSchema, err = NewTableSchema(
			ci.Filepath,
			ci.ExtractLimitSize,
			ci.EncryptKey,
			ci.DecryptedFileType,
			ci.DecryptMethod,
			ci.DecompressedFileType,
			ci.DecompressMethod,
		)
		if err != nil {
			n, _ = io.CopyN(io.Discard, r, payLen)
			return n, err
		}
		defer tableSchema.StreamIn.Close()
		schemaChan <- tableSchema
		return io.CopyN(tableSchema.StreamIn, r, payLen)
	}
	return io.CopyN(io.Discard, r, payLen)
}

func ExtractSingleSchema(
	ci *ChunkIndex,
	schemaChan chan *TableSchema,
	r io.ReadSeeker,
) (err error) {
	// seek to chunk start position
	_, err = r.Seek(ci.StartPosition, io.SeekStart)
	if err != nil {
		return err
	}
	xr := xbstream.NewReader(r)
	// decode chunk header
	header, err := DecodeChunkHeader(xr)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if ci.PayOffset != header.PayOffset {
		return fmt.Errorf("chunk pay offset not equal to chunk index pay offset")
	}
	payLen := int64(header.PayLen)
	_, err = ExtractSchemaByPayload(
		schemaChan,
		ci,
		xr,
		payLen)
	if err != nil {
		return err
	}
	return nil
}
