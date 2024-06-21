package index

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/zing22845/go-xbstream/xbstream"
)

func ExtractSchemaByPayload(
	schemaMap *TableSchemaMap,
	schemaChan chan *TableSchema,
	ci *ChunkIndex,
	r io.Reader,
	payLen int64,
) (n int64, err error) {
	var tableSchema *TableSchema
	defer func() {
		if tableSchema == nil {
			n, err = io.CopyN(io.Discard, r, payLen)
		} else {
			n, err = io.CopyN(tableSchema.StreamIn, r, payLen)
		}
		log.Infof("extract schema for %s end", ci.Filepath)
	}()
	log.Infof("extracting schema for %s, payOffset: %d", ci.Filepath, ci.PayOffset)
	if ci.PayOffset == 0 {
		tableSchema, err = NewTableSchema(
			ci.Filepath,
			ci.DecompressedFileType,
			ci.DecompressMethod,
		)
		if err != nil {
			return 0, err
		}
		schemaMap.Set(ci.Filepath, tableSchema)
		schemaChan <- tableSchema
	} else {
		var ok bool
		tableSchema, ok = schemaMap.Get(ci.Filepath)
		if !ok {
			return 0, fmt.Errorf("table schema not found for %s", ci.Filepath)
		}
	}
	return n, err
}

func DecodeChunkHeader(xr *xbstream.Reader) (header *xbstream.ChunkHeader, err error) {
	// read header
	header = &xbstream.ChunkHeader{}
	err = xr.NextHeader(header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func ExtractSingleSchema(
	ci *ChunkIndex,
	schemaChan chan *TableSchema,
	r io.ReadSeeker,
) (err error) {
	/*
		timer := utils.NewSimpleTimer()
		timer.Start()
	*/
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
	schemaMap := &TableSchemaMap{
		tables: make(map[string]*TableSchema),
	}
	if ci.PayOffset != header.PayOffset {
		return fmt.Errorf("chunk pay offset not equal to chunk index pay offset")
	}
	payLen := int64(header.PayLen)
	_, err = ExtractSchemaByPayload(
		schemaMap,
		schemaChan,
		ci,
		xr,
		payLen)
	if err != nil {
		return err
	}
	if schema, ok := schemaMap.Get(ci.Filepath); ok {
		_ = schema.StreamIn.Close()
		schemaMap.Delete(ci.Filepath)
	}
	return nil
}
