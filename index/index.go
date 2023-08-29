package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
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
)

const (
	// ChunkTypePayload indicates chunk contains file payload
	ChunkTypePayload = ChunkType('P')
	// ChunkTypeEOF indicates chunk is the eof marker for a file
	ChunkTypeEOF = ChunkType('E')
	// ChunkTypeUnknown indicates the chunk was a type that was unknown to xbstream
	ChunkTypeUnknown = ChunkType(0)
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

type StreamIndex struct {
	Filepath      string `gorm:"column:filepath;type:varchar(4096);index:idx_filepath_start_position"`
	StartPosition int64  `gorm:"column:start_position;type:bigint;index:idx_filepath_start_position"`
	EndPosition   int64  `gorm:"column:end_position;type:bigint"`
}

type IndexStream struct {
	Reader           io.Reader
	Writer           io.Writer
	ChunkFilepath    string
	ChunkStartOffset int64
	Offset           int64
	ChunkEndOffset   int64
	IndexFilePath    string
	Err              error
}

func NewIndexXbstream(reader io.Reader, writer io.Writer, indexFilePath string) *IndexStream {
	return &IndexStream{
		Reader:        reader,
		Writer:        writer,
		IndexFilePath: indexFilePath,
	}
}

func (i *IndexStream) WritePath(pathLen int64) {
	pathBytes := bytes.NewBuffer(nil)
	_, err := io.CopyN(pathBytes, i.Reader, int64(pathLen))
	if err != nil {
		i.Err = fmt.Errorf("error reading path: %w", err)
		return
	}
	i.Write(pathBytes.Bytes())
	if i.Err != nil {
		return
	}
	i.ChunkFilepath = pathBytes.String()
}

func (i *IndexStream) Write(content []byte) {
	n, err := i.Writer.Write(content)
	if err != nil {
		i.Err = err
		return
	}
	i.Offset += int64(n)
}

func (i *IndexStream) CopyN(w io.Writer, r io.Reader, n int64) {
	n, err := io.CopyN(w, r, n)
	if err != nil {
		i.Err = err
		return
	}
	i.Offset += int64(n)
}

func (i *IndexStream) IndexStream() {
	_, err := os.Stat(i.IndexFilePath)
	if !os.IsNotExist(err) {
		i.Err = fmt.Errorf("index file already exists")
		return
	}
	db, err := gorm.Open(sqlite.Open(i.IndexFilePath), &gorm.Config{})
	if err != nil {
		i.Err = err
		return
	}
	db.Exec("PRAGMA synchronous = OFF")
	i.Err = db.AutoMigrate(&StreamIndex{})
	if i.Err != nil {
		return
	}

	chunkMagic := []uint8("XBSTCK01")

	magicBytes := make([]byte, len(chunkMagic))
	flagsBytes := make([]byte, 1)
	typeBytes := make([]byte, 1)
	pathLenBytes := make([]byte, 4)
	payLenBytes := make([]byte, 8)
	payOffsetBytes := make([]byte, 8)
	checksumBytes := make([]byte, 4)

	for {
		// Chunk Magic
		i.ChunkStartOffset = i.Offset
		_, err = io.ReadFull(i.Reader, magicBytes)
		if err != nil {
			if err != io.EOF {
				i.Err = err
			}
			return
		}
		if !bytes.Equal(magicBytes, chunkMagic) {
			i.Err = fmt.Errorf("wrong chunk magic")
			return
		}
		i.Write(magicBytes)
		if i.Err != nil {
			return
		}

		// Chunk Flags
		_, err = io.ReadFull(i.Reader, flagsBytes)
		if err != nil {
			i.Err = fmt.Errorf("error reading chunk flags: %w", err)
			return
		}
		i.Write(flagsBytes)
		if i.Err != nil {
			return
		}

		// Chunk Type
		_, err = io.ReadFull(i.Reader, typeBytes)
		if err != nil {
			i.Err = fmt.Errorf(`error reading chunk type: %w`, err)
			return
		}
		if (validateChunkType(ChunkType(typeBytes[0])) == ChunkTypeUnknown) &&
			!(flagsBytes[0]&byte(FlagChunkIgnorable) == 1) {
			i.Err = fmt.Errorf("unknown chunk type")
			return
		}
		i.Write(typeBytes)
		if i.Err != nil {
			return
		}

		// Path Length
		_, err = io.ReadFull(i.Reader, pathLenBytes)
		if err != nil {
			i.Err = fmt.Errorf(`error reading path length: %w`, err)
			return
		}
		pathLen := binary.LittleEndian.Uint32(pathLenBytes)
		i.Write(pathLenBytes)
		if i.Err != nil {
			return
		}

		// Path
		if pathLen > 0 {
			i.WritePath(int64(pathLen))
			if i.Err != nil {
				return
			}
		}
		if typeBytes[0] == byte(ChunkTypeEOF) {
			continue
		}

		// pay length
		_, err = io.ReadFull(i.Reader, payLenBytes)
		if err != nil {
			i.Err = fmt.Errorf(`error reading paylen: %w`, err)
			return
		}
		payLen := binary.LittleEndian.Uint64(payLenBytes)
		i.Write(payLenBytes)
		if i.Err != nil {
			return
		}

		// pay offset
		_, err = io.ReadFull(i.Reader, payOffsetBytes)
		if err != nil {
			i.Err = fmt.Errorf(`error reading payoffset: %w`, err)
			return
		}
		i.Write(payOffsetBytes)
		if i.Err != nil {
			return
		}

		// checksum
		_, err = io.ReadFull(i.Reader, checksumBytes)
		if err != nil {
			i.Err = fmt.Errorf(`error reading checksum: %w`, err)
			return
		}
		i.Write(checksumBytes)
		if i.Err != nil {
			return
		}

		// pay load
		if payLen > 0 {
			i.CopyN(i.Writer, i.Reader, int64(payLen))
			if i.Err != nil {
				return
			}
		}
		i.ChunkEndOffset = i.Offset

		// update db
		idx := &StreamIndex{
			Filepath:      i.ChunkFilepath,
			StartPosition: i.ChunkStartOffset,
			EndPosition:   i.ChunkEndOffset,
		}
		// go func(idx *StreamIndex) {
		result := db.Create(idx)
		if result.Error != nil {
			i.Err = result.Error
			return
		}
		// }(idx)
	}
}

func main() {
	var (
		file          *os.File
		indexFilePath string
		err           error
	)

	indexFilePath = "index.db"
	if len(os.Args) < 2 {
		file = os.Stdin
	} else {
		filepath := os.Args[1]
		file, err = os.Open(filepath)
		if err != nil {
			fmt.Printf("failed to open file %q: %v\n", filepath, err)
			os.Exit(1)
		}
	}

	indexStream := NewIndexXbstream(file, io.Discard, indexFilePath)
	indexStream.IndexStream()
	if indexStream.Err != nil {
		fmt.Printf("index stream error: %v\n", indexStream.Err)
		os.Exit(1)
	}
}
