package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"github.com/skmcgrail/go-xbstream/xbstream"
	"gorm.io/gorm"
)

// ChunkFlag represents a chunks bit flag set
type ChunkFlag uint8

// ChunkType designates a given chunks type
type ChunkType uint8 // Type of Chunk

// ChunkHeader contains the metadata regarding the payload that immediately follows within the archive
type ChunkHeader struct {
	Magic          [MagicLen]uint8
	Flags          [1]uint8
	Type           [1]uint8 // The type of Chunk, Note xbstream archives end with a specific EOF type
	PathLenBytes   [4]uint8
	PathBytes      []uint8
	PayLenBytes    [8]uint8
	PayOffsetBytes [8]uint8
	ChecksumBytes  [4]uint8
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

const (
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
	MagicStr         = "XBSTCK01"
	MagicLen         = len(MagicStr)
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

type ChunkIndex struct {
	gorm.Model
	Filepath      string `gorm:"column:filepath;type:varchar(4096);index:idx_filepath_start_position"`
	StartPosition int64  `gorm:"column:start_position;type:bigint;index:idx_filepath_start_position"`
	EndPosition   int64  `gorm:"column:end_position;type:bigint"`
}

type IndexStream struct {
	CTX                               context.Context
	Cancel                            context.CancelFunc
	Reader                            io.Reader
	Writer                            io.Writer
	Offset                            int64
	CurrentChunkIndex                 *ChunkIndex
	CurrentChunkHeader                *ChunkHeader
	ChunkIndexChan                    chan *ChunkIndex
	IndexFilePath                     string
	IndexFilename                     string
	IndexFileSize                     int64
	IndexFileOffsetStart              int64 // [IndexFileOffsetStart
	IndexFileOffsetEnd                int64 // IndexFileOffsetEnd]
	IndexFileOffsetFilename           string
	IndexFileOffsetFileChunkTotalSize int64
	IndexDB                           *gorm.DB
	IsIndexDone                       bool
	WriteIndexDBDone                  chan struct{}
	WriteIndexDBBatchSize             int
	Err                               error
}

func NewIndexXbstream(
	ctx context.Context,
	reader io.Reader,
	writer io.Writer,
	indexFilePath string,
) *IndexStream {
	i := &IndexStream{
		Reader:                reader,
		Writer:                writer,
		IndexFilePath:         indexFilePath,
		CurrentChunkIndex:     &ChunkIndex{},
		CurrentChunkHeader:    &ChunkHeader{},
		ChunkIndexChan:        make(chan *ChunkIndex, 100),
		WriteIndexDBDone:      make(chan struct{}, 1),
		WriteIndexDBBatchSize: 100,
	}
	i.CTX, i.Cancel = context.WithCancel(ctx)
	return i
}

func (i *IndexStream) WriteIndexDB() {
	batchIndex := make([]*ChunkIndex, 0, i.WriteIndexDBBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchIndex) > 0 {
			i.insertBatch(batchIndex)
		}
		i.WriteIndexDBDone <- struct{}{}
	}()
	for chunkIndex := range i.ChunkIndexChan {
		if chunkIndex.Filepath == "" {
			continue
		}
		batchIndex = append(batchIndex, chunkIndex)
		if len(batchIndex) == i.WriteIndexDBBatchSize {
			i.insertBatch(batchIndex)
			if i.Err != nil {
				return
			}
			// Clear the batch without re-allocating memory.
			batchIndex = batchIndex[:0]
		}
	}
}

func (i *IndexStream) insertBatch(batchIndex []*ChunkIndex) {
	result := i.IndexDB.Create(batchIndex)
	if result.Error != nil {
		i.Err = result.Error
	}
}

func (i *IndexStream) WriteHeader(content []byte) {
	n, err := i.Writer.Write(content)
	if err != nil {
		i.Err = err
		return
	}
	i.CurrentChunkHeader.HeaderSize += uint64(n)
	i.CurrentChunkHeader.ChunkSize = i.CurrentChunkHeader.HeaderSize
	i.Offset += int64(n)
}

func (i *IndexStream) StreamMagic() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.Magic[:])
	if err != nil {
		if err == io.EOF {
			// stream done
			i.IsIndexDone = true
			return
		}
		i.Err = err
		return
	}
	if !bytes.Equal(i.CurrentChunkHeader.Magic[:], chunkMagic) {
		i.Err = fmt.Errorf("wrong chunk magic: %s", i.CurrentChunkHeader.Magic)
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.Magic[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamFlags() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.Flags[:])
	if err != nil {
		i.Err = fmt.Errorf("error reading chunk flags: %w", err)
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.Flags[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamType() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.Type[:])
	if err != nil {
		i.Err = fmt.Errorf(`error reading chunk type: %w`, err)
		return
	}
	if (validateChunkType(ChunkType(i.CurrentChunkHeader.Type[0])) == ChunkTypeUnknown) &&
		!(i.CurrentChunkHeader.Flags[0]&byte(FlagChunkIgnorable) == 1) {
		i.Err = fmt.Errorf("unknown chunk type")
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.Type[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamPathLen() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.PathLenBytes[:])
	if err != nil {
		i.Err = fmt.Errorf(`error reading path length: %w`, err)
		return
	}
	i.CurrentChunkHeader.PathLen = binary.LittleEndian.Uint32(
		i.CurrentChunkHeader.PathLenBytes[:],
	)
	i.WriteHeader(i.CurrentChunkHeader.PathLenBytes[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamPath() {
	if i.CurrentChunkHeader.PathLen <= 0 {
		return
	}
	i.CurrentChunkHeader.PathBytes = make([]byte, i.CurrentChunkHeader.PathLen)
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.PathBytes)
	if err != nil {
		i.Err = fmt.Errorf("error reading path: %w", err)
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.PathBytes)
	if i.Err != nil {
		return
	}
	if string(i.CurrentChunkHeader.PathBytes) == i.CurrentChunkIndex.Filepath {
		i.CurrentChunkIndex.EndPosition = i.Offset
	} else {
		i.ChunkIndexChan <- i.CurrentChunkIndex
		i.CurrentChunkIndex = &ChunkIndex{
			Filepath:      string(i.CurrentChunkHeader.PathBytes),
			StartPosition: i.CurrentChunkIndex.EndPosition,
			EndPosition:   i.Offset,
		}
	}
}

func (i *IndexStream) StreamPayLen() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.PayLenBytes[:])
	if err != nil {
		i.Err = fmt.Errorf("error reading paylen: %w", err)
		return
	}
	i.CurrentChunkHeader.PayLen = binary.LittleEndian.Uint64(
		i.CurrentChunkHeader.PayLenBytes[:],
	)
	i.WriteHeader(i.CurrentChunkHeader.PayLenBytes[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamPayOffset() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.PayOffsetBytes[:])
	if err != nil {
		i.Err = fmt.Errorf(`error reading payoffset: %w`, err)
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.PayOffsetBytes[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) StreamChecksum() {
	_, err := io.ReadFull(i.Reader, i.CurrentChunkHeader.ChecksumBytes[:])
	if err != nil {
		i.Err = fmt.Errorf(`error reading checksum: %w`, err)
		return
	}
	i.WriteHeader(i.CurrentChunkHeader.ChecksumBytes[:])
	if i.Err != nil {
		return
	}
}

func (i *IndexStream) CopyPayload() {
	if i.CurrentChunkHeader.PayLen <= 0 {
		return
	}
	m, err := io.CopyN(i.Writer, i.Reader, int64(i.CurrentChunkHeader.PayLen))
	if err != nil {
		i.Err = err
		return
	}
	i.CurrentChunkHeader.ChunkSize += uint64(m)
	i.Offset += m
	i.CurrentChunkIndex.EndPosition = i.Offset
}

func (i *IndexStream) StreamChunk() {
	i.CurrentChunkHeader.ResetSize()
	// Magic
	i.StreamMagic()
	if i.Err != nil || i.IsIndexDone {
		return
	}
	// Flags
	i.StreamFlags()
	if i.Err != nil {
		return
	}
	// Type
	i.StreamType()
	if i.Err != nil {
		return
	}
	// Path Length
	i.StreamPathLen()
	if i.Err != nil {
		return
	}
	// Path
	i.StreamPath()
	if i.Err != nil {
		return
	}
	// type EOF (empty pay)
	if i.CurrentChunkHeader.Type[0] == byte(ChunkTypeEOF) {
		return
	}
	// pay length
	i.StreamPayLen()
	if i.Err != nil {
		return
	}
	// pay offset
	i.StreamPayOffset()
	if i.Err != nil {
		return
	}
	// checksum
	i.StreamChecksum()
	if i.Err != nil {
		return
	}
	// pay load
	i.CopyPayload()
	if i.Err != nil {
		return
	}
}

// StreamIndexFile 方法用于将索引文件和索引文件的offset写入到xbstream中
func (i *IndexStream) StreamIndexFile(indexWriter io.WriteCloser) {
	// init range vars
	i.IndexFileOffsetStart = i.CurrentChunkIndex.EndPosition
	i.IndexFileOffsetEnd = i.IndexFileOffsetStart
	// init common fix header size
	chunkHeaderFixSize := len(i.CurrentChunkHeader.Magic) +
		len(i.CurrentChunkHeader.Flags) +
		len(i.CurrentChunkHeader.Type) +
		len(i.CurrentChunkHeader.PathLenBytes)
	chunkPayFixSize := len(i.CurrentChunkHeader.PayLenBytes) +
		len(i.CurrentChunkHeader.PayOffsetBytes) +
		len(i.CurrentChunkHeader.ChecksumBytes)
	w := xbstream.NewWriter(indexWriter)
	b := make([]byte, xbstream.MinimumChunkSize)
	// get index file size
	fi, err := os.Stat(i.IndexFilePath)
	if err != nil {
		i.Err = fmt.Errorf("error stat index file: %w", err)
		return
	}
	i.IndexFileSize = fi.Size()
	// write index file to stream
	file, err := os.Open(i.IndexFilePath)
	if err != nil {
		i.Err = fmt.Errorf("open index file error: %w", err)
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			return
		}
		// _ = os.Remove(i.IndexFilePath)
	}()
	i.IndexFilename = filepath.Base(i.IndexFilePath)
	fw, err := w.Create(i.IndexFilename)
	if err != nil {
		i.Err = fmt.Errorf("create index chunks writer error: %w", err)
		return
	}
	for {
		n, err := file.Read(b)
		if err != nil {
			if err != io.EOF {
				i.Err = fmt.Errorf("read index file error: %w", err)
			}
			break
		}
		n, err = fw.Write(b[:n])
		if err != nil {
			i.Err = fmt.Errorf("write index to xbstream error: %w", err)
			break
		}
		i.IndexFileOffsetEnd += int64(
			chunkHeaderFixSize + // header common size(magic + flag + type + pathLen)
				len(
					[]byte(i.IndexFilename),
				) + // path
				chunkPayFixSize + // pay common size(paylen + payoffset + checksum)
				n, // paysize
		)
	}
	err = fw.Close()
	if err != nil {
		i.Err = fmt.Errorf("close index stream writer error: %w", err)
		return
	}
	i.IndexFileOffsetEnd += int64(
		chunkHeaderFixSize+len(
			[]byte(i.IndexFilename),
		), // EOF chunk without pay
	) - 1 // range: left-open and right-closed interval

	// write offset of index file as last file's content
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"
	fw, err = w.Create(i.IndexFileOffsetFilename)
	if err != nil {
		i.Err = fmt.Errorf("create index chunks offset writer error: %w", err)
		return
	}
	offsetBytesLen := 8 // the storage size of i.IndexFileOffset(int64) is always 8 bytes
	offsetBytes := make([]byte, offsetBytesLen)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(i.IndexFileOffsetStart))
	_, err = fw.Write(offsetBytes)
	if err != nil {
		i.Err = fmt.Errorf("write index offset to xbstream error: %w", err)
		return
	}
	err = fw.Close()
	if err != nil {
		i.Err = fmt.Errorf("close index offset stream writer error: %w", err)
		return
	}
	i.IndexFileOffsetFileChunkTotalSize = int64( // always 2 chunk
		(chunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 +
			(chunkPayFixSize + offsetBytesLen), // last chunk without pay
	)
}

func (i *IndexStream) IndexStream() {
	defer func() {
		if i.Err != nil {
			i.Cancel()
		}
	}()
	_, err := os.Stat(i.IndexFilePath)
	if !os.IsNotExist(err) {
		i.Err = fmt.Errorf("index file already exists")
		return
	}

	// 连接 sqlite 索引数据库
	i.IndexDB, err = gorm.Open(sqlite.Open(i.IndexFilePath), &gorm.Config{})
	if err != nil {
		i.Err = err
		return
	}
	defer func() {
		sqlDB, err := i.IndexDB.DB()
		if err != nil {
			i.Err = err
			return
		}
		_ = sqlDB.Close()
		if i.Err != nil {
			return
		}
	}()
	// 关闭 sqlite 同步，提高性能
	i.IndexDB.Exec("PRAGMA synchronous = OFF")
	i.Err = i.IndexDB.AutoMigrate(&ChunkIndex{})
	if i.Err != nil {
		return
	}
	// 写索引文件
	go i.WriteIndexDB()
	// 解析 xbstream 并生成索引
	for {
		i.StreamChunk()
		if i.Err != nil || i.IsIndexDone {
			i.ChunkIndexChan <- i.CurrentChunkIndex
			close(i.ChunkIndexChan)
			break
		}
	}
	<-i.WriteIndexDBDone
}

func main() {

	var (
		file          *os.File
		indexFilePath string
		err           error
	)

	indexFilePath = "package.tar.gz.db"
	if len(os.Args) < 2 {
		file = os.Stdin
	} else {
		filepath := os.Args[1]
		file, err = os.Open(filepath)
		if err != nil {
			fmt.Printf("failed to open file %q: %v\n", filepath, err)
			return
		}
	}

	indexStream := NewIndexXbstream(
		context.TODO(),
		file,
		os.Stdout,
		indexFilePath,
	)

	go func() {
		defer file.Close()
		indexStream.IndexStream()
		if indexStream.Err != nil {
			_ = os.Remove(indexStream.IndexFilePath)
			log.Fatalf("index stream error: %+v\n", indexStream.Err)
			return
		}
	}()

	// stream index file
	indexStream.StreamIndexFile(os.Stdout)
	if indexStream.Err != nil {
		indexStream.Cancel()
	}
	fmt.Fprintf(os.Stderr, "index file range: %d-%d, offset file chunk size: %d",
		indexStream.IndexFileOffsetStart,
		indexStream.IndexFileOffsetEnd,
		indexStream.IndexFileOffsetFileChunkTotalSize)
}
