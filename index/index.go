package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/glebarez/sqlite"
	"github.com/pkg/errors"
	"github.com/zing22845/go-frm-parser/frm"
	frmutils "github.com/zing22845/go-frm-parser/frm/utils"
	ibd2schema "github.com/zing22845/go-ibd2schema"
	"github.com/zing22845/go-qpress"
	"github.com/zing22845/go-xbstream/xbstream"
	"gorm.io/gorm"
)

const (
	// FlagChunkIgnorable indicates a chunk as ignorable
	FlagChunkIgnorable ChunkFlag = 0x01
	// ChunkTypePayload indicates chunk contains file payload
	ChunkTypePayload = ChunkType('P')
	// ChunkTypeEOF indicates chunk is the eof marker for a file
	ChunkTypeEOF = ChunkType('E')
	// ChunkTypeUnknown indicates the chunk was a type that was unknown to xbstream
	ChunkTypeUnknown   = ChunkType(0)
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
	OffsetBytesLen     = 8 // the storage size of i.IndexFileOffset(int64) is always 8 bytes
)

var (
	REGMySQL8 = regexp.MustCompile(`^8\.`)
	REGMySQL5 = regexp.MustCompile(`^5\.[5-7]\.`)
)

// ChunkFlag represents a chunks bit flag set
type ChunkFlag uint8

// ChunkType designates a given chunks type
type ChunkType uint8 // Type of Chunk

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
	Filepath             string `gorm:"column:filepath;type:varchar(4096);index:idx_filepath_start_position"`
	StartPosition        int64  `gorm:"column:start_position;type:bigint;index:idx_filepath_start_position"`
	EndPosition          int64  `gorm:"column:end_position;type:bigint"`
	DecompressedFileType string `gorm:"-"`
	DecompressMethod     string `gorm:"-"`
	PayOffset            uint64 `gorm:"-"`
}

func (ci *ChunkIndex) DecodeFilepath() {
	// get ext of ci.Filepath
	ext := filepath.Ext(ci.Filepath)
	switch ext {
	case ".qp":
		ci.DecompressMethod = "qp"
		ci.DecompressedFileType = filepath.Ext(ci.Filepath[:len(ci.Filepath)-len(ext)])
	default:
		ci.DecompressMethod = ""
		ci.DecompressedFileType = ext
	}
}

type TableSchema struct {
	gorm.Model
	Filepath             string         `gorm:"column:filepath;type:varchar(4096);uniqueIndex:uk_filepath"`
	TableName            string         `gorm:"column:table_name;type:varchar(256);index:idx_table_schema"`
	SchemaName           string         `gorm:"column:schema_name;type:varchar(256);index:idx_table_schema"`
	CreateStatement      string         `gorm:"column:create_statement;type:text"`
	ParseWarn            string         `gorm:"column:parse_warn;type:text"`
	ParseErr             string         `gorm:"column:parse_err;type:text"`
	DecompressErr        string         `gorm:"column:decompress_err;type:text"`
	ParseTargetFileType  string         `gorm:"-"`
	DecompressedFileType string         `gorm:"-"`
	DecompressMethod     string         `gorm:"-"`
	DecompressedFilepath string         `gorm:"-"`
	StreamIn             *io.PipeWriter `gorm:"-"`
	StreamOut            *io.PipeReader `gorm:"-"`
	ParseIn              *io.PipeWriter `gorm:"-"`
	ParseOut             *io.PipeReader `gorm:"-"`
	ParseDone            chan struct{}  `gorm:"-"`
}

func NewTableSchema(
	filepath,
	decompressedFileType,
	decompressMethod,
	parseTargetFileType string,
) (ts *TableSchema, err error) {
	ts = &TableSchema{
		Filepath:             filepath,
		DecompressedFileType: decompressedFileType,
		DecompressMethod:     decompressMethod,
		ParseTargetFileType:  parseTargetFileType,
	}
	err = ts.prepareStream()
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func (ts *TableSchema) prepareStream() (err error) {
	schema, table := filepath.Split(ts.Filepath)
	schema = strings.TrimRight(schema, "/")
	ts.SchemaName, err = frmutils.DecodeMySQLFile2Object(schema)
	if err != nil {
		return err
	}
	ts.TableName, err = frmutils.DecodeMySQLFile2Object(table)
	if err != nil {
		return err
	}
	ts.StreamOut, ts.StreamIn = io.Pipe()
	switch ts.DecompressMethod {
	case "qp":
		ts.ParseOut, ts.ParseIn = io.Pipe()
		ts.TableName = strings.TrimRight(ts.TableName, ".qp")
	case "":
		ts.ParseIn = ts.StreamIn
		ts.ParseOut = ts.StreamOut
	default:
		return fmt.Errorf("unsupported decompress method %s", ts.DecompressMethod)
	}
	ts.TableName = strings.TrimRight(ts.TableName, ts.ParseTargetFileType)
	return nil
}

func (ts *TableSchema) ParseSchema() {
	switch ts.DecompressedFileType {
	case ".frm":
		err := ts.parseFrmFile()
		if err != nil {
			ts.ParseErr = err.Error()
		}
	case ".ibd":
		err := ts.parseIbdFile()
		if err != nil {
			ts.ParseErr = err.Error()
		}
	default:
		ts.ParseErr = fmt.Sprintf("unsupported file type %s", ts.DecompressedFileType)
	}
}

func (ts *TableSchema) decompressStream() (err error) {
	switch ts.DecompressMethod {
	case "qp":
		defer func() {
			_ = ts.ParseIn.Close()
		}()
		var limitSize int64 = 5 * 1024 * 1024
		qpressFile := &qpress.ArchiveFile{}
		isPartial, err := qpressFile.DecompressStream(
			ts.StreamOut, ts.ParseIn, limitSize)
		if err != nil {
			return err
		}
		if isPartial {
			ts.ParseWarn = fmt.Sprintf("partially decompressed to limit size  %d", limitSize)
			// copy the rest of the stream to discard
			_, err = io.Copy(io.Discard, ts.StreamOut)
			if err != nil {
				return err
			}
		}
	case "":
	default:
		return fmt.Errorf("unsupported decompress method %s", ts.DecompressMethod)
	}
	return nil
}

func (ts *TableSchema) parseFrmFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, ts.ParseOut)
	}()
	go func() {
		// file is qp compressed
		err = ts.decompressStream()
		if err != nil {
			ts.DecompressErr = err.Error()
			return
		}
	}()
	result, err := frm.Parse(ts.TableName, ts.ParseOut)
	if err != nil {
		return err
	}
	ts.CreateStatement = result.String()

	ts.TableName = strings.TrimRight(ts.TableName, ".frm")
	return nil
}

func (ts *TableSchema) parseIbdFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, ts.ParseOut)
	}()
	go func() {
		// file is qp compressed
		err = ts.decompressStream()
		if err != nil {
			ts.DecompressErr = err.Error()
			return
		}
	}()
	tableSpace, err := ibd2schema.NewTableSpace(ts.ParseOut)
	if err != nil {
		return err
	}
	err = tableSpace.DumpSchemas()
	if err != nil {
		return err
	}
	for db, table := range tableSpace.TableSchemas {
		if !strings.EqualFold(ts.SchemaName, db) ||
			!strings.EqualFold(ts.TableName, table.Name) {
			continue
		}
		ts.SchemaName = db
		ts.TableName = table.Name
		ts.CreateStatement = table.DDL
	}
	if ts.CreateStatement == "" {
		return fmt.Errorf("ddl of `%s`.`%s` not found in file %s", ts.SchemaName, ts.TableName, ts.Filepath)
	}
	return nil
}

type IndexStream struct {
	CTX                               context.Context
	Cancel                            context.CancelFunc
	Reader                            io.Reader
	Writer                            io.WriteCloser
	Offset                            atomic.Int64
	CurrentChunkIndex                 *ChunkIndex
	CurrentChunkHeader                *ChunkHeader
	ChunkIndexChan                    chan *ChunkIndex
	TableSchemaMap                    map[string]*TableSchema
	SchemaFileChan                    chan *TableSchema
	TableSchemaChan                   chan *TableSchema
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
	ParserSchemaFileDone              chan struct{}
	WriteSchemaDBDone                 chan struct{}
	WriteSchemaDBBatchSize            int
	ParseTargetFileType               string
	RegSkipPattern                    *regexp.Regexp
	Err                               error
}

func NewIndexXbstream(
	ctx context.Context,
	reader io.Reader,
	writer io.WriteCloser,
	indexFilePath string,
	mysqlVersion string,
) *IndexStream {
	i := &IndexStream{
		Reader:                 reader,
		Writer:                 writer,
		IndexFilePath:          indexFilePath,
		CurrentChunkHeader:     &ChunkHeader{},
		CurrentChunkIndex:      &ChunkIndex{},
		TableSchemaMap:         make(map[string]*TableSchema),
		ChunkIndexChan:         make(chan *ChunkIndex, 100),
		TableSchemaChan:        make(chan *TableSchema, 100),
		SchemaFileChan:         make(chan *TableSchema, 100),
		WriteIndexDBDone:       make(chan struct{}, 1),
		WriteIndexDBBatchSize:  100,
		ParserSchemaFileDone:   make(chan struct{}, 1),
		WriteSchemaDBDone:      make(chan struct{}, 1),
		WriteSchemaDBBatchSize: 100,
	}
	if REGMySQL5.MatchString(mysqlVersion) {
		i.ParseTargetFileType = ".frm"
	} else if REGMySQL8.MatchString(mysqlVersion) {
		i.ParseTargetFileType = ".ibd"
	}
	i.RegSkipPattern = regexp.MustCompile(`^(mysql|information_schema|performance_schema|sys)$`)
	i.Offset.Store(0)
	i.CTX, i.Cancel = context.WithCancel(ctx)

	return i
}

func (i *IndexStream) WriteIndexTable() {
	batchIndex := make([]*ChunkIndex, 0, i.WriteIndexDBBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchIndex) > 0 {
			i.insertBatchIndex(batchIndex)
		}
		i.WriteIndexDBDone <- struct{}{}
	}()
	for chunkIndex := range i.ChunkIndexChan {
		if chunkIndex.Filepath == "" {
			continue
		}
		batchIndex = append(batchIndex, chunkIndex)
		if len(batchIndex) == i.WriteIndexDBBatchSize {
			i.insertBatchIndex(batchIndex)
			if i.Err != nil {
				return
			}
			// Clear the batch without re-allocating memory.
			batchIndex = batchIndex[:0]
		}
	}
}

func (i *IndexStream) insertBatchIndex(batchIndex []*ChunkIndex) {
	result := i.IndexDB.Create(batchIndex)
	if result.Error != nil {
		i.Err = result.Error
	}
}

func (i *IndexStream) ParseSchemaFile() {
	defer func() {
		i.ParserSchemaFileDone <- struct{}{}
	}()
	for tableSchema := range i.SchemaFileChan {
		if tableSchema.Filepath == "" {
			continue
		}
		tableSchema.ParseSchema()
		i.TableSchemaChan <- tableSchema
	}
}

func (i *IndexStream) WriteSchemaTable() {
	batchSchema := make([]*TableSchema, 0, i.WriteSchemaDBBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchSchema) > 0 {
			i.insertBatchSchema(batchSchema)
		}
		i.WriteSchemaDBDone <- struct{}{}
	}()
	for tableSchema := range i.TableSchemaChan {
		if tableSchema.Filepath == "" {
			continue
		}
		batchSchema = append(batchSchema, tableSchema)
		if len(batchSchema) == i.WriteSchemaDBBatchSize {
			i.insertBatchSchema(batchSchema)
			if i.Err != nil {
				return
			}
			// Clear the batch without re-allocating memory.
			batchSchema = batchSchema[:0]
		}
	}
}

// insertBatchSchema
func (i *IndexStream) insertBatchSchema(batchTable []*TableSchema) {
	result := i.IndexDB.Create(batchTable)
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
	i.Offset.Add(int64(n))
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
	filepath := string(i.CurrentChunkHeader.PathBytes)
	if filepath == i.CurrentChunkIndex.Filepath {
		i.CurrentChunkIndex.EndPosition = i.Offset.Load()
	} else {
		// new file
		i.ChunkIndexChan <- i.CurrentChunkIndex
		i.CurrentChunkIndex = &ChunkIndex{
			Filepath:      filepath,
			StartPosition: i.CurrentChunkIndex.EndPosition,
			EndPosition:   i.Offset.Load(),
		}
	}
	i.CurrentChunkIndex.DecodeFilepath()
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
	i.CurrentChunkIndex.PayOffset = binary.LittleEndian.Uint64(
		i.CurrentChunkHeader.PayOffsetBytes[:])
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
	var writeSize int64
	var err error

	// read payload into tmp buffer
	payLen := int64(i.CurrentChunkHeader.PayLen)

	switch i.CurrentChunkIndex.DecompressedFileType {
	case ".frm", ".ibd":
		fileElements := strings.Split(i.CurrentChunkIndex.Filepath, "/")
		fileDepth := len(fileElements)
		if fileDepth != 2 ||
			i.CurrentChunkIndex.DecompressedFileType != i.ParseTargetFileType ||
			i.RegSkipPattern.MatchString(fileElements[0]) {
			// skip parse depth not equal to 2 file
			// or current decompressed file type not equal to parse target file type (.ibd or .frm)
			// or file dir match skip pattern
			writeSize, err = io.CopyN(i.Writer, i.Reader, payLen)
			if err != nil {
				i.Err = err
				return
			}
		} else {
			var tableSchema *TableSchema
			if i.CurrentChunkIndex.PayOffset == 0 {
				// init TableSchema
				tableSchema, err = NewTableSchema(
					i.CurrentChunkIndex.Filepath,
					i.CurrentChunkIndex.DecompressedFileType,
					i.CurrentChunkIndex.DecompressMethod,
					i.ParseTargetFileType,
				)
				if err != nil {
					i.Err = err
					return
				}
				i.TableSchemaMap[i.CurrentChunkIndex.Filepath] = tableSchema
				i.SchemaFileChan <- tableSchema
			} else {
				var ok bool
				tableSchema, ok = i.TableSchemaMap[i.CurrentChunkIndex.Filepath]
				if !ok {
					i.Err = fmt.Errorf("table schema not found for %s", i.CurrentChunkIndex.Filepath)
				}
			}
			// combile i.Writer and i.CurrentTableSchema.StreamIn
			multiWriter := io.MultiWriter(tableSchema.StreamIn, i.Writer)
			// copy payload writer
			writeSize, err = io.CopyN(multiWriter, i.Reader, payLen)
			if err != nil {
				i.Err = err
				return
			}
		}
	default:
		writeSize, err = io.CopyN(i.Writer, i.Reader, payLen)
		if err != nil {
			i.Err = err
			return
		}
	}

	i.CurrentChunkHeader.ChunkSize += uint64(writeSize)
	i.Offset.Add(writeSize)
	i.CurrentChunkIndex.EndPosition = i.Offset.Load()
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
		if tableSchema, ok := i.TableSchemaMap[i.CurrentChunkIndex.Filepath]; ok {
			_ = tableSchema.StreamIn.Close()
			delete(i.TableSchemaMap, i.CurrentChunkIndex.Filepath)
		}
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

func (i *IndexStream) StreamIndexFile() {
	i.IndexFileOffsetStart = i.CurrentChunkIndex.EndPosition
	i.IndexFileOffsetEnd = i.IndexFileOffsetStart
	w := xbstream.NewWriter(i.Writer)
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
		_ = os.Remove(i.IndexFilePath)
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
			ChunkHeaderFixSize + // header common fix size(magic + flag + type + pathLen)
				len([]byte(i.IndexFilename)) + // path name bytes size
				ChunkPayFixSize + // pay common fix size(paylen + payoffset + checksum)
				n, // paysize
		)
	}
	// range: left-open and right-closed interval
	i.IndexFileOffsetEnd += int64(ChunkHeaderFixSize + len([]byte(i.IndexFilename)) - 1) // EOF chunk without pay
	err = fw.Close()
	if err != nil {
		i.Err = fmt.Errorf("close index stream writer error: %w", err)
		return
	}
	// write offset of index file as last file's content
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"
	fw, err = w.Create(i.IndexFileOffsetFilename)
	if err != nil {
		i.Err = fmt.Errorf("create index chunks offset writer error: %w", err)
		return
	}
	offsetBytes := make([]byte, OffsetBytesLen)
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
		(ChunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 + // (8+1+1+4 + len("package.tar.gz.db.offset")) * 2
			(ChunkPayFixSize + OffsetBytesLen), // 20 + 8 last chunk without pay
	)
}

func (i *IndexStream) IndexStream() {
	defer func() {
		if i.Err != nil {
			i.Cancel()
		} else {
			i.StreamIndexFile()
			if i.Err != nil {
				i.Cancel()
			}
		}
	}()
	_, err := os.Stat(i.IndexFilePath)
	if !os.IsNotExist(err) {
		i.Err = fmt.Errorf("index file already exists")
		return
	}
	// connect sqlite index db
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
	// close sqlite sync to improve performance
	i.IndexDB.Exec("PRAGMA synchronous = OFF")
	// init index db
	i.Err = i.IndexDB.AutoMigrate(
		&ChunkIndex{},
		&TableSchema{},
	)
	if i.Err != nil {
		return
	}
	// write index of stream to sqlite
	go i.WriteIndexTable()
	// parse schema from stream
	go i.ParseSchemaFile()
	// write schema of stream to sqlite
	go i.WriteSchemaTable()
	// parse xbstream, generate index and parse TableSchema
	for {
		i.StreamChunk()
		if i.Err != nil || i.IsIndexDone {
			// i.CurrentChunkIndex is pushed when next diff chunk is parsed,
			// so last chunk is need to be pushed here
			i.ChunkIndexChan <- i.CurrentChunkIndex
			close(i.ChunkIndexChan)
			<-i.WriteIndexDBDone
			close(i.SchemaFileChan)
			<-i.ParserSchemaFileDone
			close(i.TableSchemaChan)
			<-i.WriteSchemaDBDone
			break
		}
	}
}

func (i *IndexStream) ExtractFiles(targetDIR string) (n int64, err error) {
	files := make(map[string]*os.File)

	xr := xbstream.NewReader(i.Reader)
	for {
		chunk, err := xr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1, err
		}
		fPath := string(chunk.Path)
		// create target file if not exists
		f, ok := files[fPath]
		if !ok {
			targetFilepath := filepath.Join(targetDIR, fPath)
			if err = os.MkdirAll(filepath.Dir(targetFilepath), 0777); err != nil {
				return -1, err
			}
			f, err = os.OpenFile(
				targetFilepath,
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
				0666,
			)
			if err != nil {
				return -1, err
			}
			files[fPath] = f
		}

		if chunk.Type == xbstream.ChunkTypeEOF {
			f.Close()
			continue
		}

		crc32Hash := crc32.NewIEEE()
		tReader := io.TeeReader(chunk, crc32Hash)
		_, err = f.Seek(int64(chunk.PayOffset), io.SeekStart)
		if err != nil {
			return -1, err
		}
		m, err := io.Copy(f, tReader)
		if err != nil {
			return -1, err
		}
		if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
			return -1, errors.Errorf("chunk checksum mismatch")
		}
		n += m
	}
	return n, nil
}

func (i *IndexStream) ExtractSingleFile(
	r io.ReadSeeker,
	path, targetDIR string,
	offset int64,
	whence int,
) (n int64, err error) {
	var targetFile *os.File
	targetFilepath := filepath.Join(targetDIR, path)
	_, err = r.Seek(offset, io.SeekStart)
	if err != nil {
		return -1, err
	}
	xr := xbstream.NewReader(r)
	for {
		chunk, err := xr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1, err
		}
		streamPath := string(chunk.Path)
		if streamPath != path {
			// skip other files
			_, err = r.Seek(int64(chunk.PayLen), whence)
			if err != nil {
				return -1, err
			}
		}
		// create target file if not exists
		if targetFile == nil {
			err = os.MkdirAll(filepath.Dir(targetFilepath), 0777)
			if err != nil {
				return -1, err
			}
			targetFile, err = os.OpenFile(
				targetFilepath,
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
				0666,
			)
			if err != nil {
				return -1, err
			}
		}
		if chunk.Type == xbstream.ChunkTypeEOF {
			targetFile.Close()
			break
		}
		crc32Hash := crc32.NewIEEE()
		tReader := io.TeeReader(chunk, crc32Hash)
		_, err = targetFile.Seek(int64(chunk.PayOffset), io.SeekStart)
		if err != nil {
			return -1, err
		}
		m, err := io.Copy(targetFile, tReader)
		if err != nil {
			return -1, err
		}
		if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
			return -1, errors.Errorf("chunk checksum mismatch")
		}
		n += m
	}
	return n, nil
}

func (i *IndexStream) ExtractIndexFile(r io.ReadSeeker, targetDIR string) (err error) {
	// get index file offset file name from index file name
	if i.IndexFilename == "" {
		return fmt.Errorf("both index file name and offset file name not found")
	}
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"

	// get index file offset file chunk total size
	i.IndexFileOffsetFileChunkTotalSize = int64( // always 2 chunk
		(ChunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 + // (14 + len("package.tar.gz.db.offset")) * 2
			(ChunkPayFixSize + OffsetBytesLen), // 20 + 8 last chunk without pay
	)
	// extract index file offset file
	n, err := i.ExtractSingleFile(
		r,
		i.IndexFileOffsetFilename,
		targetDIR,
		i.IndexFileOffsetFileChunkTotalSize,
		io.SeekEnd)
	if err != nil {
		return err
	}
	if n != OffsetBytesLen {
		return fmt.Errorf("offset bytes size not equal to OffsetBytesLen: %d", OffsetBytesLen)
	}
	// read index file offset from index file offset file
	offsetBytes, err := os.ReadFile(filepath.Join(targetDIR, i.IndexFileOffsetFilename))
	if err != nil {
		return err
	}
	i.IndexFileOffsetStart = int64(binary.LittleEndian.Uint64(offsetBytes))
	if i.IndexFileOffsetStart < 0 {
		return fmt.Errorf("index file offset start less than 0")
	}
	// extract index file
	i.IndexFilePath = filepath.Join(targetDIR, i.IndexFilename)
	_, err = i.ExtractSingleFile(
		r,
		i.IndexFilename,
		targetDIR,
		i.IndexFileOffsetStart,
		io.SeekStart,
	)
	if err != nil {
		return err
	}
	return nil
}
