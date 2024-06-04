package index

import (
	"bytes"
	"context"
	"database/sql"
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
	"github.com/zing22845/go-xbstream/xbstream"
	"gorm.io/gorm"
)

const (
	OffsetBytesLen = 8 // the storage size of i.IndexFileOffset(int64) is always 8 bytes
)

var (
	REGMySQL8 = regexp.MustCompile(`^8\.`)
	REGMySQL5 = regexp.MustCompile(`^5\.[1,5-7]\.`)
)

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
	IsParseTableSchema                bool
	IndexFilePath                     string
	IndexFilename                     string
	IndexFileSize                     int64
	IndexFileOffsetStart              int64 // [IndexFileOffsetStart
	IndexFileOffsetEnd                int64 // IndexFileOffsetEnd]
	IndexFileOffsetFilename           string
	IndexFileOffsetFileChunkTotalSize int64
	IndexDB                           *gorm.DB
	IsIndexDone                       bool
	IndexTableDone                    chan struct{}
	IndexTableBatchSize               int
	ParserSchemaFileDone              chan struct{}
	SchemaTableDone                   chan struct{}
	SchemaTableBatchSize              int
	ParseTargetFileType               string
	DefaultLikePaths                  []string
	DefaultNotLikePaths               []string
	OpenFilesCatch                    map[string]*os.File
	*MySQLServer
	RegSkipPattern *regexp.Regexp
	Err            error
}

func NewIndexStream(
	ctx context.Context,
	reader io.Reader,
	writer io.WriteCloser,
	indexFilePath string,
	mysqlVersion string,
) *IndexStream {
	i := &IndexStream{
		Reader:               reader,
		Writer:               writer,
		IndexFilePath:        indexFilePath,
		CurrentChunkHeader:   &ChunkHeader{},
		CurrentChunkIndex:    &ChunkIndex{},
		TableSchemaMap:       make(map[string]*TableSchema),
		ChunkIndexChan:       make(chan *ChunkIndex, 100),
		TableSchemaChan:      make(chan *TableSchema, 100),
		SchemaFileChan:       make(chan *TableSchema, 100),
		IndexTableDone:       make(chan struct{}, 1),
		IndexTableBatchSize:  100,
		ParserSchemaFileDone: make(chan struct{}, 1),
		SchemaTableDone:      make(chan struct{}, 1),
		SchemaTableBatchSize: 100,
		MySQLServer: &MySQLServer{
			MySQLVersion: mysqlVersion,
		},
	}
	i.prepareParseSchema()
	i.Offset.Store(0)
	i.CTX, i.Cancel = context.WithCancel(ctx)
	return i
}

func NewExtractStream(
	ctx context.Context,
	indexFilename, mysqlVersion string,
) *IndexStream {
	i := &IndexStream{
		IndexFilename:        indexFilename,
		CurrentChunkIndex:    &ChunkIndex{},
		TableSchemaMap:       make(map[string]*TableSchema),
		ChunkIndexChan:       make(chan *ChunkIndex, 100),
		TableSchemaChan:      make(chan *TableSchema, 100),
		SchemaFileChan:       make(chan *TableSchema, 100),
		IndexTableDone:       make(chan struct{}, 1),
		IndexTableBatchSize:  100,
		ParserSchemaFileDone: make(chan struct{}, 1),
		SchemaTableDone:      make(chan struct{}, 1),
		SchemaTableBatchSize: 100,
		MySQLServer: &MySQLServer{
			MySQLVersion: mysqlVersion,
		},
	}
	i.Offset.Store(0)
	i.CTX, i.Cancel = context.WithCancel(ctx)
	return i
}

func (i *IndexStream) prepareParseSchema() {
	if REGMySQL5.MatchString(i.MySQLVersion) {
		i.ParseTargetFileType = ".frm"
		i.IsParseTableSchema = true
		i.DefaultLikePaths = []string{`%/%%.frm`, `%/%%.frm.qp`}
		i.DefaultNotLikePaths = []string{`mysql/%`, `performance_schema/%`, `sys/%`, `information_schema/%`}
	} else if REGMySQL8.MatchString(i.MySQLVersion) {
		i.ParseTargetFileType = ".ibd"
		i.IsParseTableSchema = true
		i.DefaultLikePaths = []string{`%/%.ibd`, `%.ibd.qp`}
		i.DefaultNotLikePaths = []string{`mysql/%`, `performance_schema/%`, `sys/%`, `information_schema/%`}
	}
	i.RegSkipPattern = regexp.MustCompile(`^(mysql|information_schema|performance_schema|sys)$`)
}

func (i *IndexStream) ConnectIndexDB() {
	if i.IndexDB != nil {
		return
	}
	var err error
	i.IndexDB, err = gorm.Open(sqlite.Open(i.IndexFilePath), &gorm.Config{})
	if err != nil {
		i.Err = err
		return
	}
	// close sqlite sync to improve performance
	i.IndexDB.Exec("PRAGMA synchronous = OFF")
	if i.IndexDB.Error != nil {
		i.Err = i.IndexDB.Error
	}
}

func (i *IndexStream) CloseIndexDB() {
	if i.IndexDB == nil {
		return
	}
	sqlDB, err := i.IndexDB.DB()
	if err != nil {
		return
	}
	_ = sqlDB.Close()
}

func (i *IndexStream) WriteIndexTable() {
	batchIndex := make([]*ChunkIndex, 0, i.IndexTableBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchIndex) > 0 {
			i.insertBatchIndex(batchIndex)
		}
		i.IndexTableDone <- struct{}{}
	}()
	for chunkIndex := range i.ChunkIndexChan {
		if chunkIndex.Filepath == "" {
			continue
		}
		batchIndex = append(batchIndex, chunkIndex)
		if len(batchIndex) == i.IndexTableBatchSize {
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

func (i *IndexStream) insertMySQLServer() {
	result := i.IndexDB.Create(i.MySQLServer)
	if result.Error != nil {
		i.Err = result.Error
	}
}

func (i *IndexStream) queryMySQLServer() {
	result := i.IndexDB.First(i.MySQLServer)
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
	batchSchema := make([]*TableSchema, 0, i.SchemaTableBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchSchema) > 0 {
			i.insertBatchSchema(batchSchema)
		}
		i.SchemaTableDone <- struct{}{}
	}()
	for tableSchema := range i.TableSchemaChan {
		if tableSchema.Filepath == "" {
			continue
		}
		batchSchema = append(batchSchema, tableSchema)
		if len(batchSchema) == i.SchemaTableBatchSize {
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
		if !i.IsParseTableSchema || fileDepth != 2 ||
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
	i.ConnectIndexDB()
	if i.Err != nil {
		return
	}
	defer i.CloseIndexDB()
	// init index db
	err = i.IndexDB.AutoMigrate(
		&MySQLServer{},
		&ChunkIndex{},
	)
	if err != nil {
		i.Err = err
		return
	}
	if i.IsParseTableSchema {
		err = i.IndexDB.AutoMigrate(
			&TableSchema{},
		)
		if err != nil {
			i.Err = err
			return
		}
	}
	// write mysql server info
	i.insertMySQLServer()
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
			<-i.IndexTableDone
			close(i.SchemaFileChan)
			<-i.ParserSchemaFileDone
			close(i.TableSchemaChan)
			<-i.SchemaTableDone
			break
		}
	}
}

func (i *IndexStream) getMySQLVersion() {
	// check if mysql_server exists
	exists := i.CheckTableExists("mysql_server")
	if i.Err != nil {
		return
	}
	if exists {
		// read mysql version info
		i.queryMySQLServer()
		if i.Err != nil {
			return
		}
		if i.MySQLVersion == "" {
			i.Err = fmt.Errorf("mysql_version is empty, can not extract schemas")
		}
		return
	}
	if i.MySQLVersion == "" {
		i.Err = fmt.Errorf("mysql_version is empty, can not extract schemas")
		return
	}
}

func (i *IndexStream) getChunkIndecis(likePaths, notLikePaths []string, onlyFirstChunk bool) {
	// check if chunk_indices exists
	exists := i.CheckTableExists("chunk_indices")
	if i.Err != nil {
		return
	}
	if !exists {
		i.Err = fmt.Errorf("table chunk_indices not found, can not extract schemas")
		return
	}
	if len(likePaths) == 0 {
		likePaths = i.DefaultLikePaths
	}
	likePathsInterface := make([]interface{}, len(likePaths))
	likeConditionParts := make([]string, len(likePaths))
	// get like paths sql condition
	for i := range likePaths {
		likeConditionParts[i] = "filepath LIKE ?"
		likePathsInterface[i] = likePaths[i]
	}
	likeCondition := strings.Join(likeConditionParts, " OR ")
	// get not like paths sql condition
	if len(notLikePaths) == 0 {
		notLikePaths = i.DefaultNotLikePaths
	}
	notLikePaths = append(notLikePaths, i.DefaultNotLikePaths...)
	notLikePathsInterface := make([]interface{}, len(notLikePaths))
	notLikeConditionParts := make([]string, len(notLikePaths))
	for i := range notLikePaths {
		notLikeConditionParts[i] = "filepath NOT LIKE ?"
		notLikePathsInterface[i] = notLikePaths[i]
	}
	notLikeCondition := strings.Join(notLikeConditionParts, " AND ")

	// query chunk_indices rows
	var rows *sql.Rows
	var err error
	if onlyFirstChunk {
		payOffsetExists := i.CheckFieldExists(
			ChunkIndex{}.TableName(), "pay_offset")
		if payOffsetExists {
			rows, err = i.IndexDB.Model(&ChunkIndex{}).
				Where("pay_offset = 0").
				Where(likeCondition, likePathsInterface...).
				Where(notLikeCondition, notLikePathsInterface...).
				Rows()
		} else {
			rows, err = i.IndexDB.Raw(`
				WITH RankedChunks AS (
					SELECT *,
						   ROW_NUMBER() OVER (PARTITION BY filepath ORDER BY start_position) AS row_num
					FROM chunk_indices
				)
				SELECT *
				FROM RankedChunks
				WHERE row_num = 1;
			`).
				Where(likeCondition, likePathsInterface...).
				Where(notLikeCondition, notLikePathsInterface...).
				Rows()
		}
	} else {
		rows, err = i.IndexDB.Model(&ChunkIndex{}).
			Where(likeCondition, likePathsInterface...).
			Where(notLikeCondition, notLikePathsInterface...).
			Rows()
	}

	if err != nil {
		i.Err = fmt.Errorf("get chunk_indices rows error: %w", err)
		return
	}
	defer rows.Close()

	// send chunk index into channel
	for rows.Next() {
		ci := &ChunkIndex{}
		err = i.IndexDB.ScanRows(rows, ci)
		if err != nil {
			i.Err = err
			return
		}
		ci.DecodeFilepath()
		i.ChunkIndexChan <- ci
	}
	// close channel
	close(i.ChunkIndexChan)
	// send done signal
	i.IndexTableDone <- struct{}{}
	if rows.Err() != nil {
		i.Err = rows.Err()
	}
}

func (i *IndexStream) ExtractFiles(r io.ReadSeeker, targetDIR string, likePaths, notLikePaths []string) {
	// extract index file
	i.ExtractIndexFile(r, targetDIR)
	if i.Err != nil {
		return
	}
	// connect sqlite index db
	i.ConnectIndexDB()
	if i.Err != nil {
		return
	}
	defer i.CloseIndexDB()
	// get first chunk_indices
	go i.getChunkIndecis(likePaths, notLikePaths, false)

	// extract schemas from index stream
	for ci := range i.ChunkIndexChan {
		i.ExtractSingleFile(ci, r, targetDIR)
		if i.Err != nil {
			return
		}
	}
}

func (i *IndexStream) ExtractSchemas(r io.ReadSeeker, targetDIR string, likePaths, notLikePaths []string) {
	// extract index file
	i.ExtractIndexFile(r, targetDIR)
	if i.Err != nil {
		return
	}
	// connect sqlite index db
	i.ConnectIndexDB()
	if i.Err != nil {
		return
	}
	defer i.CloseIndexDB()
	// check if table_schemas exists
	exists := i.CheckTableExists("table_schemas")
	if i.Err != nil {
		return
	}
	if exists {
		// table schema exists, skip extract schemas
		return
	}
	i.getMySQLVersion()
	if i.Err != nil {
		return
	}
	// prepare parse schema
	i.prepareParseSchema()
	if !i.IsParseTableSchema {
		i.Err = fmt.Errorf("mysql version not supported: %s", i.MySQLVersion)
		return
	}
	// init table schema
	i.Err = i.IndexDB.AutoMigrate(
		&TableSchema{},
	)
	// get first chunk_indices
	go i.getChunkIndecis(likePaths, notLikePaths, true)
	// parse schema from stream
	go i.ParseSchemaFile()
	// write schema of stream to sqlite
	go i.WriteSchemaTable()

	// extract schemas from index stream
	for ci := range i.ChunkIndexChan {
		i.ExtractSingleSchema(ci, r)
		if i.Err != nil {
			return
		}
	}
	<-i.IndexTableDone
	close(i.SchemaFileChan)
	<-i.ParserSchemaFileDone
	close(i.TableSchemaChan)
	<-i.SchemaTableDone
}

func (i *IndexStream) ExtractSingleFile(ci *ChunkIndex, r io.ReadSeeker, targetDIR string) {
	// seek to chunk start position
	_, err := r.Seek(ci.StartPosition, io.SeekStart)
	if err != nil {
		i.Err = err
		return
	}
	// read chunk
	xr := xbstream.NewReader(r)
	chunk, err := xr.Next()
	if err != nil {
		if err == io.EOF {
			return
		}
		i.Err = err
		return
	}
	// check stream path
	streamPath := string(chunk.Path)
	if streamPath != ci.Filepath {
		i.Err = fmt.Errorf("stream path not equal to chunk path at offset %d", ci.StartPosition)
		return
	}
	// create file if not exists
	f, ok := i.OpenFilesCatch[ci.Filepath]
	if !ok {
		targetFilepath := filepath.Join(targetDIR, ci.Filepath)
		err = os.MkdirAll(
			filepath.Dir(targetFilepath),
			0755,
		)
		if err != nil {
			i.Err = err
			return
		}
		f, err = os.OpenFile(
			targetFilepath,
			os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
			0666,
		)
		if err != nil {
			i.Err = err
			return
		}
		i.OpenFilesCatch[ci.Filepath] = f
	}

	// empty file
	if chunk.Type == xbstream.ChunkTypeEOF {
		f.Close()
		delete(i.OpenFilesCatch, ci.Filepath)
		return
	}
	// seek to chunk pay offset
	_, err = f.Seek(int64(chunk.PayOffset), io.SeekStart)
	if err != nil {
		i.Err = err
		return
	}
	// checksum and write chunk pay to file
	crc32Hash := crc32.NewIEEE()
	tReader := io.TeeReader(chunk, crc32Hash)
	_, err = io.Copy(f, tReader)
	if err != nil {
		i.Err = err
		return
	}
	if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
		i.Err = fmt.Errorf("chunk checksum mismatch")
		return
	}
}

func (i *IndexStream) ExtractSingleSchema(ci *ChunkIndex, r io.ReadSeeker) {
	// generate table schema
	ts, err := NewTableSchema(
		ci.Filepath,
		ci.DecompressedFileType,
		ci.DecompressMethod,
		i.ParseTargetFileType,
	)
	if err != nil {
		i.Err = err
		return
	}
	// close stream when done
	defer func() {
		err = ts.StreamIn.Close()
		if err != nil {
			i.Err = err
		}
	}()
	// insert table schema into channel
	i.SchemaFileChan <- ts
	// seek to chunk start position
	_, err = r.Seek(ci.StartPosition, io.SeekStart)
	if err != nil {
		i.Err = err
		return
	}
	// read chunk
	xr := xbstream.NewReader(r)
	chunk, err := xr.Next()
	if err != nil {
		if err == io.EOF {
			return
		}
		i.Err = err
		return
	}
	// check stream path
	streamPath := string(chunk.Path)
	if streamPath != ci.Filepath {
		i.Err = fmt.Errorf("stream path not equal to chunk path at offset %d", ci.StartPosition)
		return
	}
	// empty file
	if chunk.Type == xbstream.ChunkTypeEOF {
		return
	}
	crc32Hash := crc32.NewIEEE()
	tReader := io.TeeReader(chunk, crc32Hash)
	_, err = io.Copy(ts.StreamIn, tReader)
	if err != nil {
		i.Err = err
		return
	}
	if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
		i.Err = fmt.Errorf("chunk checksum mismatch")
		return
	}
}

func (i *IndexStream) CheckTableExists(tableName string) (exists bool) {
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`
	var result int64
	err := i.IndexDB.Raw(query, tableName).Scan(&result).Error
	if err != nil {
		i.Err = err
		return false
	}
	if result > 0 {
		return true
	}
	return false
}

// checkFieldExists checks if a specific column exists in the specified table
func (i *IndexStream) CheckFieldExists(tableName, fieldName string) (exists bool) {
	var count int
	query := fmt.Sprintf("PRAGMA table_info(%s);", tableName)
	rows, err := i.IndexDB.Raw(query).Rows()
	if err != nil {
		i.Err = err
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, dflt_value, pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return false
		}
		if name == fieldName {
			count++
			break
		}
	}
	return count > 0
}

func (i *IndexStream) ExtractIndexFile(r io.ReadSeeker, targetDIR string) {
	// get index file offset file name from index file name
	if i.IndexFilename == "" {
		i.Err = fmt.Errorf("both index file name and offset file name not found")
		return
	}
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"
	// get index file offset file chunk total size
	i.IndexFileOffsetFileChunkTotalSize = int64( // always 2 chunk
		(ChunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 + // (14 + len("package.tar.gz.db.offset")) * 2
			(ChunkPayFixSize + OffsetBytesLen), // 20 + 8 last chunk without pay
	)
	// extract index file offset file
	n, err := ExtractSingleFile(
		r,
		i.IndexFileOffsetFilename,
		targetDIR,
		-i.IndexFileOffsetFileChunkTotalSize,
		io.SeekEnd)
	if err != nil {
		i.Err = err
		return
	}
	if n != OffsetBytesLen {
		i.Err = fmt.Errorf("offset bytes size not equal to OffsetBytesLen: %d", OffsetBytesLen)
		return
	}
	// read index file offset from index file offset file
	offsetBytes, err := os.ReadFile(filepath.Join(targetDIR, i.IndexFileOffsetFilename))
	if err != nil {
		i.Err = err
		return
	}
	i.IndexFileOffsetStart = int64(binary.LittleEndian.Uint64(offsetBytes))
	if i.IndexFileOffsetStart < 0 {
		i.Err = fmt.Errorf("index file offset start less than 0")
		return
	}
	// extract index file
	i.IndexFilePath = filepath.Join(targetDIR, i.IndexFilename)
	_, err = ExtractSingleFile(
		r,
		i.IndexFilename,
		targetDIR,
		i.IndexFileOffsetStart,
		io.SeekStart,
	)
	if err != nil {
		i.Err = err
		return
	}
}
