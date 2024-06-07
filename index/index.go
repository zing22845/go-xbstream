package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"

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
	XbstreamReader                    *xbstream.Reader
	Offset                            atomic.Int64
	CurrentChunkIndex                 *ChunkIndex
	CurrentChunkHeader                *xbstream.ChunkHeader
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
	indexFilename string,
	baseDIR string,
	mysqlVersion string,
) *IndexStream {
	i := &IndexStream{
		IndexFilePath:        filepath.Join(baseDIR, indexFilename),
		IndexFilename:        indexFilename,
		CurrentChunkHeader:   &xbstream.ChunkHeader{},
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

func (i *IndexStream) prepareParseSchema() {
	if REGMySQL5.MatchString(i.MySQLVersion) {
		i.ParseTargetFileType = ".frm"
		i.IsParseTableSchema = true
		i.DefaultLikePaths = []string{`%/%%.frm`, `%/%%.frm.qp`}
		i.DefaultNotLikePaths = []string{`mysql/%`, `performance_schema/%`, `sys/%`, `information_schema/%`}
	} else if REGMySQL8.MatchString(i.MySQLVersion) {
		i.ParseTargetFileType = ".ibd"
		i.IsParseTableSchema = true
		i.DefaultLikePaths = []string{`%/%%.ibd`, `%/%%.ibd.qp`}
		i.DefaultNotLikePaths = []string{`mysql/%`, `performance_schema/%`, `sys/%`, `information_schema/%`}
	}
	i.RegSkipPattern = regexp.MustCompile(`^(mysql|information_schema|performance_schema|sys)$`)
}

func (i *IndexStream) ConnectIndexDB() {
	if i.IndexDB != nil {
		return
	}
	db, err := NewConnection(i.IndexFilePath)
	if err != nil {
		i.Err = err
		return
	}
	i.IndexDB = db
}

func (i *IndexStream) CloseIndexDB() {
	CloseConnection(i.IndexDB)
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

func (i *IndexStream) insertMySQLServer(db *gorm.DB) {
	result := db.Create(i.MySQLServer)
	if result.Error != nil {
		i.Err = result.Error
	}
}

func (i *IndexStream) queryMySQLServer(db *gorm.DB) {
	result := db.First(i.MySQLServer)
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

func (i *IndexStream) WriteSchemaTable(db *gorm.DB) {
	batchSchema := make([]*TableSchema, 0, i.SchemaTableBatchSize)
	defer func() {
		// Insert any remaining records.
		if len(batchSchema) > 0 {
			i.insertBatchSchema(batchSchema, db)
		}
		i.SchemaTableDone <- struct{}{}
	}()
	for tableSchema := range i.TableSchemaChan {
		if tableSchema.Filepath == "" {
			continue
		}
		batchSchema = append(batchSchema, tableSchema)
		if len(batchSchema) == i.SchemaTableBatchSize {
			i.insertBatchSchema(batchSchema, db)
			if i.Err != nil {
				return
			}
			// Clear the batch without re-allocating memory.
			batchSchema = batchSchema[:0]
		}
	}
}

// insertBatchSchema
func (i *IndexStream) insertBatchSchema(batchTable []*TableSchema, db *gorm.DB) {
	result := db.Create(batchTable)
	if result.Error != nil {
		i.Err = result.Error
		return
	}
}

func (i *IndexStream) PreparePayloadWriter(w io.Writer) (newWriter io.Writer) {
	defer func() {
		if i.Err == nil && newWriter == nil {
			i.Err = fmt.Errorf("writer is nil, nowhere to stream payload")
		}
	}()
	// set default writer
	if i.CurrentChunkHeader.PayLen <= 0 {
		return w
	}
	var err error
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
			return w
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
					return w
				}
				i.TableSchemaMap[i.CurrentChunkIndex.Filepath] = tableSchema
				i.SchemaFileChan <- tableSchema
			} else {
				var ok bool
				tableSchema, ok = i.TableSchemaMap[i.CurrentChunkIndex.Filepath]
				if !ok {
					i.Err = fmt.Errorf("table schema not found for %s", i.CurrentChunkIndex.Filepath)
					return w
				}
			}
			// only write to tableSchema.StreamIn if i.Writer is nil
			if w == nil {
				return tableSchema.StreamIn
			}
			// combile i.Writer and i.CurrentTableSchema.StreamIn
			return io.MultiWriter(tableSchema.StreamIn, w)
		}
	}
	return w
}

func (i *IndexStream) StreamPayload(payWriter io.Writer) (n int64) {
	// prepare payload writer
	payWriter = i.PreparePayloadWriter(payWriter)
	if i.Err != nil {
		return
	}
	// copy payload
	payLen := int64(i.CurrentChunkHeader.PayLen)
	n, err := io.CopyN(payWriter, i.XbstreamReader, payLen)
	if err != nil {
		i.Err = err
		return
	}
	return n
}

func (i *IndexStream) DecodeChunkHeader() {
	// read header
	err := i.XbstreamReader.NextHeader(i.CurrentChunkHeader)
	if err != nil {
		if err == io.EOF {
			// stream done
			i.IsIndexDone = true
			return
		}
		i.Err = err
		return
	}
}

func (i *IndexStream) IndexHeader() {
	// set offset
	i.Offset.Add(int64(i.CurrentChunkHeader.HeaderSize))
	// set index by file path
	filepath := string(i.CurrentChunkHeader.Path)
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

func (i *IndexStream) StreamChunk(payWriter io.Writer) {
	// decode chunk header
	i.DecodeChunkHeader()
	if i.Err != nil || i.IsIndexDone {
		return
	}

	// generate index by current chunk header
	i.IndexHeader()

	// type EOF (end of inner file)
	if i.CurrentChunkHeader.Type == xbstream.ChunkTypeEOF {
		if tableSchema, ok := i.TableSchemaMap[i.CurrentChunkIndex.Filepath]; ok {
			_ = tableSchema.StreamIn.Close()
			delete(i.TableSchemaMap, i.CurrentChunkIndex.Filepath)
		}
		return
	}

	// pay load
	n := i.StreamPayload(payWriter)
	if i.Err != nil {
		return
	}
	i.Offset.Add(n)
	i.CurrentChunkIndex.EndPosition = i.Offset.Load()
}

// StreamIndexFile 方法用于将索引文件和索引文件的offset写入到xbstream中

func (i *IndexStream) StreamIndexFile(w io.WriteCloser) {
	i.IndexFileOffsetStart = i.CurrentChunkIndex.EndPosition
	i.IndexFileOffsetEnd = i.IndexFileOffsetStart
	xw := xbstream.NewWriter(w)
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
	fw, err := xw.Create(i.IndexFilename)
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
			xbstream.ChunkHeaderFixSize + // header common fix size(magic + flag + type + pathLen)
				len([]byte(i.IndexFilename)) + // path name bytes size
				xbstream.ChunkPayFixSize + // pay common fix size(paylen + payoffset + checksum)
				n, // paysize
		)
	}
	// range: left-open and right-closed interval
	i.IndexFileOffsetEnd += int64(xbstream.ChunkHeaderFixSize + len([]byte(i.IndexFilename)) - 1) // EOF chunk without pay
	err = fw.Close()
	if err != nil {
		i.Err = fmt.Errorf("close index stream writer error: %w", err)
		return
	}
	// write offset of index file as last file's content
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"
	fw, err = xw.Create(i.IndexFileOffsetFilename)
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
		(xbstream.ChunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 + // (8+1+1+4 + len("package.tar.gz.db.offset")) * 2
			(xbstream.ChunkPayFixSize + OffsetBytesLen), // 20 + 8 last chunk without pay
	)
}

func (i *IndexStream) IndexStream(r io.Reader, w io.WriteCloser) {
	defer func() {
		if i.Err != nil {
			i.Cancel()
		} else {
			i.StreamIndexFile(w)
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
	i.insertMySQLServer(i.IndexDB)
	if i.Err != nil {
		return
	}

	// write index of stream to sqlite
	go i.WriteIndexTable()
	// parse schema from stream
	go i.ParseSchemaFile()
	// write schema of stream to sqlite
	go i.WriteSchemaTable(i.IndexDB)
	// parse xbstream, generate index and parse TableSchema
	i.XbstreamReader = xbstream.NewReader(r)
	for {
		i.StreamChunk(w)
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

func (i *IndexStream) getMySQLVersion(db *gorm.DB) {
	// check if mysql_server exists
	exists, err := CheckTableExists("mysql_server", db)
	if err != nil {
		i.Err = err
		return
	}
	if exists {
		// read mysql version info
		i.queryMySQLServer(db)
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
	defer func() {
		// close channel
		close(i.ChunkIndexChan)
		// send done signal
		i.IndexTableDone <- struct{}{}
	}()
	// check if chunk_indices exists
	exists, err := CheckTableExists("chunk_indices", i.IndexDB)
	if err != nil {
		i.Err = err
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
	var indices []*ChunkIndex
	var tx *gorm.DB
	if onlyFirstChunk {
		var payOffsetExists bool
		payOffsetExists, err = CheckFieldExists(
			ChunkIndex{}.TableName(), "pay_offset", i.IndexDB)
		if err != nil {
			i.Err = err
			return
		}
		if payOffsetExists {
			tx = i.IndexDB.
				Where("pay_offset = 0").
				Where(likeCondition, likePathsInterface...).
				Where(notLikeCondition, notLikePathsInterface...)
		} else {
			tx = i.IndexDB.
				Where(likeCondition, likePathsInterface...).
				Where(notLikeCondition, notLikePathsInterface...).
				Select("id, filepath, MIN(start_position) AS start_position, MIN(end_position) AS end_position").
				Group("filepath")
		}
	} else {
		tx = i.IndexDB.
			Where(likeCondition, likePathsInterface...).
			Where(notLikeCondition, notLikePathsInterface...)
	}
	tx.Find(&indices)
	if tx.Error != nil {
		i.Err = fmt.Errorf("get chunk_indices rows error: %w", tx.Error)
	}
	for _, ci := range indices {
		ci.DecodeFilepath()
		i.ChunkIndexChan <- ci
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
	exists, err := CheckTableExists("table_schemas", i.IndexDB)
	if err != nil {
		i.Err = err
		return
	}
	if exists {
		// table schema exists, skip extract schemas
		return
	}
	i.getMySQLVersion(i.IndexDB)
	if i.Err != nil {
		return
	}
	// prepare parse schema
	i.prepareParseSchema()
	if !i.IsParseTableSchema {
		i.Err = fmt.Errorf("mysql version not supported: %s", i.MySQLVersion)
		return
	}

	// get first chunk_indices
	go i.getChunkIndecis(likePaths, notLikePaths, true)
	// parse schema from stream
	go i.ParseSchemaFile()
	// init table schema
	i.Err = i.IndexDB.AutoMigrate(
		&TableSchema{},
	)
	if i.Err != nil {
		return
	}
	// write table schema
	go i.WriteSchemaTable(i.IndexDB)

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
	_, err = io.Copy(f, chunk)
	if err != nil {
		i.Err = err
		return
	}
}

func (i *IndexStream) ExtractSingleSchema(ci *ChunkIndex, r io.ReadSeeker) {
	/*
		timer := utils.NewSimpleTimer()
		timer.Start()
	*/
	// seek to chunk start position
	_, err := r.Seek(ci.StartPosition, io.SeekStart)
	if err != nil {
		i.Err = err
		return
	}
	i.CurrentChunkIndex = ci
	i.XbstreamReader = xbstream.NewReader(r)
	// decode chunk header
	i.DecodeChunkHeader()
	if i.Err != nil || i.IsIndexDone {
		return
	}
	_ = i.StreamPayload(nil)
	if i.Err != nil {
		return
	}
	// always extract table schema from only first chunk
	if tableSchema, ok := i.TableSchemaMap[i.CurrentChunkIndex.Filepath]; ok {
		_ = tableSchema.StreamIn.Close()
		delete(i.TableSchemaMap, i.CurrentChunkIndex.Filepath)
	}
}

// checkFieldExists checks if a specific column exists in the specified table

func (i *IndexStream) ExtractIndexFile(r io.ReadSeeker, targetDIR string) {
	// get index file offset file name from index file name
	if i.IndexFilename == "" {
		i.Err = fmt.Errorf("both index file name and offset file name not found")
		return
	}
	i.IndexFileOffsetFilename = i.IndexFilename + ".offset"
	// get index file offset file chunk total size
	i.IndexFileOffsetFileChunkTotalSize = int64( // always 2 chunk
		(xbstream.ChunkHeaderFixSize+len([]byte(i.IndexFileOffsetFilename)))*2 + // (14 + len("package.tar.gz.db.offset")) * 2
			(xbstream.ChunkPayFixSize + OffsetBytesLen), // 20 + 8 last chunk without pay
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
