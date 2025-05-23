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
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/meilisearch/meilisearch-go"
	log "github.com/sirupsen/logrus"
	gormv2logrus "github.com/thomas-tacquet/gormv2-logrus"
	"github.com/zing22845/go-xbstream/pkg/xbstream"
	"github.com/zing22845/readseekerpool"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	OffsetBytesLen = 8 // the storage size of i.IndexFileOffset(int64) is always 8 bytes
)

var (
	REGMySQL8 = regexp.MustCompile(`^8\.`)
	REGMySQL5 = regexp.MustCompile(`^5\.[1,5-7]\.`)
)

type TableSchemaMap struct {
	tables map[string]*TableSchema
	mu     sync.Mutex
}

func (t *TableSchemaMap) Get(key string) (*TableSchema, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.tables[key]
	return v, ok
}

func (t *TableSchemaMap) Set(key string, value *TableSchema) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables[key] = value
}

func (t *TableSchemaMap) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tables, key)
}

type IndexStream struct {
	CTX                               context.Context
	Cancel                            context.CancelFunc
	Offset                            atomic.Int64
	ChunkIndexChan                    chan *ChunkIndex
	SchemaFileChan                    chan *TableSchema
	TableSchemaChan                   chan *TableSchema
	IsParseTableSchema                bool
	IsIndexDone                       bool
	IndexFilePath                     string
	IndexFilename                     string
	IndexFileSize                     int64
	IndexFileOffsetStart              int64 // [IndexFileOffsetStart
	IndexFileOffsetEnd                int64 // IndexFileOffsetEnd]
	IndexFileOffsetFilename           string
	IndexFileOffsetFileChunkTotalSize int64
	IndexDB                           *gorm.DB
	IndexTableDone                    chan struct{}
	IndexTableBatchSize               int
	ParserSchemaFileDone              chan struct{}
	SchemaTableDone                   chan struct{}
	SchemaTableBatchSize              int
	ParseTargetFileType               string
	DefaultLikePaths                  []string
	DefaultNotLikePaths               []string
	OpenFilesCatch                    map[string]*os.File
	RegSkipPattern                    *regexp.Regexp
	Err                               error
	GormLogger                        logger.Interface
	IsRemoveLocalIndexFile            bool
	MeilisearchIndex                  meilisearch.IndexManager
	MeilisearchDefaultDoc             map[string]interface{}
	EncryptKey                        []byte
	ExtractLimitSize                  int64
	Conn                              *client.Conn
	*MySQLServer
}

func NewIndexStream(
	ctx context.Context,
	concurrency int,
	indexFilename string,
	baseDIR string,
	mysqlVersion string,
	isRemoveLocalIndexFile bool,
	encryptKey []byte,
	extractLimitSize int64,
	conn *client.Conn,
	meilisearchIndex meilisearch.IndexManager,
	meilisearchDefaultDoc map[string]interface{},
) *IndexStream {
	if extractLimitSize < 0 {
		extractLimitSize = 0
	}
	i := &IndexStream{
		IndexFilePath:        filepath.Join(baseDIR, indexFilename),
		IndexFilename:        indexFilename,
		ChunkIndexChan:       make(chan *ChunkIndex, concurrency),
		TableSchemaChan:      make(chan *TableSchema, concurrency),
		SchemaFileChan:       make(chan *TableSchema, concurrency),
		IndexTableDone:       make(chan struct{}, 1),
		IndexTableBatchSize:  concurrency,
		ParserSchemaFileDone: make(chan struct{}, 1),
		SchemaTableDone:      make(chan struct{}, 1),
		SchemaTableBatchSize: concurrency,
		MySQLServer: &MySQLServer{
			MySQLVersion: mysqlVersion,
		},
		GormLogger: gormv2logrus.NewGormlog(
			gormv2logrus.WithLogrus(log.StandardLogger()),
			gormv2logrus.WithGormOptions(
				gormv2logrus.GormOptions{
					SlowThreshold: 800 * time.Millisecond,
					LogLevel:      logger.LogLevel(log.GetLevel()),
					LogLatency:    true,
				},
			),
		),
		IsRemoveLocalIndexFile: isRemoveLocalIndexFile,
		EncryptKey:             encryptKey,
		MeilisearchIndex:       meilisearchIndex,
		MeilisearchDefaultDoc:  meilisearchDefaultDoc,
		ExtractLimitSize:       extractLimitSize,
		Conn:                   conn,
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
		i.DefaultLikePaths = []string{
			`%/%%.frm`, `%/%%.frm.qp`, `%/%%.frm.xbcrypt`, `%/%%.frm.qp.xbcrypt`}
		i.DefaultNotLikePaths = []string{`mysql/%`, `performance_schema/%`, `sys/%`, `information_schema/%`}
	} else if REGMySQL8.MatchString(i.MySQLVersion) {
		i.ParseTargetFileType = ".ibd"
		i.IsParseTableSchema = true
		i.DefaultLikePaths = []string{
			`%/%%.ibd`, `%/%%.ibd.qp`, `%/%%.ibd.xbcrypt`, `%/%%.ibd.qp.xbcrypt`}
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
	db.Logger = i.GormLogger
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

func (i *IndexStream) ParseSchemaFile() {
	defer func() {
		i.ParserSchemaFileDone <- struct{}{}
	}()
	for tableSchema := range i.SchemaFileChan {
		if tableSchema.Filepath == "" {
			continue
		}
		tableSchema.ParseSchema(i.Conn)
		if tableSchema.IsHidden {
			continue
		}
		i.TableSchemaChan <- tableSchema
	}
}

func (i *IndexStream) WriteSchemaTable(db *gorm.DB) {
	batchSchema := make([]*TableSchema, 0, i.SchemaTableBatchSize)
	var batchDoc []map[string]interface{}
	if i.MeilisearchIndex != nil {
		batchDoc = make([]map[string]interface{}, 0, i.SchemaTableBatchSize)
	}
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
		if i.MeilisearchIndex != nil {
			doc, err := tableSchema.GetMeiliSearchDoc(i.MeilisearchDefaultDoc)
			if err != nil {
				log.Warnf("get meilisearch document failed %+v", err)
			}
			batchDoc = append(batchDoc, doc)
		}
		if len(batchSchema) == i.SchemaTableBatchSize {
			i.insertBatchSchema(batchSchema, db)
			if i.Err != nil {
				return
			}
			if i.MeilisearchIndex != nil {
				task, err := i.MeilisearchIndex.AddDocuments(batchDoc)
				if err != nil {
					log.Warnf("create meilisearch documents task(%d) failed %+v",
						task.TaskUID, err)
				}
				log.Infof("id_prefix: %s, doc count: %d, taskUID: %d",
					i.MeilisearchDefaultDoc["id_prefix"], len(batchDoc), task.TaskUID)
			}
			// Clear the batch without re-allocating memory.
			batchSchema = batchSchema[:0]
			batchDoc = batchDoc[:0]
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

func (i *IndexStream) IsNeedParsSchema(ci *ChunkIndex) bool {
	fileElements := strings.Split(ci.Filepath, "/")
	fileDepth := len(fileElements)
	if i.IsParseTableSchema &&
		fileDepth == 2 &&
		ci.DecompressedFileType == i.ParseTargetFileType &&
		!i.RegSkipPattern.MatchString(fileElements[0]) {
		return true
	}
	return false
}

func (i *IndexStream) DecodeChunkPayload(
	ci *ChunkIndex,
	r io.Reader,
	payLen int64,
) (n int64, err error) {
	if i.IsNeedParsSchema(ci) {
		return ExtractSchemaByPayload(i.SchemaFileChan, ci, r, payLen)
	}
	return io.CopyN(io.Discard, r, payLen)
}

func (i *IndexStream) IndexHeader(header *xbstream.ChunkHeader, ci *ChunkIndex) (newCI *ChunkIndex) {
	if ci == nil {
		i.Err = fmt.Errorf("empty chunk index")
		return
	}
	// set offset
	i.Offset.Add(int64(header.HeaderSize))
	// set index by file path
	filepath := string(header.Path)
	if filepath == ci.Filepath {
		ci.EndPosition = i.Offset.Load()
		ci.PayOffset = header.PayOffset
	} else {
		// send last exist chunk index to channel
		if ci.EndPosition != 0 {
			i.ChunkIndexChan <- ci
		}
		// new file
		ci = &ChunkIndex{
			Filepath:         filepath,
			StartPosition:    ci.EndPosition,
			EndPosition:      i.Offset.Load(),
			PayOffset:        header.PayOffset,
			EncryptKey:       i.EncryptKey,
			ExtractLimitSize: ci.ExtractLimitSize,
		}
	}
	ci.DecodeFilepath()
	return ci
}

func (i *IndexStream) DecodeChunk(xr *xbstream.Reader, ci *ChunkIndex) *ChunkIndex {
	// decode chunk header
	header, err := DecodeChunkHeader(xr)
	if err != nil {
		if err == io.EOF {
			// send last chunk index to channel
			i.ChunkIndexChan <- ci
			i.IsIndexDone = true
			return ci
		}
		i.Err = err
		return nil
	}

	// generate index by current chunk header
	ci = i.IndexHeader(header, ci)
	if i.Err != nil {
		return nil
	}

	// type EOF (end of inner file)
	if header.Type == xbstream.ChunkTypeEOF {
		return ci
	}

	// pay load
	payLen := int64(header.PayLen)
	n, err := i.DecodeChunkPayload(
		ci,
		xr,
		payLen)
	if err != nil {
		i.Err = err
		return ci
	}
	i.Offset.Add(n)
	ci.EndPosition = i.Offset.Load()
	return ci
}

// StreamIndexFile write index file and index file offset to xbstream
func (i *IndexStream) StreamIndexFile(w io.WriteCloser) {
	i.IndexFileOffsetStart = i.Offset.Load()
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
		if i.IsRemoveLocalIndexFile {
			_ = os.Remove(i.IndexFilePath)
		}
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
	r = io.TeeReader(r, w)
	xr := xbstream.NewReader(r)
	ci := &ChunkIndex{}
	for {
		ci = i.DecodeChunk(xr, ci)
		if i.Err != nil || i.IsIndexDone {
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
			Where(notLikeCondition, notLikePathsInterface...).
			Order("filepath, pay_offset ASC")
	}
	tx.Find(&indices)
	if tx.Error != nil {
		i.Err = fmt.Errorf("get chunk_indices rows error: %w", tx.Error)
	}
	for _, ci := range indices {
		ci.EncryptKey = i.EncryptKey
		ci.ExtractLimitSize = i.ExtractLimitSize
		ci.DecodeFilepath()
		i.ChunkIndexChan <- ci
		if i.Err != nil {
			return
		}
	}
}

func (i *IndexStream) ExtractSingleFile(
	rs io.ReadSeeker,
	ci *ChunkIndex,
	targetDIR string,
) (n int64, err error) {
	// create base dir
	targetFilePath := filepath.Join(targetDIR, ci.OriginalFilepath)
	baseDir := filepath.Dir(targetFilePath)
	err = os.MkdirAll(baseDir, 0755)
	if err != nil {
		return 0, err
	}
	// create target file
	targetFile, err := os.Create(targetFilePath)
	if err != nil {
		return 0, err
	}
	log.Infof("create file success: %s", targetFilePath)
	defer targetFile.Close()

	// create file schema
	var fileSchema *FileSchema
	fileSchema, err = NewFileSchema(
		i.CTX,
		ci.Filepath,
		ci.ExtractLimitSize,
		ci.EncryptKey,
		ci.DecryptedFileType,
		ci.DecryptMethod,
		ci.DecompressedFileType,
		ci.DecompressMethod,
	)
	if err != nil {
		return 0, err
	}
	defer fileSchema.StreamIn.Close()

	// connect sqlite index db
	i.ConnectIndexDB()
	if i.Err != nil {
		return 0, i.Err
	}
	defer i.CloseIndexDB()

	// get chunk_indices
	go i.getChunkIndecis([]string{ci.Filepath}, nil, false)
	// read chunks and write to fileSchema's streamIn
	go func(fileSchema *FileSchema) {
		for subCi := range i.ChunkIndexChan {
			_, err = rs.Seek(subCi.StartPosition, io.SeekStart)
			if err != nil {
				i.Err = err
				break
			}
			xr := xbstream.NewReader(rs)
			// decode chunk header
			header, err := DecodeChunkHeader(xr)
			if err != nil {
				if err == io.EOF {
					break
				}
				i.Err = err
				break
			}
			if subCi.PayOffset != header.PayOffset {
				i.Err = fmt.Errorf("chunk pay offset not equal to chunk index pay offset")
				break
			}
			payLen := int64(header.PayLen)
			_, err = io.CopyN(fileSchema.StreamIn, xr, payLen)
			if err != nil {
				i.Err = err
				break
			}
		}
		// close streamIn
		fileSchema.StreamIn.Close()
		if i.Err != nil {
			log.Infof("write file %s with error: %s", ci.OriginalFilepath, i.Err)
			return
		}
		log.Infof("write file success: %s", ci.OriginalFilepath)
	}(fileSchema)
	return fileSchema.ProcessToWriter(targetFile)
}

func (i *IndexStream) ExtractFiles(
	rsp *readseekerpool.ReadSeekerPool,
	concurrency int,
	targetDIR string,
	likePaths, notLikePaths []string,
) (totalSize int64, err error) {
	// extract index file
	i.ExtractIndexFile(rsp, targetDIR)
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
	go i.getChunkIndecis(likePaths, notLikePaths, true)

	// extract files from index stream
	var wg sync.WaitGroup
	for ci := range i.ChunkIndexChan {
		wg.Add(1)
		go func(ci *ChunkIndex) {
			defer wg.Done()
			rs, err := rsp.Get()
			if err != nil {
				i.Err = err
				return
			}
			defer rsp.Put(rs)
			subStream := NewIndexStream(
				i.CTX,
				concurrency,
				i.IndexFilename,
				targetDIR,
				"",    // MySQL version not needed for extraction
				false, // Don't remove local index file
				i.EncryptKey,
				0,   // No extract limit size
				nil, // No MySQL connection
				nil, // No Meilisearch index
				nil, // No Meilisearch default doc
			)
			n, err := subStream.ExtractSingleFile(rs, ci, targetDIR)
			if err != nil {
				close(i.ChunkIndexChan)
				i.Err = err
			}
			atomic.AddInt64(&totalSize, n)
		}(ci)
	}
	wg.Wait()
	return totalSize, i.Err
}

func (i *IndexStream) ExtractSchemas(
	rsp *readseekerpool.ReadSeekerPool,
	targetDIR string,
	likePaths, notLikePaths []string,
) {
	// extract index file
	i.ExtractIndexFile(rsp, targetDIR)
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
	var wg sync.WaitGroup
	for ci := range i.ChunkIndexChan {
		rs, err := rsp.Get()
		if err != nil {
			i.Err = err
			return
		}
		wg.Add(1)
		go func(ci *ChunkIndex, rs io.ReadSeeker) {
			defer wg.Done()
			defer rsp.Put(rs)
			ci.DecodeFilepath()
			err = ExtractSingleSchema(ci, i.SchemaFileChan, rs)
			if err != nil {
				i.Err = err
			}
		}(ci, rs)
	}
	wg.Wait()
	<-i.IndexTableDone
	close(i.SchemaFileChan)
	<-i.ParserSchemaFileDone
	close(i.TableSchemaChan)
	<-i.SchemaTableDone
}

func (i *IndexStream) ExtractIndexFile(rsp *readseekerpool.ReadSeekerPool, targetDIR string) {
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
	cis := make(chan *ChunkIndex, 1)
	rs, err := rsp.Get()
	if err != nil {
		i.Err = err
		return
	}
	offset, err := rs.Seek(-i.IndexFileOffsetFileChunkTotalSize, io.SeekEnd)
	if err != nil {
		i.Err = err
		return
	}
	rsp.Put(rs)
	ci := &ChunkIndex{
		Filepath:         i.IndexFileOffsetFilename,
		StartPosition:    offset,
		EndPosition:      offset + i.IndexFileOffsetFileChunkTotalSize,
		EncryptKey:       i.EncryptKey,
		ExtractLimitSize: i.ExtractLimitSize,
	}
	cis <- ci
	close(cis)
	n, err := ExtractFile(rsp, cis, i.IndexFileOffsetFilename, targetDIR)
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
	ci = &ChunkIndex{
		Filepath:         i.IndexFilename,
		StartPosition:    i.IndexFileOffsetStart,
		EncryptKey:       i.EncryptKey,
		EndPosition:      offset,
		ExtractLimitSize: i.ExtractLimitSize,
	}
	cis = make(chan *ChunkIndex, 1)
	cis <- ci
	close(cis)
	i.IndexFilePath = filepath.Join(targetDIR, i.IndexFilename)
	_, err = ExtractFile(
		rsp,
		cis,
		i.IndexFilename,
		targetDIR,
	)
	if err != nil {
		i.Err = err
		return
	}
}
