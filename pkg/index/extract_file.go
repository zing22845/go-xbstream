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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/zing22845/go-xbstream/pkg/xbstream"
	"github.com/zing22845/readseekerpool"
	"golang.org/x/time/rate"
)

// 限速写入器，用于包装文件写入并应用限速
type rateLimitedWriter struct {
	w       io.Writer // 原始写入器
	limiter *rate.Limiter
}

func NewRateLimitedWriter(w io.Writer, bytesPerSecond uint64) *rateLimitedWriter {
	var limiter *rate.Limiter
	if bytesPerSecond > 0 {
		// 创建令牌桶限速器，令牌桶容量为1MB，每秒填充速率为指定字节数
		limiter = rate.NewLimiter(rate.Limit(bytesPerSecond), 1024*1024)
	}
	return &rateLimitedWriter{
		w:       w,
		limiter: limiter,
	}
}

func (w *rateLimitedWriter) Write(p []byte) (n int, err error) {
	if w.limiter == nil {
		// 如果限速器为空，直接写入
		return w.w.Write(p)
	}

	// 写入数据并应用限速
	var written int
	for written < len(p) {
		// 计算本次写入量（最多512KB）
		chunkSize := 512 * 1024
		if remaining := len(p) - written; remaining < chunkSize {
			chunkSize = remaining
		}

		// 等待令牌桶中有足够的令牌
		err = w.limiter.WaitN(context.Background(), chunkSize)
		if err != nil {
			return written, err
		}

		// 写入数据块
		nw, err := w.w.Write(p[written : written+chunkSize])
		written += nw
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func writeChunkPayload(rsp *readseekerpool.ReadSeekerPool, ci *ChunkIndex, filePath string) (n int64, err error) {
	// timer := utils.NewSimpleTimer()
	// timer.Start()
	payLen := int64(ci.Chunk.PayLen)
	if ci.Chunk.PayLen == 0 {
		return payLen, nil
	}
	rs, err := rsp.Get()
	if err != nil {
		return -1, errors.Wrap(err, "get reader from pool")
	}
	defer rsp.Put(rs)
	// seek reader to the start position of chunk payload
	payStartPosition := ci.StartPosition + int64(ci.Chunk.HeaderSize)
	_, err = rs.Seek(payStartPosition, io.SeekStart)
	if err != nil {
		return -1, errors.Wrapf(err, "seek reader to pay start position %d", payStartPosition)
	}
	// Open the file only if it exists
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return -1, errors.Wrapf(err, "opening file %s", filePath)
	}
	defer file.Close()
	// seek file to the payload write offset
	_, err = file.Seek(int64(ci.Chunk.PayOffset), io.SeekStart)
	if err != nil {
		return -1, errors.Wrapf(err, "seek to %d", ci.Chunk.PayOffset)
	}
	// copy payload from reader to file
	buffer := bytes.NewBuffer(nil)
	n, err = io.CopyN(buffer, rs, payLen)
	if err != nil {
		return -1, err
	}
	// fmt.Printf("copy chunk payload to buffer took %v\n", timer.Elapsed())
	n, err = file.ReadFrom(buffer)
	if err != nil {
		return -1, err
	}
	// fmt.Printf("copy chunk payload to file took %v\n", timer.Elapsed())
	return payLen, nil
}

func writeChunks(
	rsp *readseekerpool.ReadSeekerPool,
	subChunkChan chan *ChunkIndex,
	filePath string,
) (n int64, err error) {
	// timer := utils.NewSimpleTimer()
	// timer.Start()
	var wg sync.WaitGroup
	var writtenSize atomic.Int64
	errChan := make(chan error, 1)
	for ci := range subChunkChan {
		wg.Add(1)
		go func(ci *ChunkIndex) {
			var err error
			var payLen int64
			defer func() {
				if err != nil {
					select {
					case errChan <- err:
						// Error sent to channel
					default:
						// If the channel is already written to (in case multiple goroutines error out simultaneously),
						// do nothing.
					}
				}
				writtenSize.Add(payLen)
				// fmt.Printf("written size %d\n", writtenSize.Load())
				wg.Done()
			}()
			// fmt.Printf("start writing chunk at %d\n", ci.StartPosition)
			payLen, err = writeChunkPayload(rsp, ci, filePath)
		}(ci)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		if err != nil {
			return -1, err
		}
	}
	// fmt.Printf("write all chunks took %v, written size %d\n", timer.Elapsed(), writtenSize.Load())
	return writtenSize.Load(), nil
}

func readChunks(
	ci *ChunkIndex,
	rsp *readseekerpool.ReadSeekerPool,
	subChunkChan chan *ChunkIndex,
) (err error) {
	defer close(subChunkChan)
	// read chunks
	var totalChunksSize int64
	expectChunksSize := ci.EndPosition - ci.StartPosition
	for totalChunksSize < expectChunksSize {
		err = func() error {
			rs, err := rsp.Get()
			if err != nil {
				err = errors.Wrap(err, "getting read seeker")
				return err
			}
			_, err = rs.Seek(ci.StartPosition+totalChunksSize, io.SeekStart)
			if err != nil {
				err = errors.Wrapf(err, "seek to %d", ci.StartPosition)
				return err
			}
			xr := xbstream.NewReader(rs)
			subChunkIndex := &ChunkIndex{
				Filepath:         ci.Filepath,
				StartPosition:    ci.StartPosition + totalChunksSize,
				EncryptKey:       ci.EncryptKey,
				ExtractLimitSize: ci.ExtractLimitSize,
			}
			defer func() {
				rsp.Put(rs)
				if err != nil {
					return
				}
				subChunkChan <- subChunkIndex
			}()
			subChunkIndex.Chunk = new(xbstream.Chunk)
			err = xr.NextHeader(&subChunkIndex.Chunk.ChunkHeader)
			if err != nil {
				err = errors.Wrap(err, "extracting chunk")
				return err
			}
			payLen := int64(subChunkIndex.Chunk.PayLen)
			totalChunksSize += int64(subChunkIndex.Chunk.HeaderSize) + payLen
			// fmt.Printf("pay length %d, totalChunkSize: %d\n", payLen, totalChunksSize)
			subChunkIndex.EndPosition = subChunkIndex.StartPosition + totalChunksSize
			return nil
		}()
		if err != nil {
			return err
		}
	}
	if totalChunksSize != expectChunksSize {
		err = errors.Errorf("expected read size %d, got %d", expectChunksSize, totalChunksSize)
		return err
	}
	return nil
}

func extractSingleChunkIndex(
	ci *ChunkIndex,
	rsp *readseekerpool.ReadSeekerPool,
	filePath string,
) (n int64, err error) {
	subChunkChan := make(chan *ChunkIndex, 20)
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			if err != nil {
				select {
				case errChan <- err:
					// Error sent to channel
				default:
					// If the channel is already written to (in case multiple goroutines error out simultaneously),
					// do nothing.
				}
			}
			close(errChan)
		}()
		n, err = writeChunks(rsp, subChunkChan, filePath)
	}()
	err = readChunks(ci, rsp, subChunkChan)
	if err != nil {
		return -1, err
	}
	for err := range errChan {
		if err != nil {
			return -1, err
		}
	}
	return n, nil
}

func ExtractFile(
	rsp *readseekerpool.ReadSeekerPool,
	cis chan *ChunkIndex,
	filePath,
	targetDIR string,
) (n int64, err error) {
	targetFilepath := filepath.Join(targetDIR, filePath)
	err = os.MkdirAll(filepath.Dir(targetFilepath), 0777)
	if err != nil {
		return -1, err
	}
	targetFile, err := os.OpenFile(
		targetFilepath,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_EXCL,
		0666,
	)
	if err != nil {
		return -1, errors.Wrapf(err, "create target file %s", targetFilepath)
	}
	defer targetFile.Close()
	var wg sync.WaitGroup
	var fileSize int64
	var totalWritten atomic.Int64
	errChan := make(chan error, 1)
	for ci := range cis {
		if ci.Filepath != filePath {
			return -1, errors.Errorf("unexpected chunk index filepath: %s", ci.Filepath)
		}
		wg.Add(1)
		go func(ci *ChunkIndex) {
			var err error
			var n int64
			defer func() {
				if err != nil {
					select {
					case errChan <- err:
						// Error sent to channel
					default:
						// If the channel is already written to (in case multiple goroutines error out simultaneously),
						// do nothing.
					}
				}
				wg.Done()
				totalWritten.Add(n)
			}()
			n, err = extractSingleChunkIndex(ci, rsp, targetFilepath)
		}(ci)
	}
	// Wait for either an error or all goroutines to finish
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		if err != nil {
			return -1, err
		}
	}
	fileInfo, err := targetFile.Stat()
	if err != nil {
		return -1, errors.Wrapf(err, "stat target file %s", targetFilepath)
	}
	fileSize = fileInfo.Size()
	writtenSize := totalWritten.Load()
	if fileSize != writtenSize {
		return -1, errors.Errorf("expected file size %d, got %d", fileSize, writtenSize)
	}
	return fileSize, nil
}

func ExtractFiles(r io.Reader, targetDIR string) (n int64, err error) {
	files := make(map[string]*os.File)

	xr := xbstream.NewReader(r)
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

// extractFileWorker 处理单个文件的提取
func extractFileWorker(
	ci *ChunkIndex,
	indexStream *IndexStream,
	rsp *readseekerpool.ReadSeekerPool,
	targetDIR string,
	rateLimiter *rate.Limiter,
) (fileSize int64, err error) {
	// 创建 FileSchema 实例
	fileSchema, err := NewFileSchema(
		ci.Filepath,
		ci.ExtractLimitSize,
		ci.EncryptKey,
		ci.DecryptedFileType,
		ci.DecryptMethod,
		ci.DecompressedFileType,
		ci.DecompressMethod,
	)
	if err != nil {
		return 0, errors.Wrap(err, "creating file schema")
	}
	// 确保在函数退出时关闭所有管道，防止资源泄漏
	defer fileSchema.CloseAllPipes()

	// 创建目标文件
	targetFilepath := filepath.Join(targetDIR, fileSchema.DecompressedFilepath)
	err = os.MkdirAll(filepath.Dir(targetFilepath), 0777)
	if err != nil {
		return 0, errors.Wrapf(err, "creating directory for %s", targetFilepath)
	}

	// 创建目标文件
	targetFile, err := os.OpenFile(
		targetFilepath,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_EXCL,
		0666,
	)
	if err != nil {
		return 0, errors.Wrapf(err, "creating target file %s", targetFilepath)
	}
	defer targetFile.Close()

	// 创建写入器链
	var writer io.Writer = targetFile
	if rateLimiter != nil {
		writer = &rateLimitedWriter{w: writer, limiter: rateLimiter}
	}

	// 创建一个goroutine来处理数据写入
	processDone := make(chan struct{})
	processErr := make(chan error, 1)

	// 启动文件处理，直接写入目标文件
	go func() {
		defer close(processDone)
		if err := fileSchema.ProcessToWriter(writer); err != nil {
			processErr <- err
			return
		}
	}()

	// 获取文件的所有数据块
	fileChunks, err := getFileChunks(indexStream, ci.Filepath)
	if err != nil {
		return 0, err
	}

	// 处理所有数据块
	var writtenSize int64
	for _, chunk := range fileChunks {
		rs, err := rsp.Get()
		if err != nil {
			return 0, errors.Wrap(err, "getting reader from pool")
		}

		// 定位到数据块开始位置
		_, err = rs.Seek(chunk.StartPosition+int64(chunk.Chunk.HeaderSize), io.SeekStart)
		if err != nil {
			rsp.Put(rs)
			return 0, errors.Wrapf(err, "seeking to position %d", chunk.StartPosition)
		}

		// 写入数据到 FileSchema 的输入流
		n, err := io.CopyN(fileSchema.StreamIn, rs, int64(chunk.Chunk.PayLen))
		rsp.Put(rs)
		if err != nil {
			return 0, errors.Wrap(err, "writing chunk data")
		}
		writtenSize += n
	}

	// 关闭输入流，表示写入完成
	fileSchema.StreamIn.Close()

	// 等待处理完成
	select {
	case err := <-processErr:
		return 0, err
	case <-processDone:
		// 获取文件大小作为处理后的大小
		fileInfo, err := targetFile.Stat()
		if err != nil {
			return 0, errors.Wrap(err, "getting file size")
		}
		return fileInfo.Size(), nil
	}
}

// getFileChunks 获取文件的所有数据块
func getFileChunks(indexStream *IndexStream, filepath string) ([]*ChunkIndex, error) {
	var fileChunks []*ChunkIndex
	result := indexStream.IndexDB.Where("filepath = ?", filepath).Find(&fileChunks)
	if result.Error != nil {
		return nil, errors.Wrapf(result.Error, "querying chunks for file %s", filepath)
	}

	for _, chunk := range fileChunks {
		chunk.DecodeFilepath()
		chunk.EncryptKey = indexStream.EncryptKey
		chunk.ExtractLimitSize = indexStream.ExtractLimitSize
	}

	return fileChunks, nil
}

// setupIndexStream 设置和初始化 IndexStream
func setupIndexStream(
	ctx context.Context,
	idxFileName string,
	targetDIR string,
	encryptKey []byte,
	rsp *readseekerpool.ReadSeekerPool,
) (*IndexStream, error) {
	indexStream := NewIndexStream(
		ctx,
		idxFileName,
		targetDIR,
		"",    // MySQL version not needed for extraction
		false, // Don't remove local index file
		encryptKey,
		0,   // No extract limit size
		nil, // No MySQL connection
		nil, // No Meilisearch index
		nil, // No Meilisearch default doc
	)

	// Extract the index file
	indexStream.ExtractIndexFile(rsp, targetDIR)
	if indexStream.Err != nil {
		return nil, errors.Wrap(indexStream.Err, "extracting index file")
	}

	// Connect to the SQLite index database
	indexStream.ConnectIndexDB()
	if indexStream.Err != nil {
		return nil, errors.Wrap(indexStream.Err, "connecting to index database")
	}

	return indexStream, nil
}

// getChunkIndices 从数据库获取所有数据块索引
func getChunkIndices(indexStream *IndexStream) (map[string][]*ChunkIndex, error) {
	var indices []*ChunkIndex
	result := indexStream.IndexDB.Find(&indices)
	if result.Error != nil {
		return nil, errors.Wrap(result.Error, "querying chunk indices")
	}

	// Group chunk indices by filepath
	fileChunks := make(map[string][]*ChunkIndex)
	for _, ci := range indices {
		ci.DecodeFilepath()
		fileChunks[ci.Filepath] = append(fileChunks[ci.Filepath], ci)
	}

	return fileChunks, nil
}

func ExtractFilesByIndex(
	ctx context.Context,
	idxFileName string,
	encryptKey []byte,
	rsp *readseekerpool.ReadSeekerPool,
	targetDIR string,
	concurrency int,
	bytesPerSecond uint64,
) (n int64, err error) {
	// 如果并发数设置为0或负数，则使用默认值
	if concurrency <= 0 {
		concurrency = 10 // 默认并发数
	}

	// 创建一个带超时的上下文
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*60*time.Second) // 30分钟超时
	defer cancel()

	// 创建信号量来限制并发数
	sem := make(chan struct{}, concurrency)

	// 创建全局限速器用于所有文件操作共享
	var rateLimiter *rate.Limiter
	if bytesPerSecond > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(bytesPerSecond), 1024*1024)
	}

	// 设置 IndexStream
	indexStream, err := setupIndexStream(ctxWithTimeout, idxFileName, targetDIR, encryptKey, rsp)
	if err != nil {
		return 0, err
	}
	defer indexStream.CloseIndexDB()

	// 获取所有数据块索引，并按文件分组
	fileChunksMap, err := getChunkIndices(indexStream)
	if err != nil {
		return 0, err
	}

	// 创建文件路径列表
	filePaths := make([]string, 0, len(fileChunksMap))
	for filePath := range fileChunksMap {
		filePaths = append(filePaths, filePath)
	}

	// Extract files in parallel
	var wg sync.WaitGroup
	var totalSize atomic.Int64
	errorChan := make(chan error, concurrency) // 增大错误通道容量
	totalFiles := len(filePaths)
	processedFiles := atomic.Int32{}

	// 处理文件
	for fileIndex, filePath := range filePaths {
		// 定期输出进度
		if fileIndex > 0 && fileIndex%10 == 0 {
			fmt.Printf("Processing file %d of %d (%.1f%%)\n",
				fileIndex, totalFiles, float64(fileIndex)/float64(totalFiles)*100)
		}

		// 使用信号量控制并发数
		select {
		case sem <- struct{}{}: // 获取信号量，如果已满则阻塞
		case <-ctxWithTimeout.Done(): // 如果上下文被取消，则退出
			return 0, ctxWithTimeout.Err()
		}

		wg.Add(1)
		go func(filePath string, fileIndex int) {
			defer wg.Done()
			defer func() {
				<-sem // 释放信号量
				newProcessed := processedFiles.Add(1)
				if newProcessed%5 == 0 {
					fmt.Printf("Completed %d of %d files (%.1f%%)\n",
						newProcessed, totalFiles, float64(newProcessed)/float64(totalFiles)*100)
				}
			}()

			// 获取第一个块作为代表
			chunks := fileChunksMap[filePath]
			if len(chunks) == 0 {
				return // 跳过没有块的文件
			}

			ci := chunks[0] // 使用第一个块作为代表

			fileSize, err := extractFileWorker(ci, indexStream, rsp, targetDIR, rateLimiter)
			if err != nil {
				select {
				case errorChan <- errors.Wrapf(err, "processing file %s (index %d)", filePath, fileIndex):
				default:
					// 如果通道已满，记录错误但不阻塞
					fmt.Printf("Error processing file %s (index %d): %v\n", filePath, fileIndex, err)
				}
				return
			}

			totalSize.Add(fileSize)
		}(filePath, fileIndex)
	}

	// 等待所有工作完成或出错
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// 等待完成或上下文取消
	select {
	case <-done:
		// 所有工作完成
		close(errorChan)
		// 检查是否有错误
		for err := range errorChan {
			if err != nil {
				return 0, err
			}
		}
		fmt.Printf("All %d files processed successfully\n", totalFiles)
	case <-ctxWithTimeout.Done():
		return 0, errors.Wrap(ctxWithTimeout.Err(), "operation timed out or was cancelled")
	}

	return totalSize.Load(), nil
}
