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

func ExtractFilesByIndex(
	ctx context.Context,
	idxFileName string,
	encryptKey []byte,
	rsp *readseekerpool.ReadSeekerPool,
	targetDIR string,
	bytesPerSecond uint64,
	likePaths, notLikePaths []string,
) (n int64, err error) {
	// 创建全局限速器用于所有文件操作共享
	var rateLimiter *rate.Limiter
	if bytesPerSecond > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(bytesPerSecond), 1024*1024)
	}

	// 设置 IndexStream
	indexStream, err := setupIndexStream(ctx, idxFileName, targetDIR, encryptKey, rsp)
	if err != nil {
		return 0, err
	}
	defer indexStream.CloseIndexDB()

	indexStream.ExtractFiles(rsp, targetDIR, likePaths, notLikePaths)

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
	case <-ctx.Done():
		return 0, errors.Wrap(ctx.Err(), "operation timed out or was cancelled")
	}

	return totalSize.Load(), nil
}
