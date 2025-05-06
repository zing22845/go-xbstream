package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/zing22845/go-qpress"
	"github.com/zing22845/go-xbstream/pkg/xbcrypt"
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

func ExtractFilesByIndex(
	ctx context.Context,
	idxFileName string,
	encryptKey []byte,
	rsp *readseekerpool.ReadSeekerPool,
	targetDIR string,
	concurrency int,
	bytesPerSecond uint64, // 添加限速参数，单位为字节/秒
) (n int64, err error) {
	// 如果并发数设置为0或负数，则使用默认值
	if concurrency <= 0 {
		concurrency = 10 // 默认并发数
	}

	// 创建信号量来限制并发数
	sem := make(chan struct{}, concurrency)

	// 创建全局限速器用于所有文件操作共享
	var rateLimiter *rate.Limiter
	if bytesPerSecond > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(bytesPerSecond), 1024*1024)
	}

	// Create a new IndexStream instance
	indexStream := NewIndexStream(
		ctx,         // context will be created in NewIndexStream
		idxFileName, // Index filename
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
		return 0, errors.Wrap(indexStream.Err, "extracting index file")
	}

	// Connect to the SQLite index database
	indexStream.ConnectIndexDB()
	if indexStream.Err != nil {
		return 0, errors.Wrap(indexStream.Err, "connecting to index database")
	}
	defer indexStream.CloseIndexDB()

	// Create a channel for chunk indices
	chunkIndexChan := make(chan *ChunkIndex, 100)

	// Query all chunk indices from the database
	go func() {
		defer close(chunkIndexChan)

		var indices []*ChunkIndex
		result := indexStream.IndexDB.Find(&indices)
		if result.Error != nil {
			err = errors.Wrap(result.Error, "querying chunk indices")
			return
		}

		// Group chunk indices by filepath
		fileChunks := make(map[string][]*ChunkIndex)
		for _, ci := range indices {
			ci.DecodeFilepath()
			fileChunks[ci.Filepath] = append(fileChunks[ci.Filepath], ci)
		}

		// Send chunk indices grouped by file to the channel
		for _, chunks := range fileChunks {
			// Send each chunk to the channel
			for _, chunk := range chunks {
				chunkIndexChan <- chunk
			}
		}
	}()

	// Extract files in parallel
	var wg sync.WaitGroup
	var totalSize atomic.Int64
	errorChan := make(chan error, 1)

	// Track unique filepaths to avoid duplicate extractions
	extractedFiles := sync.Map{}

	for ci := range chunkIndexChan {
		// Skip if we've already started extracting this file
		if _, loaded := extractedFiles.LoadOrStore(ci.Filepath, true); loaded {
			continue
		}

		// 使用信号量控制并发数
		select {
		case sem <- struct{}{}: // 获取信号量，如果已满则阻塞
		case <-ctx.Done(): // 如果上下文被取消，则退出
			return 0, ctx.Err()
		}

		wg.Add(1)
		go func(ci *ChunkIndex) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			// Create a channel for this file's chunks
			fileChunkChan := make(chan *ChunkIndex, 20)

			// Get all chunks for this file
			var fileChunks []*ChunkIndex
			result := indexStream.IndexDB.Where("filepath = ?", ci.Filepath).Find(&fileChunks)
			if result.Error != nil {
				select {
				case errorChan <- errors.Wrapf(result.Error, "querying chunks for file %s", ci.Filepath):
				default:
				}
				return
			}

			// Send all chunks to the channel
			go func() {
				defer close(fileChunkChan)
				for _, chunk := range fileChunks {
					chunk.DecodeFilepath()
					chunk.EncryptKey = indexStream.EncryptKey
					chunk.ExtractLimitSize = indexStream.ExtractLimitSize
					fileChunkChan <- chunk
				}
			}()

			// Extract the file with rate limiting
			fileSize, err := extractFileWithRateLimit(rsp, fileChunkChan, ci.Filepath, targetDIR, rateLimiter)
			if err != nil {
				select {
				case errorChan <- errors.Wrapf(err, "extracting file %s", ci.Filepath):
				default:
				}
				return
			}

			// Handle decryption if needed
			if ci.DecryptMethod == "xbcrypt" {
				err = processDecryptionWithRateLimit(filepath.Join(targetDIR, ci.Filepath), indexStream.EncryptKey, rateLimiter)
				if err != nil {
					select {
					case errorChan <- errors.Wrapf(err, "decrypting file %s", ci.Filepath):
					default:
					}
					return
				}
			}

			// Handle decompression if needed
			if ci.DecompressMethod == "qp" {
				err = processDecompressionWithRateLimit(filepath.Join(targetDIR, ci.Filepath), rateLimiter)
				if err != nil {
					select {
					case errorChan <- errors.Wrapf(err, "decompressing file %s", ci.Filepath):
					default:
					}
					return
				}
			}

			totalSize.Add(fileSize)
		}(ci)
	}

	// Wait for either an error or all goroutines to finish
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// Return first error encountered, if any
	for err := range errorChan {
		if err != nil {
			return 0, err
		}
	}

	return totalSize.Load(), nil
}

// 带限速的文件提取方法
func extractFileWithRateLimit(
	rsp *readseekerpool.ReadSeekerPool,
	cis chan *ChunkIndex,
	filePath,
	targetDIR string,
	limiter *rate.Limiter,
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
			n, err = extractSingleChunkIndexWithRateLimit(ci, rsp, targetFilepath, limiter)
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

// 带限速的解密方法
func processDecryptionWithRateLimit(filePath string, key []byte, limiter *rate.Limiter) error {
	// Read the encrypted file
	encryptedFile, err := os.Open(filePath)
	if err != nil {
		return errors.Wrapf(err, "opening encrypted file %s", filePath)
	}
	defer encryptedFile.Close()

	// Create a temporary file for decrypted content
	decryptedPath := filePath[:len(filePath)-len(".xbcrypt")]
	decryptedFile, err := os.Create(decryptedPath)
	if err != nil {
		return errors.Wrapf(err, "creating decrypted file %s", decryptedPath)
	}
	defer decryptedFile.Close()

	// 应用限速写入
	var writer io.Writer = decryptedFile
	if limiter != nil {
		writer = &rateLimitedWriter{w: decryptedFile, limiter: limiter}
	}

	// Initialize decryption context
	dc, err := xbcrypt.NewDecryptContext(key, encryptedFile, writer, 0)
	if err != nil {
		return errors.Wrap(err, "creating decrypt context")
	}

	// Process decryption
	err = dc.ProcessChunks()
	if err != nil {
		return errors.Wrap(err, "processing decrypt chunks")
	}

	// Remove the encrypted file
	err = os.Remove(filePath)
	if err != nil {
		return errors.Wrapf(err, "removing encrypted file %s", filePath)
	}

	return nil
}

// 带限速的解压缩方法
func processDecompressionWithRateLimit(filePath string, limiter *rate.Limiter) error {
	// Read the compressed file
	compressedFile, err := os.Open(filePath)
	if err != nil {
		return errors.Wrapf(err, "opening compressed file %s", filePath)
	}
	defer compressedFile.Close()

	// Create a temporary file for decompressed content
	decompressedPath := filePath[:len(filePath)-len(".qp")]
	decompressedFile, err := os.Create(decompressedPath)
	if err != nil {
		return errors.Wrapf(err, "creating decompressed file %s", decompressedPath)
	}
	defer decompressedFile.Close()

	// 应用限速写入
	var writer io.Writer = decompressedFile
	if limiter != nil {
		writer = &rateLimitedWriter{w: decompressedFile, limiter: limiter}
	}

	// Initialize qpress decompression
	qpressFile := &qpress.ArchiveFile{}
	_, err = qpressFile.DecompressStream(compressedFile, writer, 0)
	if err != nil {
		return errors.Wrap(err, "decompressing data")
	}

	// Remove the compressed file
	err = os.Remove(filePath)
	if err != nil {
		return errors.Wrapf(err, "removing compressed file %s", filePath)
	}

	return nil
}

// 带限速的文件写入方法
func writeChunkPayloadWithRateLimit(rsp *readseekerpool.ReadSeekerPool, ci *ChunkIndex, filePath string, limiter *rate.Limiter) (n int64, err error) {
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

	// 应用限速写入
	var writer io.Writer = file
	if limiter != nil {
		// 如果有限速器，包装成限速写入器
		writer = &rateLimitedWriter{w: file, limiter: limiter}
	}

	// 直接从读取器写入到文件，避免中间缓冲区
	n, err = io.CopyN(writer, rs, payLen)
	if err != nil {
		return -1, err
	}

	return payLen, nil
}

// 带限速的块写入方法
func writeChunksWithRateLimit(
	rsp *readseekerpool.ReadSeekerPool,
	subChunkChan chan *ChunkIndex,
	filePath string,
	limiter *rate.Limiter,
) (n int64, err error) {
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
				wg.Done()
			}()
			payLen, err = writeChunkPayloadWithRateLimit(rsp, ci, filePath, limiter)
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
	return writtenSize.Load(), nil
}

// 带限速的单块提取方法
func extractSingleChunkIndexWithRateLimit(
	ci *ChunkIndex,
	rsp *readseekerpool.ReadSeekerPool,
	filePath string,
	limiter *rate.Limiter,
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
		n, err = writeChunksWithRateLimit(rsp, subChunkChan, filePath, limiter)
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
