package index

import (
	"crypto/aes"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/zing22845/go-qpress"
	"github.com/zing22845/go-xbstream/pkg/xbcrypt"
	"gorm.io/gorm"
)

type FileSchema struct {
	gorm.Model
	Filepath             string         `gorm:"column:filepath;type:varchar(4096);uniqueIndex:uk_filepath"`
	DecryptErr           string         `gorm:"column:decrypt_err;type:text"`
	DecompressErr        string         `gorm:"column:decompress_err;type:text"`
	ExtractLimitSize     int64          `gorm:"-"`
	DecryptMethod        string         `gorm:"-"`
	DecryptedFileType    string         `gorm:"-"`
	DecryptedFilepath    string         `gorm:"-"`
	DecompressMethod     string         `gorm:"-"`
	DecompressedFileType string         `gorm:"-"`
	DecompressedFilepath string         `gorm:"-"`
	StreamIn             *io.PipeWriter `gorm:"-"`
	StreamOut            *io.PipeReader `gorm:"-"`
	EncryptKey           []byte         `gorm:"-"`
	MidPipeIn            *io.PipeWriter `gorm:"-"`
	MidPipeOut           *io.PipeReader `gorm:"-"`
	OutputWriter         io.Writer      `gorm:"-"`
}

func NewFileSchema(
	filepath string,
	limitSize int64,
	encryptKey []byte,
	decryptedFileType,
	decryptMethod,
	decompressedFileType,
	decompressMethod string,
) (fs *FileSchema, err error) {
	fs = &FileSchema{
		Filepath:             filepath,
		ExtractLimitSize:     limitSize,
		EncryptKey:           encryptKey,
		DecryptedFileType:    decryptedFileType,
		DecryptMethod:        decryptMethod,
		DecompressedFileType: decompressedFileType,
		DecompressMethod:     decompressMethod,
	}
	err = fs.prepareStream()
	if err != nil {
		return nil, err
	}
	return fs, nil
}

// prepareStream prepares the stream for processing
func (fs *FileSchema) prepareStream() (err error) {
	fs.StreamOut, fs.StreamIn = io.Pipe()

	switch fs.DecryptMethod {
	case "xbcrypt":
		fs.DecryptedFilepath = strings.TrimSuffix(fs.Filepath, ".xbcrypt")

		// check if the encrypt key is valid
		keyLen := len(fs.EncryptKey)
		switch keyLen {
		default:
			return aes.KeySizeError(keyLen)
		case 16, 24, 32:
			// do nothing
		}

		switch fs.DecompressMethod {
		case "qp":
			// decrypt and decompress
			fs.MidPipeOut, fs.MidPipeIn = io.Pipe()
			fs.DecompressedFilepath = strings.TrimSuffix(fs.DecryptedFilepath, ".qp")
		case "":
			// decrypt and no decompress
			fs.DecompressedFilepath = fs.DecryptedFilepath
		}
	case "":
		// 对于未加密的情况，中间管道可能在处理时创建
		fs.DecryptedFilepath = fs.Filepath
		switch fs.DecompressMethod {
		case "qp":
			// no decrypt and decompress
			fs.DecompressedFilepath = strings.TrimSuffix(fs.Filepath, ".qp")
		case "":
			// no decrypt and no decompress
			fs.DecompressedFilepath = fs.Filepath
		default:
			return fmt.Errorf("unsupported decompress method %s", fs.DecompressMethod)
		}
	default:
		return fmt.Errorf("unsupported decrypt method %s", fs.DecryptMethod)
	}

	return nil
}

func (fs *FileSchema) decryptStream() (err error) {
	if fs.StreamOut == nil {
		return fmt.Errorf("StreamOut is nil")
	}

	switch fs.DecryptMethod {
	case "xbcrypt":
		defer func() {
			if err != nil {
				if errors.Is(err, xbcrypt.ErrExceedExtractSize) {
					// copy the rest of the stream to discard
				} else {
					err = fmt.Errorf("failed to process chunks: %w", err)
				}
				_, _ = io.Copy(io.Discard, fs.StreamOut)
			}
			if fs.MidPipeIn != nil {
				_ = fs.MidPipeIn.Close()
			}
		}()

		var writer io.Writer
		if fs.DecompressMethod == "qp" {
			if fs.MidPipeIn == nil {
				return fmt.Errorf("MidPipeIn is nil for qp decompression")
			}
			writer = fs.MidPipeIn
		} else {
			if fs.OutputWriter == nil {
				return fmt.Errorf("OutputWriter is nil")
			}
			writer = fs.OutputWriter
		}

		decryptContext, err := xbcrypt.NewDecryptContext(
			fs.EncryptKey, fs.StreamOut, writer, fs.ExtractLimitSize)
		if err != nil {
			return fmt.Errorf("failed to create decrypt context: %w", err)
		}
		err = decryptContext.ProcessChunks()
		return err
	case "":
		// 未加密，直接转发到下一阶段
		if fs.DecompressMethod == "qp" {
			// 需要解压缩，创建中间管道
			fs.MidPipeOut, fs.MidPipeIn = io.Pipe()
			go func() {
				defer fs.MidPipeIn.Close()
				_, _ = io.Copy(fs.MidPipeIn, fs.StreamOut)
			}()
		} else {
			// 无需解压缩，直接复制到输出
			if fs.OutputWriter == nil {
				return fmt.Errorf("OutputWriter is nil")
			}
			go func() {
				_, _ = io.Copy(fs.OutputWriter, fs.StreamOut)
			}()
		}
		return nil
	default:
		return fmt.Errorf("unsupported decrypt method %s", fs.DecryptMethod)
	}
}

func (fs *FileSchema) decompressStream() (err error) {
	switch fs.DecompressMethod {
	case "qp":
		if fs.MidPipeOut == nil {
			return fmt.Errorf("MidPipeOut is nil for qp decompression")
		}
		if fs.OutputWriter == nil {
			return fmt.Errorf("OutputWriter is nil")
		}

		var isPartial bool
		defer func() {
			if err != nil {
				_, _ = io.Copy(io.Discard, fs.MidPipeOut)
			} else if isPartial {
				_, err = io.Copy(io.Discard, fs.MidPipeOut)
			}
		}()
		qpressFile := &qpress.ArchiveFile{}
		isPartial, err = qpressFile.DecompressStream(
			fs.MidPipeOut, fs.OutputWriter, fs.ExtractLimitSize)
		if err != nil {
			return err
		}
	case "":
		// 无需解压缩，已在解密阶段处理
		return nil
	default:
		return fmt.Errorf("unsupported decompress method %s", fs.DecompressMethod)
	}
	return nil
}

// ProcessToWriter 处理文件并写入到指定的写入器
func (fs *FileSchema) ProcessToWriter(writer io.Writer) (err error) {
	if writer == nil {
		return fmt.Errorf("output writer is nil")
	}

	fs.OutputWriter = writer

	// 启动协程来处理流式数据
	errChan := make(chan error, 2) // 一个用于解密错误，一个用于解压错误
	done := make(chan struct{})

	go func() {
		defer close(done)

		// 解密流
		err := fs.decryptStream()
		if err != nil {
			fs.DecryptErr = err.Error()
			errChan <- err
			return
		}

		// 如果需要解压缩，处理解压缩
		if fs.DecompressMethod == "qp" {
			err = fs.decompressStream()
			if err != nil {
				fs.DecompressErr = err.Error()
				errChan <- err
				return
			}
		}
	}()

	// 等待处理完成或出错
	select {
	case err := <-errChan:
		return err
	case <-done:
		return nil
	}
}

// CloseAllPipes 关闭所有打开的管道
func (fs *FileSchema) CloseAllPipes() {
	if fs.StreamIn != nil {
		fs.StreamIn.Close()
	}

	if fs.MidPipeIn != nil {
		fs.MidPipeIn.Close()
	}
}

func (fs *FileSchema) GetMeiliSearchDoc(
	defaultDoc map[string]interface{},
) (
	meilisearchDoc map[string]interface{},
	err error,
) {
	meilisearchDoc = make(map[string]interface{})
	idPrefix := ""
	// convert FileSchema to doc and merge with default Doc fields
	for k, v := range defaultDoc {
		if k == "id_prefix" {
			idPrefix = v.(string)
			continue
		}
		if _, ok := meilisearchDoc[k]; !ok {
			meilisearchDoc[k] = v
		}
	}
	if idPrefix == "" {
		return nil, fmt.Errorf("id_prefix is empty")
	}
	meilisearchDoc["id"] = SanitizeString(fmt.Sprintf("%s_%s", idPrefix, fs.Filepath))
	meilisearchDoc["filepath"] = fs.Filepath
	meilisearchDoc["decrypted_filepath"] = fs.DecryptedFilepath
	meilisearchDoc["decompressed_filepath"] = fs.DecompressedFilepath
	meilisearchDoc["decrypt_error"] = fs.DecryptErr
	meilisearchDoc["decompress_error"] = fs.DecompressErr
	return meilisearchDoc, nil
}
