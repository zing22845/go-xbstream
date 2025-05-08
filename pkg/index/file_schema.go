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
	ParseIn              *io.PipeWriter `gorm:"-"`
	ParseOut             *io.PipeReader `gorm:"-"`
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
		fs.ParseOut, fs.ParseIn = io.Pipe()
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
			fs.MidPipeIn = fs.ParseIn
			fs.MidPipeOut = fs.ParseOut
			fs.DecompressedFilepath = fs.DecryptedFilepath
		}
	case "":
		fs.MidPipeIn = fs.StreamIn
		fs.MidPipeOut = fs.StreamOut
		fs.DecryptedFilepath = fs.Filepath
		switch fs.DecompressMethod {
		case "qp":
			// no decrypt and decompress
			fs.ParseOut, fs.ParseIn = io.Pipe()
			fs.DecompressedFilepath = strings.TrimSuffix(fs.Filepath, ".qp")
		case "":
			// no decrypt and no decompress
			fs.ParseIn = fs.StreamIn
			fs.ParseOut = fs.StreamOut
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
			_ = fs.MidPipeIn.Close()
		}()
		decryptContext, err := xbcrypt.NewDecryptContext(
			fs.EncryptKey, fs.StreamOut, fs.MidPipeIn, fs.ExtractLimitSize)
		if err != nil {
			return fmt.Errorf("failed to create decrypt context: %w", err)
		}
		err = decryptContext.ProcessChunks()
		return err
	case "":
		// no decrypt
		return nil
	default:
		return fmt.Errorf("unsupported decrypt method %s", fs.DecryptMethod)
	}
}

func (fs *FileSchema) decompressStream() (err error) {
	switch fs.DecompressMethod {
	case "qp":
		var isPartial bool
		defer func() {
			if err != nil {
				_, _ = io.Copy(io.Discard, fs.StreamOut)
			} else if isPartial {
				_, err = io.Copy(io.Discard, fs.MidPipeOut)
			}
			_ = fs.ParseIn.Close()
		}()
		qpressFile := &qpress.ArchiveFile{}
		isPartial, err = qpressFile.DecompressStream(
			fs.MidPipeOut, fs.ParseIn, fs.ExtractLimitSize)
		if err != nil {
			return err
		}
	case "":
		// no decompression
		return nil
	default:
		return fmt.Errorf("unsupported decompress method %s", fs.DecompressMethod)
	}
	return nil
}

func (fs *FileSchema) ProcessFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, fs.ParseOut)
	}()

	go func() {
		// decrypt the stream
		err := fs.decryptStream()
		if err != nil {
			fs.DecryptErr = err.Error()
			return
		}
	}()

	go func() {
		// decompress the stream
		err = fs.decompressStream()
		if err != nil {
			fs.DecompressErr = err.Error()
			return
		}
	}()

	return nil
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
