package index

import (
	"path/filepath"

	"github.com/zing22845/go-xbstream/pkg/xbstream"

	"gorm.io/gorm"
)

type ChunkIndex struct {
	gorm.Model
	Filepath             string          `gorm:"column:filepath;type:varchar(4096);index:idx_filepath_start_position;index:idx_filepath_pay_offset"`
	StartPosition        int64           `gorm:"column:start_position;type:bigint;index:idx_filepath_start_position"`
	EndPosition          int64           `gorm:"column:end_position;type:bigint"`
	PayOffset            uint64          `gorm:"column:pay_offset;type:bigint;index:idx_filepath_pay_offset"`
	EncryptKey           []byte          `gorm:"-"`
	DecryptedFileType    string          `gorm:"-"`
	DecryptMethod        string          `gorm:"-"`
	DecompressedFileType string          `gorm:"-"`
	DecompressMethod     string          `gorm:"-"`
	ExtractLimitSize     int64           `gorm:"-"`
	OriginalFilepath     string          `gorm:"-"`
	Chunk                *xbstream.Chunk `gorm:"-"`
}

func (ChunkIndex) TableName() string {
	return "chunk_indices"
}

func (ci *ChunkIndex) DecodeFilepath() {
	// get ext of ci.Filepath
	ext := filepath.Ext(ci.Filepath)
	switch ext {
	case ".qp":
		ci.DecryptMethod = ""
		ci.DecryptedFileType = ext
		ci.DecompressMethod = "qp"
		ci.DecompressedFileType = filepath.Ext(ci.Filepath[:len(ci.Filepath)-len(ext)])
		ci.OriginalFilepath = ci.Filepath[:len(ci.Filepath)-len(ext)]
	case ".xbcrypt":
		ci.DecryptMethod = "xbcrypt"
		ci.DecryptedFileType = filepath.Ext(ci.Filepath[:len(ci.Filepath)-len(ext)])
		if ci.DecryptedFileType == ".qp" {
			ci.DecompressMethod = "qp"
			ci.DecompressedFileType = filepath.Ext(
				ci.Filepath[:len(ci.Filepath)-len(ext)-len(ci.DecryptedFileType)])
			ci.OriginalFilepath = ci.Filepath[:len(ci.Filepath)-len(ext)-len(ci.DecryptedFileType)]
		} else {
			ci.DecompressMethod = ""
			ci.DecompressedFileType = ci.DecryptedFileType
			ci.OriginalFilepath = ci.Filepath[:len(ci.Filepath)-len(ext)]
		}
	default:
		ci.DecryptMethod = ""
		ci.DecryptedFileType = ext
		ci.DecompressMethod = ""
		ci.DecompressedFileType = ext
		ci.OriginalFilepath = ci.Filepath
	}
}
