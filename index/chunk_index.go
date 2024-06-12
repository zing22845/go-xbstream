package index

import (
	"path/filepath"

	"github.com/zing22845/go-xbstream/xbstream"

	"gorm.io/gorm"
)

type ChunkIndex struct {
	gorm.Model
	Filepath             string          `gorm:"column:filepath;type:varchar(4096);index:idx_filepath_start_position;index:idx_filepath_pay_offset"`
	StartPosition        int64           `gorm:"column:start_position;type:bigint;index:idx_filepath_start_position"`
	EndPosition          int64           `gorm:"column:end_position;type:bigint"`
	PayOffset            uint64          `gorm:"column:pay_offset;type:bigint;index:idx_filepath_pay_offset"`
	DecompressedFileType string          `gorm:"-"`
	DecompressMethod     string          `gorm:"-"`
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
		ci.DecompressMethod = "qp"
		ci.DecompressedFileType = filepath.Ext(ci.Filepath[:len(ci.Filepath)-len(ext)])
	default:
		ci.DecompressMethod = ""
		ci.DecompressedFileType = ext
	}
}
