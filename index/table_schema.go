package index

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/zing22845/go-frm-parser/frm"
	frmutils "github.com/zing22845/go-frm-parser/frm/utils"
	"github.com/zing22845/go-ibd2schema"
	"github.com/zing22845/go-qpress"
	"gorm.io/gorm"
)

type TableSchema struct {
	gorm.Model
	Filepath             string         `gorm:"column:filepath;type:varchar(4096);uniqueIndex:uk_filepath"`
	TableName            string         `gorm:"column:table_name;type:varchar(256);index:idx_table_schema"`
	SchemaName           string         `gorm:"column:schema_name;type:varchar(256);index:idx_table_schema"`
	CreateStatement      string         `gorm:"column:create_statement;type:text"`
	ParseWarn            string         `gorm:"column:parse_warn;type:text"`
	ParseErr             string         `gorm:"column:parse_err;type:text"`
	DecompressErr        string         `gorm:"column:decompress_err;type:text"`
	ParseTargetFileType  string         `gorm:"-"`
	DecompressedFileType string         `gorm:"-"`
	DecompressMethod     string         `gorm:"-"`
	DecompressedFilepath string         `gorm:"-"`
	StreamIn             *io.PipeWriter `gorm:"-"`
	StreamOut            *io.PipeReader `gorm:"-"`
	ParseIn              *io.PipeWriter `gorm:"-"`
	ParseOut             *io.PipeReader `gorm:"-"`
	ParseDone            chan struct{}  `gorm:"-"`
}

func NewTableSchema(
	filepath,
	decompressedFileType,
	decompressMethod,
	parseTargetFileType string,
) (ts *TableSchema, err error) {
	ts = &TableSchema{
		Filepath:             filepath,
		DecompressedFileType: decompressedFileType,
		DecompressMethod:     decompressMethod,
		ParseTargetFileType:  parseTargetFileType,
	}
	err = ts.prepareStream()
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func (ts *TableSchema) prepareStream() (err error) {
	schema, table := filepath.Split(ts.Filepath)
	schema = strings.TrimRight(schema, "/")
	ts.SchemaName, err = frmutils.DecodeMySQLFile2Object(schema)
	if err != nil {
		return err
	}
	ts.TableName, err = frmutils.DecodeMySQLFile2Object(table)
	if err != nil {
		return err
	}
	ts.StreamOut, ts.StreamIn = io.Pipe()
	switch ts.DecompressMethod {
	case "qp":
		ts.ParseOut, ts.ParseIn = io.Pipe()
		ts.TableName = strings.TrimRight(ts.TableName, ".qp")
	case "":
		ts.ParseIn = ts.StreamIn
		ts.ParseOut = ts.StreamOut
	default:
		return fmt.Errorf("unsupported decompress method %s", ts.DecompressMethod)
	}
	ts.TableName = strings.TrimRight(ts.TableName, ts.ParseTargetFileType)
	return nil
}

func (ts *TableSchema) ParseSchema() {
	switch ts.DecompressedFileType {
	case ".frm":
		err := ts.parseFrmFile()
		if err != nil {
			ts.ParseErr = err.Error()
		}
	case ".ibd":
		err := ts.parseIbdFile()
		if err != nil {
			ts.ParseErr = err.Error()
		}
	default:
		ts.ParseErr = fmt.Sprintf("unsupported file type %s", ts.DecompressedFileType)
	}
}

func (ts *TableSchema) decompressStream() (err error) {
	switch ts.DecompressMethod {
	case "qp":
		defer func() {
			_ = ts.ParseIn.Close()
		}()
		var limitSize int64 = 5 * 1024 * 1024
		qpressFile := &qpress.ArchiveFile{}
		isPartial, err := qpressFile.DecompressStream(
			ts.StreamOut, ts.ParseIn, limitSize)
		if err != nil {
			return err
		}
		if isPartial {
			ts.ParseWarn = fmt.Sprintf("partially decompressed to limit size  %d", limitSize)
			// copy the rest of the stream to discard
			_, err = io.Copy(io.Discard, ts.StreamOut)
			if err != nil {
				return err
			}
		}
	case "":
	default:
		return fmt.Errorf("unsupported decompress method %s", ts.DecompressMethod)
	}
	return nil
}

func (ts *TableSchema) parseFrmFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, ts.ParseOut)
	}()
	go func() {
		// file is qp compressed
		err = ts.decompressStream()
		if err != nil {
			ts.DecompressErr = err.Error()
			return
		}
	}()
	result, err := frm.Parse(ts.TableName, ts.ParseOut)
	if err != nil {
		return err
	}
	ts.CreateStatement = result.String()

	ts.TableName = strings.TrimRight(ts.TableName, ".frm")
	return nil
}

func (ts *TableSchema) parseIbdFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, ts.ParseOut)
	}()
	go func() {
		// file is qp compressed
		err = ts.decompressStream()
		if err != nil {
			ts.DecompressErr = err.Error()
			return
		}
	}()
	tableSpace, err := ibd2schema.NewTableSpace(ts.ParseOut)
	if err != nil {
		return err
	}
	err = tableSpace.DumpSchemas()
	if err != nil {
		return err
	}
	for db, table := range tableSpace.TableSchemas {
		if !strings.EqualFold(ts.SchemaName, db) ||
			!strings.EqualFold(ts.TableName, table.Name) {
			continue
		}
		ts.SchemaName = db
		ts.TableName = table.Name
		ts.CreateStatement = table.DDL
	}
	if ts.CreateStatement == "" {
		return fmt.Errorf("ddl of `%s`.`%s` not found in file %s", ts.SchemaName, ts.TableName, ts.Filepath)
	}
	return nil
}
