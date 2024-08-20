package index

import (
	"crypto/aes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // import test_driver or sqlparser will panic
	errs "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/zing22845/go-frm-parser/frm"
	frmutils "github.com/zing22845/go-frm-parser/frm/utils"
	"github.com/zing22845/go-ibd2schema"
	"github.com/zing22845/go-qpress"
	"github.com/zing22845/go-xbstream/xbcrypt"
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
	ParseDone            chan struct{}  `gorm:"-"`
	IsHidden             bool           `gorm:"-"`
}

func NewTableSchema(
	filepath string,
	limitSize int64,
	encryptKey []byte,
	decryptedFileType,
	decryptMethod,
	decompressedFileType,
	decompressMethod string,
) (ts *TableSchema, err error) {
	ts = &TableSchema{
		Filepath:             filepath,
		ExtractLimitSize:     limitSize,
		EncryptKey:           encryptKey,
		DecryptedFileType:    decryptedFileType,
		DecryptMethod:        decryptMethod,
		DecompressedFileType: decompressedFileType,
		DecompressMethod:     decompressMethod,
	}
	err = ts.prepareStream()
	if err != nil {
		return nil, err
	}
	return ts, nil
}

// prepareStream prepares the stream for parsing
func (ts *TableSchema) prepareStream() (err error) {
	ts.SchemaName, ts.TableName = filepath.Split(ts.Filepath)
	ts.SchemaName = strings.TrimSuffix(ts.SchemaName, "/")
	ts.SchemaName, err = frmutils.DecodeMySQLFile2Object(ts.SchemaName)
	if err != nil {
		return err
	}
	ts.StreamOut, ts.StreamIn = io.Pipe()

	switch ts.DecryptMethod {
	case "xbcrypt":
		ts.ParseOut, ts.ParseIn = io.Pipe()
		ts.TableName = strings.TrimSuffix(ts.TableName, ".xbcrypt")

		// check if the encrypt key is valid
		keyLen := len(ts.EncryptKey)
		switch keyLen {
		default:
			return aes.KeySizeError(keyLen)
		case 16, 24, 32:
			// do nothing
		}

		switch ts.DecompressMethod {
		case "qp":
			// decrypt and decompress
			ts.MidPipeOut, ts.MidPipeIn = io.Pipe()
			ts.TableName = strings.TrimSuffix(ts.TableName, ".qp")
		case "":
			// decrypt and no decompress
			ts.MidPipeIn = ts.ParseIn
			ts.MidPipeOut = ts.ParseOut
		}
	case "":
		ts.MidPipeIn = ts.StreamIn
		ts.MidPipeOut = ts.StreamOut
		switch ts.DecompressMethod {
		case "qp":
			// no decrypt and decompress
			ts.ParseOut, ts.ParseIn = io.Pipe()
			ts.TableName = strings.TrimSuffix(ts.TableName, ".qp")
		case "":
			// no decrypt and no decompress
			ts.ParseIn = ts.StreamIn
			ts.ParseOut = ts.StreamOut
		default:
			return fmt.Errorf("unsupported decompress method %s", ts.DecompressMethod)
		}
	default:
		return fmt.Errorf("unsupported decrypt method %s", ts.DecryptMethod)
	}

	return nil
}

func (ts *TableSchema) ParseSchema(conn *client.Conn) {
	defer func() {
		if r := recover(); r != nil {
			stackBuf := make([]byte, 102400)
			stackSize := runtime.Stack(stackBuf, false)
			ts.ParseErr = fmt.Sprintf("panic occurred: %+v\nstack trace:\n%s", r, stackBuf[:stackSize])
		}
	}()
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
		return
	}
	// verify the create statement
	if conn != nil && ts.CreateStatement != "" {
		err := ts.verifyCreateStatement(conn)
		if err != nil {
			ts.ParseErr = err.Error()
		}
	}
}

func normalizeSQL(sql string) (string, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		// tidb sqlparser not support the sql
		// fallback to remove auto_increment by string replace
		sql = strings.Trim(sql, ";\n")
		re := regexp.MustCompile(`\sAUTO_INCREMENT=\d+\s`)
		sql = re.ReplaceAllString(sql, " ")
		return sql, nil
	}

	if len(stmtNodes) != 1 {
		return "", fmt.Errorf("expected 1 statement, got %d", len(stmtNodes))
	}

	createStmt, ok := stmtNodes[0].(*ast.CreateTableStmt)
	if !ok {
		return "", fmt.Errorf("expected CreateTableStmt, got %T", stmtNodes[0])
	}

	// Remove AUTO_INCREMENT
	for i, opt := range createStmt.Options {
		if opt.Tp == ast.TableOptionAutoIncrement {
			createStmt.Options = append(createStmt.Options[:i], createStmt.Options[i+1:]...)
			break
		}
	}

	// Regenerate SQL
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := createStmt.Restore(ctx); err != nil {
		return "", fmt.Errorf("restoring SQL: %w", err)
	}

	return sb.String(), nil
}

func (ts *TableSchema) verifyCreateStatement(conn *client.Conn) error {
	// show create table statement by conn
	sql := fmt.Sprintf("show create table `%s`.`%s`", ts.SchemaName, ts.TableName)
	result, err := conn.Execute(sql)
	if err != nil {
		return err
	}
	if result.RowNumber() == 0 {
		return fmt.Errorf("no create statement found for table `%s`.`%s`", ts.SchemaName, ts.TableName)
	}
	for ri := range result.Values {
		expect, err := result.GetString(ri, 1)
		if err != nil {
			return errs.Wrap(err, "failed to get create statement")
		}
		actual, err := normalizeSQL(ts.CreateStatement)
		if err != nil {
			return errs.Wrap(err, "failed to normalize create statement from file")
		}
		expect, err = normalizeSQL(expect)
		if err != nil {
			return errs.Wrap(err, "failed to normalize create statement from db")
		}
		if expect != actual {
			err = fmt.Errorf(
				"create statement mismatch for table `%s`.`%s`, expected: %s, actual: %s",
				ts.SchemaName, ts.TableName, expect, actual)
			return err
		}
	}
	return nil
}

func (ts *TableSchema) decryptStream() (err error) {
	switch ts.DecryptMethod {
	case "xbcrypt":
		defer func() {
			if err != nil {
				if errors.Is(err, xbcrypt.ErrExceedExtractSize) {
					ts.ParseWarn = fmt.Sprintf("partially decrypted to limit size  %d", ts.ExtractLimitSize)
					// copy the rest of the stream to discard
				} else {
					err = fmt.Errorf("failed to process chunks: %w", err)
				}
				_, _ = io.Copy(io.Discard, ts.StreamOut)
			}
			_ = ts.MidPipeIn.Close()
		}()
		decryptContext, err := xbcrypt.NewDecryptContext(
			ts.EncryptKey, ts.StreamOut, ts.MidPipeIn, ts.ExtractLimitSize)
		if err != nil {
			return fmt.Errorf("failed to create decrypt context: %w", err)
		}
		err = decryptContext.ProcessChunks()
		return err
	case "":
		// no decrypt
		return nil
	default:
		return fmt.Errorf("unsupported decrypt method %s", ts.DecryptMethod)
	}
}

func (ts *TableSchema) decompressStream() (err error) {
	switch ts.DecompressMethod {
	case "qp":
		var isPartial bool
		defer func() {
			if err != nil {
				_, _ = io.Copy(io.Discard, ts.StreamOut)
			} else if isPartial {
				ts.ParseWarn = fmt.Sprintf("partially decompressed to limit size  %d", ts.ExtractLimitSize)
				_, err = io.Copy(io.Discard, ts.MidPipeOut)
			}
			_ = ts.ParseIn.Close()
		}()
		qpressFile := &qpress.ArchiveFile{}
		isPartial, err = qpressFile.DecompressStream(
			ts.MidPipeOut, ts.ParseIn, ts.ExtractLimitSize)
		if err != nil {
			return err
		}
	case "":
		// no decompression
		return nil
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
		// decrypt the stream
		err := ts.decryptStream()
		if err != nil {
			ts.DecryptErr = err.Error()
			return
		}
	}()
	go func() {
		// decompress the stream
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
	ts.TableName = result.GetName()
	ts.CreateStatement = result.String()
	return nil
}

func (ts *TableSchema) parseIbdFile() (err error) {
	defer func() {
		// drain out the rest of the stream
		_, _ = io.Copy(io.Discard, ts.ParseOut)
	}()
	go func() {
		// decrypt the stream
		err := ts.decryptStream()
		if err != nil {
			ts.DecryptErr = err.Error()
			return
		}
	}()
	go func() {
		// decompress the stream
		err = ts.decompressStream()
		if err != nil {
			ts.DecompressErr = err.Error()
			return
		}
	}()
	ts.TableName = strings.TrimSuffix(ts.TableName, ".ibd")
	ts.TableName, err = frmutils.DecodeMySQLFile2Object(ts.TableName)
	if err != nil {
		return err
	}
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
			log.Infof("unexpected db(%s) or table name(%s) in file %s",
				db, table.Name, ts.Filepath)
			continue
		}
		if table.Hidden != ibd2schema.HT_VISIBLE {
			ts.IsHidden = true
			return nil
		}
		if table.DDL == "" {
			return fmt.Errorf("DDL of `%s`.`%s` is empty in file %s", db, table.Name, ts.Filepath)
		}
		ts.CreateStatement = table.DDL
		return nil
	}
	if ts.CreateStatement == "" {
		return fmt.Errorf("no matching DDL of `%s`.`%s` found in file %s", ts.SchemaName, ts.TableName, ts.Filepath)
	}
	return nil
}

func (ts *TableSchema) GetMeiliSearchDoc(
	defaultDoc map[string]interface{},
) (
	meilisearchDoc map[string]interface{},
	err error,
) {
	meilisearchDoc = make(map[string]interface{})
	idPrefix := ""
	// convert TableSchema to doc and merge with default Doc fields
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
	meilisearchDoc["id"] = SanitizeString(fmt.Sprintf("%s_%s", idPrefix, ts.Filepath))
	meilisearchDoc["schema_name"] = ts.SchemaName
	meilisearchDoc["table_name"] = ts.TableName
	meilisearchDoc["create_statement"] = ts.CreateStatement
	meilisearchDoc["parse_warn"] = ts.ParseWarn
	meilisearchDoc["parse_error"] = ts.ParseErr
	meilisearchDoc["decompress_error"] = ts.DecompressErr
	return meilisearchDoc, nil
}

func SanitizeString(input string) string {
	// Replace invalid characters with an underscore
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	sanitized := re.ReplaceAllString(input, "-")
	return sanitized
}
