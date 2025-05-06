package index

import (
	"database/sql"
	"fmt"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func NewConnection(dbPath string) (db *gorm.DB, err error) {
	db, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	// close sqlite sync to improve performance
	err = db.Exec("PRAGMA synchronous = OFF").Error
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CloseConnection(db *gorm.DB) {
	if db == nil {
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		return
	}
	_ = sqlDB.Close()
}

func CheckTableExists(tableName string, db *gorm.DB) (exists bool, err error) {
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`
	var result int64
	err = db.Raw(query, tableName).Scan(&result).Error
	if err != nil {
		return false, err
	}
	if result > 0 {
		return true, nil
	}
	return false, nil
}

func CheckFieldExists(
	tableName, fieldName string,
	db *gorm.DB,
) (exists bool, err error) {
	var count int
	query := fmt.Sprintf("PRAGMA table_info(%s);", tableName)
	rows, err := db.Raw(query).Rows()
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt_value sql.NullString
		if err = rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return false, err
		}
		if name == fieldName {
			count++
			break
		}
	}
	return count > 0, nil
}
