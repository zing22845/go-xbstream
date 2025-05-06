package index

type MySQLServer struct {
	MySQLVersion string `gorm:"column:mysql_version;type:varchar(64)"`
}

func (MySQLServer) TableName() string {
	return "mysql_server"
}
