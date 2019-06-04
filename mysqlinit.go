package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/obase/conf"
)

const DriverName = "mysql"
const DriverSourceNameFormat = "%s:%s@tcp(%s)/%s?parseTime=1&loc=Local"
const CKEY = "mysql"

// 对接conf.yml, 读取原mysql相关配置
func init() {
	conf.Init()
	configs, ok := conf.GetSlice(CKEY)
	if !ok || len(configs) == 0 {
		return
	}

	for _, config := range configs {
		if key, ok := conf.ElemString(config, "key"); ok {
			address, ok := conf.ElemString(config, "address")
			database, ok := conf.ElemString(config, "database")
			username, ok := conf.ElemString(config, "username")
			password, ok := conf.ElemString(config, "password")
			maxIdleConns, ok := conf.ElemInt(config, "maxIdleConns")
			if !ok {
				maxIdleConns = 16
			}
			maxOpenConns, ok := conf.ElemInt(config, "maxOpenConns")
			if !ok {
				maxOpenConns = 16
			}
			connMaxLifetime, ok := conf.ElemDuration(config, "connMaxLifetime")
			defalt, ok := conf.ElemBool(config, "default")

			db, err := sql.Open(DriverName, fmt.Sprintf(DriverSourceNameFormat, username, password, address, database))
			if err != nil {
				panic(err)
			}
			if maxIdleConns > 0 {
				db.SetMaxIdleConns(maxIdleConns)
			}
			if maxOpenConns > 0 {
				db.SetMaxOpenConns(maxOpenConns)
			}
			if connMaxLifetime > 0 {
				db.SetConnMaxLifetime(connMaxLifetime)
			}

			err = Init(key, db, defalt)
			if err != nil {
				panic(err)
			}
		}
	}
}
