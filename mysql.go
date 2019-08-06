package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const InitialCapacity = 256

/*
用于Rows.Scan()使用, 并返回解析结果. 必须注意:
- 参数cache用于对应当次rows的可重用缓存,避免反复创建导致GC! 如果cacheo==nil表明无需cache!. 该参数一般情况不需用到!
- 结果ret不能是nil, 否则反射报错!
*/
type ScanRowFunc func(row *sql.Rows) (interface{}, error)

/*
也用于Rows.Scan()使用, 并返回全部解析结果. 由用户自定义解析过程, 所以没有ScanRowFunc的局限!
*/
type ScanRowsFunc func(rows *sql.Rows) (interface{}, error)

type Operation interface {
	// 用户自定义解析过程
	Scan(psql string, srf ScanRowsFunc, args ...interface{}) (ret interface{}, err error)
	// 根据第一条数据反射结果, 要求首条数据结果不能为nil.
	ScanAll(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error)
	ScanOne2(psql string, ret interface{}, args ...interface{}) (ok bool, err error)
	ScanOne(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error)
	ScanRange(psql string, srf ScanRowFunc, offset int, limit int, args ...interface{}) (ret interface{}, err error)
	ScanPage(psql string, srf ScanRowFunc, offset int, limit int, sort string, desc bool, args ...interface{}) (tot int, ret interface{}, err error)
	scanPageTotal(psql string, meta *SqlMeta, args ...interface{}) (ret int, err error)

	Exec(psql string, args ...interface{}) (ret sql.Result, err error)
	ExecBatch(psql string, argsList ...interface{}) (retList []sql.Result, err error)
}

type Mysql interface {
	Operation
	BeginTx(ctx context.Context) (tx Tx, err error)
}

type Tx interface {
	Operation
	Commit() (err error)
	Rollback() (err error)
}

func Scan(psql string, srf ScanRowsFunc, args ...interface{}) (ret interface{}, err error) {
	return Default.Scan(psql, srf, args...)
}

func ScanAll(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error) {
	return Default.ScanAll(psql, srf, args...)
}

func ScanOne2(psql string, ret interface{}, args ...interface{}) (ok bool, err error) {
	return Default.ScanOne2(psql, ret, args...)
}

func ScanOne(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error) {
	return Default.ScanOne(psql, srf, args...)
}

func ScanRange(psql string, srf ScanRowFunc, offset int, limit int, args ...interface{}) (ret interface{}, err error) {
	return Default.ScanRange(psql, srf, offset, limit, args...)
}

func ScanPage(psql string, srf ScanRowFunc, offset int, limit int, sort string, desc bool, args ...interface{}) (tot int, ret interface{}, err error) {
	return Default.ScanPage(psql, srf, offset, limit, sort, desc, args...)
}

func Exec(psql string, args ...interface{}) (ret sql.Result, err error) {
	return Default.Exec(psql, args...)
}

func ExecBatch(psql string, argsList ...interface{}) (retList []sql.Result, err error) {
	return Default.ExecBatch(psql, argsList...)
}

func BeginTx(ctx context.Context) (Tx, error) {
	return Default.BeginTx(ctx)
}

var (
	Default *mysqlImpl
	Clients map[string]*mysqlImpl = make(map[string]*mysqlImpl, 8) //默认给8个
)

// 注意,该方法非线程安全
func Setup(name string, db *sql.DB, def bool) (err error) {

	keys := strings.Split(name, ",")
	for _, k := range keys {
		if _, ok := Clients[k]; ok {
			err = errors.New("duplicate mysql key " + k)
			return
		}
	}

	client := &mysqlImpl{DB: db}
	for _, k := range keys {
		Clients[k] = client
	}
	if def {
		Default = client
	}

	return
}

func Get(name string) Mysql {
	if rt, ok := Clients[name]; ok {
		return rt
	}
	return nil
}
