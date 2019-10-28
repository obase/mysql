package mysql

import (
	"context"
	"database/sql"
	"math"
	"reflect"
)

type MysqlImpl struct {
	*sql.DB
}

func newMysql(db *sql.DB) *MysqlImpl {
	return &MysqlImpl{
		DB: db,
	}
}

func (m *MysqlImpl) BeginTx(ctx context.Context) (ret Tx, err error) {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	ret = &txImpl{
		Tx: tx,
	}
	return
}

func (m *MysqlImpl) Scan(psql string, srf ScanRowsFunc, args ...interface{}) (ret interface{}, err error) {
	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(psql, args...)

	if err != nil {
		return
	}
	defer rows.Close()

	return srf(rows)
}

func (m *MysqlImpl) ScanAll(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error) {

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(psql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	if rows.Next() {
		var (
			val   interface{}
			slice reflect.Value
		)
		val, err = srf(rows)
		if err != nil {
			return
		}
		slice = reflect.Append(reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(val)), 0, InitialCapacity), reflect.ValueOf(val))

		for rows.Next() {
			val, err = srf(rows)
			if err != nil {
				return
			}
			slice = reflect.Append(slice, reflect.ValueOf(val))
		}
		ret = slice.Interface()
	}
	return
}

func (m *MysqlImpl) ScanOne2(psql string, to interface{}, args ...interface{}) (ok bool, err error) {

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(psql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	if rows.Next() {
		switch to := to.(type) {
		case []interface{}:
			err = rows.Scan(to...)
		default:
			err = rows.Scan(to)
		}
		if err != nil {
			return
		}
		ok = true
	}
	return
}

func (m *MysqlImpl) ScanOne(psql string, srf ScanRowFunc, args ...interface{}) (ret interface{}, err error) {

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(psql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	if rows.Next() {
		ret, err = srf(rows)
		if err != nil {
			return
		}
	}

	return
}

/*如果源SQL没有limit子句,则直接拼到最后即可*/
func (m *MysqlImpl) ScanRange(psql string, srf ScanRowFunc, offset int, limit int, args ...interface{}) (ret interface{}, err error) {
	meta := GetSqlMeta(psql)
	if meta.LimitPsql == "" {
		GenLimitSql(psql, meta)
	}
	args = append(args, offset, limit)

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(meta.LimitPsql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	if rows.Next() {
		var (
			val   interface{}
			slice reflect.Value
		)
		val, err = srf(rows)
		if err != nil {
			return
		}
		slice = reflect.Append(reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(val)), 0, InitialCapacity), reflect.ValueOf(val))

		for rows.Next() {
			val, err = srf(rows)
			if err != nil {
				return
			}
			slice = reflect.Append(slice, reflect.ValueOf(val))
		}
		ret = slice.Interface()
	}
	return
}

func (m *MysqlImpl) ScanPage(psql string, srf ScanRowFunc, offset int, limit int, sort string, desc bool, args ...interface{}) (tot int, ret interface{}, err error) {

	aln := len(args)

	meta := GetSqlMeta(psql)

	// 查询记录
	dataPsql := GenDataSql(psql, meta, sort, desc)
	if limit <= 0 {
		limit = math.MaxInt32
	}
	args = append(args, offset, limit)

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(dataPsql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	var dlen int
	if rows.Next() {
		var (
			val   interface{}
			slice reflect.Value
		)
		val, err = srf(rows)
		if err != nil {
			return
		}
		slice = reflect.Append(reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(val)), 0, InitialCapacity), reflect.ValueOf(val))

		for rows.Next() {
			val, err = srf(rows)
			if err != nil {
				return
			}
			slice = reflect.Append(slice, reflect.ValueOf(val))
		}
		ret = slice.Interface()
		dlen = slice.Len()
	}
	if dlen == 0 && offset == 0 {
		tot = 0
	} else if dlen > 0 && dlen < limit {
		tot = offset + dlen
	} else {
		tot, err = m.scanPageTotal(psql, meta, args[0:aln]...)
	}

	return
}

func (m *MysqlImpl) scanPageTotal(psql string, meta *SqlMeta, args ...interface{}) (ret int, err error) {
	// 查询总数
	if meta.TotalPsql == "" {
		GenTotalSql(psql, meta)
	}

	// Kingshared禁止预编译与事务
	rows, err := m.DB.Query(meta.TotalPsql, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&ret)
	}

	return
}

func (m *MysqlImpl) Exec(psql string, args ...interface{}) (ret sql.Result, err error) {
	ret, err = m.DB.Exec(psql, args...)
	return
}

func (m *MysqlImpl) ExecBatch(psql string, argsList ...interface{}) (retList []sql.Result, err error) {
	tx, err := m.DB.Begin()
	if err != nil {
		return
	}

	retList = make([]sql.Result, len(argsList))
	var ret sql.Result
	for i, args := range argsList {
		switch args := args.(type) {
		case []interface{}:
			ret, err = tx.Exec(psql, args...)
		default:
			ret, err = tx.Exec(psql, args)
		}
		if err != nil {
			tx.Rollback()
			return
		}
		retList[i] = ret
	}
	err = tx.Commit()
	return
}
