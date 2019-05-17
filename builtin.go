package mysql

import (
	"database/sql"
	"fmt"
	"time"
)

type PType uint

const (
	Bool PType = iota
	Int
	Int32
	Int64
	Float32
	Float64
	String
	Time
	Bytes
)

func Newp(v PType) interface{} {
	switch v {
	case Bool:
		ret := (*bool)(nil)
		return &ret
	case Int:
		ret := (*int)(nil)
		return &ret
	case Int32:
		ret := (*int32)(nil)
		return &ret
	case Int64:
		ret := (*int64)(nil)
		return &ret
	case Float32:
		ret := (*float32)(nil)
		return &ret
	case Float64:
		ret := (*float64)(nil)
		return &ret
	case String:
		ret := (*string)(nil)
		return &ret
	case Time:
		ret := (*time.Time)(nil)
		return &ret
	case Bytes:
		ret := (*[]byte)(nil)
		return &ret
	default:
		panic(fmt.Errorf("newp failed for: %#v", v))
	}
}

func Extv(v interface{}) interface{} {
	switch v := v.(type) {
	case **bool:
		return *v
	case **int:
		return *v
	case **int32:
		return *v
	case **int64:
		return *v
	case **float32:
		return *v
	case **float64:
		return *v
	case **string:
		return *v
	case **time.Time:
		return *v
	case *[]byte:
		return *v
	case *interface{}:
		return *v
	default:
		panic(fmt.Errorf("extv failed for: %#v", v))
	}
}

func BoolRow(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(bool)
	err := rows.Scan(&ret)
	return ret, err
}

func IntRow(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(int)
	err := rows.Scan(&ret)
	return ret, err
}

func Int32Row(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(int32)
	err := rows.Scan(&ret)
	return ret, err
}

func Int64Row(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(int64)
	err := rows.Scan(&ret)
	return ret, err
}

func Float32Row(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(float32)
	err := rows.Scan(&ret)
	return ret, err
}

func Float64Row(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(float64)
	err := rows.Scan(&ret)
	return ret, err
}

func StringRow(rows *sql.Rows, cache *[]interface{}) (interface{}, error) {
	ret := new(string)
	err := rows.Scan(&ret)
	return ret, err
}

func TimeRow(rows *sql.Rows) (interface{}, error) {
	ret := new(time.Time)
	err := rows.Scan(&ret)
	return ret, err
}

func SliceRow(ks ...PType) ScanRowFunc {
	// 下述在整个扫描
	ln := len(ks)
	return func(rows *sql.Rows) (interface{}, error) {
		ret := make([]interface{}, ln)
		for i, k := range ks {
			ret[i] = Newp(k)
		}
		err := rows.Scan(ret...)
		if err != nil {
			return nil, err
		}
		for i := 0; i < ln; i++ {
			ret[i] = Extv(ret[i])
		}
		return ret, nil
	}
}

/*name1,type1,name2,type2...*/
func MapRow(pairs ...interface{}) ScanRowFunc {
	pln := len(pairs)
	len := pln / 2

	ks := make([]string, len)
	ts := make([]PType, len)

	idx := 0
	for i := 1; i < pln; i += 2 {
		ks[idx] = pairs[i-1].(string)
		ts[idx] = pairs[i].(PType)
		idx++
	}

	return func(rows *sql.Rows) (interface{}, error) {
		vs := make([]interface{}, len)
		for i, t := range ts {
			vs[i] = Newp(t)
		}
		err := rows.Scan(vs...)
		if err != nil {
			return nil, err
		}

		ret := make(map[string]interface{}, len)
		for i, k := range ks {
			ret[k] = Extv(vs[i])
		}
		return ret, nil
	}
}
