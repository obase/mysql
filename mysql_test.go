package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

type Rec struct {
	Time   *time.Time
	Int    *int
	String *string
}

func TestScan(t *testing.T) {
	demo := Get("demo")
	ret, err := demo.ScanOne("select 123", Int32R)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
}

func TestScanOne(t *testing.T) {
	demo := Get("demo")
	ret, err := demo.ScanOne("select now(),123,'abc'", func(row *sql.Rows) (interface{}, error) {
		rec := new(Rec)
		if err := row.Scan(&rec.Time, &rec.Int, &rec.String); err != nil {
			return nil, err
		}
		return rec, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v\n", ret.(*Rec))
}

func TestScanOne2(t *testing.T) {
	demo := Get("demo")
	var rec Rec
	ok, err := demo.ScanOne2("select now(),123,'abc' from t1 where 1> 2", []interface{}{&rec.Time, &rec.Int, &rec.String})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		fmt.Printf("not existed")
	}
	fmt.Printf("%v\n", rec)
}

func TestBeginTx(t *testing.T) {
	demo := Get("demo")
	tx, err := demo.BeginTx(context.Background())

	rt, err := tx.ExecBatch("insert into t1(name) values(?)", "abc", "jason", "woh")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(rt)
	tx.Commit()
}
