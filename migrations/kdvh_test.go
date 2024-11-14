package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"

func mockConfig(t *ImportTest) *kdvh.ImportConfig {
	return &kdvh.ImportConfig{
		Tables:    []string{t.table},
		Stations:  []string{fmt.Sprint(t.station)},
		Elements:  []string{t.elem},
		BaseDir:   "./tests",
		HasHeader: true,
		Sep:       ";",
		StinfoMap: map[kdvh.StinfoKey]kdvh.StinfoParam{
			{ElemCode: t.elem, TableName: t.table}: {
				TypeID:   501,
				ParamID:  212,
				Hlevel:   nil,
				Sensor:   0,
				Fromtime: time.Date(2001, 7, 1, 9, 0, 0, 0, time.UTC),
				IsScalar: true,
			},
		},
	}
}

type ImportTest struct {
	table        string
	station      int32
	elem         string
	expectedRows int64
}

func TestImportKDVH(t *testing.T) {
	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	testCases := []ImportTest{
		{table: "T_MDATA", station: 12345, elem: "TA", expectedRows: 2644},
	}

	for _, c := range testCases {
		config := mockConfig(&c)
		table, ok := kdvh.KDVH[c.table]
		if !ok {
			t.Fatal("Table does not exist in database")
		}

		insertedRows := table.Import(pool, config)
		if insertedRows != c.expectedRows {
			t.Fail()
		}
	}
}
