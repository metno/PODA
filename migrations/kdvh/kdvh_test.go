package kdvh

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
	port "migrate/kdvh/import"
	"migrate/kdvh/import/cache"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"

type ImportTest struct {
	table        string
	station      int32
	elem         string
	expectedRows int64
}

func (t *ImportTest) mockConfig() (*port.Config, *cache.Cache) {
	return &port.Config{
			Tables:    []string{t.table},
			Stations:  []string{fmt.Sprint(t.station)},
			Elements:  []string{t.elem},
			BaseDir:   "./tests",
			HasHeader: true,
			Sep:       ";",
		},
		&cache.Cache{
			Stinfo: cache.StinfoMap{
				{ElemCode: t.elem, TableName: t.table}: {
					Fromtime: time.Date(2001, 7, 1, 9, 0, 0, 0, time.UTC),
					IsScalar: true,
				},
			},
		}
}

func TestImportKDVH(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	testCases := []ImportTest{
		{table: "T_MDATA", station: 12345, elem: "TA", expectedRows: 2644},
	}

	// TODO: test does not fail, if flags are not inserted
	// TODO: bar does not work well with log print outs
	for _, c := range testCases {
		config, cache := c.mockConfig()

		table, ok := db.KDVH[c.table]
		if !ok {
			t.Fatal("Table does not exist in database")
		}

		insertedRows := port.ImportTable(table, cache, pool, config)
		if insertedRows != c.expectedRows {
			t.Fail()
		}
	}
}
