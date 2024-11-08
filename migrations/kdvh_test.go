package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	// "github.com/rickb777/period"

	"migrate/kdvh"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"

func mockConfig(t *ImportTest) *kdvh.ImportConfig {
	config := kdvh.ImportConfig{
		Tables:    []string{t.table},
		Stations:  []string{fmt.Sprint(t.station)},
		Elements:  []string{t.elem},
		BaseDir:   "./tests",
		HasHeader: true,
		Sep:       ";",
	}

	config.CacheMetadata()
	return &config

}

type ImportTest struct {
	table        string
	station      int32
	elem         string
	expectedRows int64
}

func TestImportKDVH(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	// TODO: could also define a smaller version just for tests
	db := kdvh.KDVH

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
		table, ok := db[c.table]
		if !ok {
			t.Fatal("Table does not exist in database")
		}

		insertedRows := table.Import(pool, config)
		if insertedRows != c.expectedRows {
			t.Fail()
		}
	}
}
