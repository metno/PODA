package main

import (
	"context"
	"log"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	port "migrate/kvalobs/import"
	"migrate/lard"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"
const DUMPS_PATH string = "./files"

type KvalobsTestCase struct {
	db           db.DB
	station      int32
	paramid      int32
	typeid       int32
	sensor       *int32
	level        *int32
	permit       int32
	expectedRows int64
}

func (t *KvalobsTestCase) mockConfig() (*port.Config, *lard.PermitMaps) {
	return &port.Config{
			BaseConfig: db.BaseConfig[int32]{
				Stations: []int32{t.station},
			},
		}, &lard.PermitMaps{
			StationPermits: lard.StationPermitMap{
				t.station: t.permit,
			},
		}
}

type KvalobsDataCase struct {
	KvalobsTestCase
	table db.DataTable
}

func DataCase(ktc KvalobsTestCase) KvalobsDataCase {
	path := filepath.Join(DUMPS_PATH, ktc.db.Name)
	return KvalobsDataCase{ktc, port.DataTable(path)}
}

func TestImportDataKvalobs(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	_, histkvalobs := db.InitDBs()

	cases := []KvalobsDataCase{
		DataCase(KvalobsTestCase{db: histkvalobs, station: 18700, permit: 1, expectedRows: 100}),
		// DataCase(KvalobsTestCase{db: histkvalobs, station: 18700, permit: 0, expectedRows: 100}),
	}

	for _, c := range cases {
		config, permits := c.mockConfig()
		insertedRows, err := port.ImportTable(c.table, permits, pool, config)

		switch {
		case err != nil:
			t.Fatal(err)
		case insertedRows != c.expectedRows:
			t.Log(insertedRows)
			// t.Fail()
		}
	}
}

// type KvalobsTextCase struct {
// 	KvalobsTestCase
// 	table db.TextTable
// }
//
// func TextCase(ktc KvalobsTestCase) KvalobsTextCase {
// 	path := filepath.Join(DUMPS_PATH, ktc.db.Name)
// 	return KvalobsTextCase{ktc, port.TextTable(path)}
// }
//
// func TestImportTextKvalobs(t *testing.T) {
// 	log.SetFlags(log.LstdFlags | log.Lshortfile)
//
// 	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
// 	if err != nil {
// 		t.Log("Could not connect to Lard:", err)
// 	}
// 	defer pool.Close()
//
// 	kvalobs, histkvalobs := db.InitDBs()
//
// 	cases := []KvalobsTextCase{
// 		TextCase(KvalobsTestCase{db: kvalobs, station: 18700, paramid: 212, permit: 0, expectedRows: 100}),
// 		TextCase(KvalobsTestCase{db: histkvalobs, station: 18700, paramid: 212, permit: 0, expectedRows: 100}),
// 	}
//
// 	for _, c := range cases {
// 		config, permits := c.mockConfig()
// 		insertedRows, err := port.ImportTable(c.table, permits, pool, config)
//
// 		switch {
// 		case err != nil:
// 			t.Fatal(err)
// 		case insertedRows != c.expectedRows:
// 			t.Fail()
// 		}
// 	}
// }
