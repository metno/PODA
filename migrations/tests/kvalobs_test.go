package tests

import (
	"context"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	port "migrate/kvalobs/import"
	"migrate/kvalobs/import/cache"
	"migrate/lard"
	"migrate/utils"
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

func (t *KvalobsTestCase) mockConfig() (*port.Config, *cache.Cache) {
	fromtime, _ := time.Parse(time.DateOnly, "1900-01-01")
	return &port.Config{
			BaseConfig: db.BaseConfig{
				Stations: []int32{t.station},
			},
		},
		&cache.Cache{
			Meta: map[cache.MetaKey]utils.TimeSpan{
				{Stationid: t.station}: {From: &fromtime},
			},
			Permits: lard.PermitMaps{
				StationPermits: lard.StationPermitMap{
					t.station: t.permit,
				},
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
		DataCase(KvalobsTestCase{db: histkvalobs, station: 18700, paramid: 313, permit: 1, expectedRows: 39}),
	}

	for _, c := range cases {
		config, cache := c.mockConfig()
		insertedRows, err := port.ImportTable(c.table, cache, pool, config)

		switch {
		case err != nil:
			t.Fatal(err)
		case insertedRows != c.expectedRows:
			t.Fail()
		}
	}
}

type KvalobsTextCase struct {
	KvalobsTestCase
	table db.TextTable
}

func TextCase(ktc KvalobsTestCase) KvalobsTextCase {
	path := filepath.Join(DUMPS_PATH, ktc.db.Name)
	return KvalobsTextCase{ktc, port.TextTable(path)}
}

func TestImportTextKvalobs(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	kvalobs, _ := db.InitDBs()

	cases := []KvalobsTextCase{
		TextCase(KvalobsTestCase{db: kvalobs, station: 18700, permit: 1, expectedRows: 182}),
	}

	for _, c := range cases {
		config, cache := c.mockConfig()
		insertedRows, err := port.ImportTable(c.table, cache, pool, config)

		switch {
		case err != nil:
			t.Fatal(err)
		case insertedRows != c.expectedRows:
			t.Fail()
		}
	}
}
