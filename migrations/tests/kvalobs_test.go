package tests

import (
	"context"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
	port "migrate/kvalobs/import"
	"migrate/kvalobs/import/cache"
	"migrate/stinfosys"
	"migrate/utils"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"
const DUMPS_PATH string = "./files"

type KvalobsTestCase struct {
	db           kvalobs.DB
	table        kvalobs.Table
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
			BaseConfig: kvalobs.BaseConfig{
				Stations: []int32{t.station},
			},
		},
		&cache.Cache{
			Meta: map[cache.MetaKey]utils.TimeSpan{
				{Stationid: t.station}: {From: &fromtime},
			},
			Permits: stinfosys.PermitMaps{
				StationPermits: stinfosys.StationPermitMap{
					t.station: t.permit,
				},
			},
		}
}

func TestImportDataKvalobs(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	prod, hist := kvalobs.InitDBs()
	prod.Path = filepath.Join(DUMPS_PATH, prod.Name)
	hist.Path = filepath.Join(DUMPS_PATH, hist.Name)

	cases := []KvalobsTestCase{
		{
			db:           hist,
			table:        port.DataTable(hist.Path),
			station:      18700,
			paramid:      313,
			permit:       1,
			expectedRows: 39,
		},
		{
			db:           prod,
			table:        port.TextTable(prod.Path),
			station:      18700,
			permit:       1,
			expectedRows: 182,
		},
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
