package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
)

// Same timeseries could be in both 'data' and 'text_data' tables
// First of all, why?
// Second, do we care?
// func readDataAndText(label *lard.Label, pool *pgxpool.Pool, config *DumpConfig) Data {
//     // Supposed to join text anf number data to single slice
//     return nil
// }
//
// TODO: not sure what to do with this one
// func joinTS(first, second []lard.Label)

type Config struct {
	db.BaseConfig
	UpdateLabels bool `help:"Overwrites the label CSV files"`
}

func (config *Config) Execute() {
	// dump kvalobs
	config.dump("KVALOBS_CONN_STRING", filepath.Join(config.Path, "kvalobs"))

	// dump histkvalobs
	// TODO: maybe it's worth adding a separate flag?
	config.dump("HISTKVALOBS_CONN_STRING", filepath.Join(config.Path, "histkvalobs"))
}

func (config *Config) dump(envvar, path string) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(envvar))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
		return
	}
	defer pool.Close()

	dumpText(path, pool, config)
	config.dumpData(path, pool)
}
