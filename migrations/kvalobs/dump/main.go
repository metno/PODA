package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/lard"
	"migrate/utils"
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
	Path     string           `arg:"-p" default:"./dumps" help:"Location the dumped data will be stored in"`
	FromTime *utils.Timestamp `arg:"--from" help:"Fetch data only starting from this date-only timestamp"`
	ToTime   *utils.Timestamp `arg:"--to" help:"Fetch data only until this date-only timestamp"`
	// Ts       []int32    `long:"ts" help:"Optional comma separated list of timeseries. By default all available timeseries are processed"`
	Stations []int32 `help:"Optional space separated list of station numbers"`
	TypeIds  []int32 `help:"Optional space separated list of type IDs"`
	ParamIds []int32 `help:"Optional space separated list of param IDs"`
	Sensors  []int32 `help:"Optional space separated list of sensors"`
	Levels   []int32 `help:"Optional space separated list of levels"`
}

func (config *Config) ShouldDumpLabel(label *lard.Label) bool {
	// (config.Ts == nil || slices.Contains(config.Ts, ts.ID)) ||
	return utils.Contains(config.Stations, label.StationID) ||
		utils.Contains(config.TypeIds, label.TypeID) ||
		utils.Contains(config.ParamIds, label.ParamID) ||
		// TODO: these two should never be null anyway
		utils.NullableContains(config.Sensors, label.Sensor) ||
		utils.NullableContains(config.Levels, label.Level)
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
