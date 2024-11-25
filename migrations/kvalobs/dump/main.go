package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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
// TODO: not sure what to do with this one
// func joinTS(first, second []lard.Label)

type Config struct {
	BaseDir  string     `short:"p" long:"path" default:"./dumps" description:"Location the dumped data will be stored in"`
	FromTime *time.Time `long:"from" description:"Fetch data only starting from this timestamp"`
	ToTime   *time.Time `long:"to" description:"Fetch data only until this timestamp"`
	// Ts       []int32    `long:"ts" description:"Optional comma separated list of timeseries. By default all available timeseries are processed"`
	Stations []int32 `long:"station" description:"Optional comma separated list of station numbers. By default all available station numbers are processed"`
	TypeIds  []int32 `long:"typeid" description:"Optional comma separated list of type IDs. By default all available type IDs are processed"`
	ParamIds []int32 `long:"paramid" description:"Optional comma separated list of param IDs. By default all available param IDs are processed"`
	Sensors  []int32 `long:"sensor" description:"Optional comma separated list of sensors. By default all available sensors are processed"`
	Levels   []int32 `long:"level" description:"Optional comma separated list of levels. By default all available levels are processed"`
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

func (config *Config) Execute(_ []string) error {
	// dump kvalobs
	config.dump("KVALOBS_CONN_STRING", filepath.Join(config.BaseDir, "kvalobs"))

	// dump histkvalobs
	// TODO: maybe it's worth adding a separate flag?
	config.dump("HISTKVALOBS_CONN_STRING", filepath.Join(config.BaseDir, "histkvalobs"))

	return nil
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
