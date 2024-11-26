package port

import (
	"context"

	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	BaseDir  string     `arg:"-p,--path" default:"./dumps" help:"Location the dumped data will be stored in"`
	FromTime *time.Time `arg:"--from" help:"Fetch data only starting from this timestamp"`
	ToTime   *time.Time `arg:"--to" help:"Fetch data only until this timestamp"`
	Ts       []int32    `help:"Optional comma separated list of timeseries. By default all available timeseries are processed"`
	Stations []int32    `help:"Optional comma separated list of station numbers. By default all available station numbers are processed"`
	TypeIds  []int32    `help:"Optional comma separated list of type IDs. By default all available type IDs are processed"`
	ParamIds []int32    `help:"Optional comma separated list of param IDs. By default all available param IDs are processed"`
	Sensors  []int32    `help:"Optional comma separated list of sensors. By default all available sensors are processed"`
	Levels   []int32    `help:"Optional comma separated list of levels. By default all available levels are processed"`
}

func (config *Config) ShouldImport(ts *lard.Label) bool {
	// TODO: there's no need to get the tsid if the other parameters don't match
	// So extract the first condition
	// return contains(config.Ts, tsid) ||
	return utils.Contains(config.Stations, ts.StationID) ||
		utils.Contains(config.TypeIds, ts.TypeID) ||
		utils.Contains(config.ParamIds, ts.ParamID) ||
		// TODO: these two should never be null anyway
		utils.NullableContains(config.Sensors, ts.Sensor) ||
		utils.NullableContains(config.Levels, ts.Level)
}

func (config *Config) Execute() error {
	pool, err := pgxpool.New(context.Background(), os.Getenv("KVALOBS_CONN_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	return nil
}
