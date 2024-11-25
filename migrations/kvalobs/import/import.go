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
	BaseDir  string     `short:"p" long:"path" default:"./dumps" description:"Location the dumped data will be stored in"`
	FromTime *time.Time `long:"from" description:"Fetch data only starting from this timestamp"`
	ToTime   *time.Time `long:"to" description:"Fetch data only until this timestamp"`
	Ts       []int32    `long:"ts" description:"Optional comma separated list of timeseries. By default all available timeseries are processed"`
	Stations []int32    `long:"station" description:"Optional comma separated list of station numbers. By default all available station numbers are processed"`
	TypeIds  []int32    `long:"typeid" description:"Optional comma separated list of type IDs. By default all available type IDs are processed"`
	ParamIds []int32    `long:"paramid" description:"Optional comma separated list of param IDs. By default all available param IDs are processed"`
	Sensors  []int32    `long:"sensor" description:"Optional comma separated list of sensors. By default all available sensors are processed"`
	Levels   []int32    `long:"level" description:"Optional comma separated list of levels. By default all available levels are processed"`
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

func (config *Config) Execute(_ []string) error {
	pool, err := pgxpool.New(context.Background(), os.Getenv("KVALOBS_CONN_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	return nil
}
