package dump

import (
	"context"
	"log/slog"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
	"migrate/utils"
)

type Config struct {
	BaseDir   string   `arg:"-p,--path" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables    []string `arg:"-t" help:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations  []string `arg:"-s" help:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements  []string `arg:"-e" help:"Optional comma separated list of element codes. By default all element codes are processed"`
	Overwrite bool     `help:"Overwrite any existing dumped files"`
	Email     []string `help:"Optional comma separated list of email addresses used to notify if the program crashed"`
	MaxConn   int      `arg:"-n,--conn" default:"4" help:"Max number of concurrent connections allowed to KDVH"`
}

func (config *Config) Execute() {
	pool, err := pgxpool.New(context.Background(), os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error(err.Error())
		return
	}

	kdvh := db.Init()
	for _, table := range kdvh.Tables {
		if len(config.Tables) > 0 && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		utils.SetLogFile(table.TableName, "dump")
		DumpTable(table, pool, config)
	}
}
