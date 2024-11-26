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
	Path      string   `arg:"-p" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables    []string `arg:"-t" help:"Optional space separated list of table names"`
	Stations  []string `arg:"-s" help:"Optional space separated list of stations IDs"`
	Elements  []string `arg:"-e" help:"Optional space separated list of element codes"`
	Overwrite bool     `help:"Overwrite any existing dumped files"`
	MaxConn   int      `arg:"-n" default:"4" help:"Max number of concurrent connections allowed to KDVH"`
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
