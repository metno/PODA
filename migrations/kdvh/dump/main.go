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

type DumpConfig struct {
	BaseDir   string   `short:"p" long:"path" default:"./dumps/kdvh" description:"Location the dumped data will be stored in"`
	Tables    []string `short:"t" delimiter:"," long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations  []string `short:"s" delimiter:"," long:"stnr" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements  []string `short:"e" delimiter:"," long:"elem" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Overwrite bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Email     []string `long:"email" delimiter:"," description:"Optional comma separated list of email addresses used to notify if the program crashed"`
	MaxConn   int      `long:"conns" default:"10" description:"Max number of concurrent connections allowed to KDVH"`
}

func (config *DumpConfig) Execute([]string) error {
	pool, err := pgxpool.New(context.Background(), os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error(err.Error())
		return nil
	}

	kdvh := db.Init()
	for _, table := range kdvh.Tables {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		utils.SetLogFile(table.TableName, "dump")
		DumpTable(table, pool, config)
	}

	return nil
}
