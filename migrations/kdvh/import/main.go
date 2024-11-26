package port

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
	"migrate/kdvh/import/cache"
	"migrate/utils"
)

type Config struct {
	Verbose   bool     `arg:"-v" help:"Increase verbosity level"`
	BaseDir   string   `arg:"-p,--path" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables    []string `arg:"-t" help:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations  []string `arg:"-s" help:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements  []string `arg:"-e" help:"Optional comma separated list of element codes. By default all element codes are processed"`
	Sep       string   `default:"," help:"Separator character in the dumped files. Needs to be quoted"`
	HasHeader bool     `help:"Add this flag if the dumped files have a header row"`
	// TODO: this isn't implemented in go-arg
	// Skip      string   `choice:"data" choice:"flags" help:"Skip import of data or flags"`
	Email   []string `help:"Optional comma separated list of email addresses used to notify if the program crashed"`
	Reindex bool     `help:"Drops PG indices before insertion. Might improve performance"`
}

func (config *Config) Execute() {
	if len(config.Sep) > 1 {
		fmt.Printf("Error: '--sep' only accepts single-byte characters. Got %s", config.Sep)
		os.Exit(1)
	}

	slog.Info("Import started!")
	kdvh := db.Init()

	// Cache metadata from Stinfosys, KDVH, and local `product_offsets.csv`
	cache := cache.CacheMetadata(config.Tables, config.Stations, config.Elements, kdvh)

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return
	}
	defer pool.Close()

	if config.Reindex {
		dropIndices(pool)
	}

	// Recreate indices even in case the main function panics
	defer func() {
		r := recover()
		if config.Reindex {
			createIndices(pool)
		}

		if r != nil {
			panic(r)
		}
	}()

	for _, table := range kdvh.Tables {
		if len(config.Tables) > 0 && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		if !table.ShouldImport() {
			if config.Verbose {
				slog.Info("Skipping import of " + table.TableName + " because this table is not set for import")
			}
			continue
		}

		utils.SetLogFile(table.TableName, "import")
		ImportTable(table, cache, pool, config)
	}

	log.SetOutput(os.Stdout)
	slog.Info("Import complete!")
}

func dropIndices(pool *pgxpool.Pool) {
	slog.Info("Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		panic(err.Error())
	}

	_, err = pool.Exec(context.Background(), string(file))
	if err != nil {
		panic(err.Error())
	}
}

func createIndices(pool *pgxpool.Pool) {
	slog.Info("Recreating table indices...")

	files := []string{"../db/public.sql", "../db/flags.sql"}
	for _, filename := range files {
		file, err := os.ReadFile(filename)
		if err != nil {
			panic(err.Error())
		}

		_, err = pool.Exec(context.Background(), string(file))
		if err != nil {
			panic(err.Error())
		}
	}
}
