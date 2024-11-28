package port

import (
	"context"

	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/lard"
)

type Config struct {
	db.BaseConfig[int32]
	Ts []int32 `help:"Optional space separated list of timeseries."`
}

func (config *Config) Execute() error {
	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	kvalobs, histkvalobs := db.InitDBs()

	if config.ChosenDB(kvalobs.Name) {
		// dumpDB(kvalobs, dataTable, textTable, config)
	}

	if config.ChosenDB(histkvalobs.Name) {
		// dumpDB(histkvalobs, dataTable, textTable, config)
	}

	return nil
}
