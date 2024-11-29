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
	db.BaseConfig
}

func (config *Config) Execute() error {
	permits := lard.NewPermitTables()

	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	kvalobs, histkvalobs := db.InitDBs()

	if config.ChosenDB(kvalobs.Name) {
		ImportDB(kvalobs, permits, pool, config)
	}

	if config.ChosenDB(histkvalobs.Name) {
		ImportDB(histkvalobs, permits, pool, config)
	}

	return nil
}
