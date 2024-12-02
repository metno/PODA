package port

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/kvalobs/import/cache"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	db.BaseConfig
}

func (config *Config) Execute() error {
	kvalobs, histkvalobs := db.InitDBs()
	cache := cache.New(kvalobs)

	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	if utils.IsEmptyOrEqual(config.Database, kvalobs.Name) {
		ImportDB(kvalobs, cache, pool, config)
	}

	if utils.IsEmptyOrEqual(config.Database, histkvalobs.Name) {
		ImportDB(histkvalobs, cache, pool, config)
	}

	return nil
}
