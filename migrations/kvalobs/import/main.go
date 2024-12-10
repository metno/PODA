package port

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
	"migrate/kvalobs/import/cache"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	kvalobs.BaseConfig
	Reindex bool `help:"Drop PG indices before insertion. Might improve performance"`
}

func (config *Config) Execute() error {
	dbs := kvalobs.InitDBs()
	// Only cache from histkvalobs?
	cache := cache.New(dbs["histkvalobs"])

	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	if config.Reindex {
		utils.DropIndices(pool)
	}

	// Recreate indices even in case the main function panics
	defer func() {
		r := recover()
		if config.Reindex {
			utils.CreateIndices(pool)
		}

		if r != nil {
			panic(r)
		}
	}()

	for name, db := range dbs {
		if !utils.IsEmptyOrEqual(config.Database, name) {
			continue
		}
		ImportDB(db, cache, pool, config)

	}

	return nil
}
