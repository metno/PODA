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

// NOTE:
// - for both kvalobs and histkvalobs:
//      - all stinfo non-scalar params that can be found in Kvalobs are stored in `text_data`
//      - 305, 306, 307, 308 are also in `data` but should be treated as `text_data`
//          => should always use readDataCSV and lard.InsertData for these
// - only for histkvalobs
//      - 2751, 2752, 2753, 2754 are in `text_data` but should be treated as `data`?
//          => These are more complicated, but probably we should

type Config struct {
	kvalobs.BaseConfig
	Reindex bool `help:"Drop PG indices before insertion. Might improve performance"`
}

func (config *Config) Execute() error {
	prod, hist := kvalobs.InitDBs()
	cache := cache.New(prod)

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

	if utils.IsEmptyOrEqual(config.Database, prod.Name) {
		ImportDB(prod, cache, pool, config)
	}

	if utils.IsEmptyOrEqual(config.Database, hist.Name) {
		ImportDB(hist, cache, pool, config)
	}

	return nil
}
