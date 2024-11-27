package port

import (
	"context"

	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
)

type Config struct {
	db.BaseConfig
	Ts []int32 `help:"Optional space separated list of timeseries."`
}

func (config *Config) Execute() error {
	pool, err := pgxpool.New(context.Background(), os.Getenv("KVALOBS_CONN_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	return nil
}
