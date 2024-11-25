package dump

import (
	"log/slog"
	"migrate/lard"
	"os"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Function used to du
type LabelDumpFunc = func(pool *pgxpool.Pool, config *Config) ([]*lard.Label, error)

func dumpLabels(pool *pgxpool.Pool, path string, fn LabelDumpFunc, config *Config) ([]*lard.Label, error) {
	labels, err := fn(pool, config)
	if err != nil {
		// Error logged inside fn
		return nil, err
	}

	file, err := os.Create(path)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	slog.Info("Writing timeseries labels to " + path)
	if err = gocsv.Marshal(labels, file); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}
