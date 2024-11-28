package port

import (
	"log/slog"
	"migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func (config *Config) ImportText(pool *pgxpool.Pool, path string) error {
	dir, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	var totalRowsInserted int64
	for _, file := range dir {
		label, err := db.LabelFromFilename(file.Name())
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		if !config.ShouldProcessLabel(label) {
			continue
		}

		// FIXME: FromTime can be nil and anyway config.FromTime is wrong here!
		lardLabel := lard.Label(*label)
		tsid, err := lard.GetTimeseriesID(&lardLabel, config.FromTime.Inner(), pool)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		if !utils.IsEmptyOrContains(config.Ts, tsid) {
			continue
		}

		data, err := db.ReadTextCSV(tsid, file.Name())
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		count, err := lard.InsertTextData(data, pool, "")
		if err != nil {
			slog.Error("Failed bulk insertion: " + err.Error())
			continue
		}

		totalRowsInserted += count
	}

	return nil
}
