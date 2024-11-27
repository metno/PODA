package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

func writeLabels[T int32 | string](path string, labels []*db.Label[T]) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	slog.Info("Writing timeseries labels to " + path)
	if err = gocsv.Marshal(labels, file); err != nil {
		return err
	}

	return nil
}

func writeSeries[T int32 | string, S db.DataSeries | db.TextSeries](series S, path, table string, label *db.Label[T]) error {
	filename := filepath.Join(path, label.ToFilename())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	slog.Info(fmt.Sprintf("Writing %s observations to '%s'", table, filename))
	if err = gocsv.MarshalFile(series, file); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

// TODO: switch to log file
func dumpTable[S db.DataSeries | db.TextSeries](path string, table Table[string, S], pool *pgxpool.Pool, config *Config) {
	var labels []*db.Label[string]

	timespan := config.TimeSpan()

	labelFile := filepath.Join(path, table.Name+"_labels.csv")
	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		labels, err = table.LabelFn(timespan, pool)
		if err != nil {
			slog.Error(err.Error())
			return
		}
		if err = writeLabels(labelFile, labels); err != nil {
			slog.Error(err.Error())
			return
		}
	} else {
		if labels, err = db.ReadLabelCSV(labelFile); err != nil {
			slog.Error(err.Error())
			return
		}
	}

	// TODO: this bar is a bit deceiving if you don't dump all the labels
	bar := utils.NewBar(len(labels), path)

	path = filepath.Join(path, table.Name)
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	for _, label := range labels {
		bar.Add(1)

		if !config.ShouldProcessLabel(label) {
			continue
		}

		series, err := table.ObsFn(label, timespan, pool)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := writeSeries(series, path, table.Name, label); err != nil {
			slog.Error(err.Error())
			continue
		}

		slog.Info(label.ToString() + ": dumped successfully")
	}
}

func dumpDB(database DB, dataTable Table[string, db.DataSeries], textTable Table[string, db.TextSeries], config *Config) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(database.ConnEnvVar))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
		return
	}
	defer pool.Close()

	path := filepath.Join(config.Path, database.Name)
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	if config.ChosenTable(dataTable.Name) {
		dumpTable(path, dataTable, pool, config)
	}

	if config.ChosenTable(textTable.Name) {
		dumpTable(path, textTable, pool, config)
	}
}
