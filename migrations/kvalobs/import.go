package kvalobs

import (
	"context"
	"fmt"
	"log/slog"
	"migrate/lard"
	"os"
	"time"

	// "path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ImportConfig struct {
	BaseConfig
}

func (config *ImportConfig) Execute(_ []string) error {
	config.setup()

	pool, err := pgxpool.New(context.Background(), os.Getenv("KVALOBS_CONN_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	return nil
}

type TextTimeseries struct {
	id    int32
	obses []TextObs
}

func (ts *TextTimeseries) Len() int {
	return len(ts.obses)
}

func (ts *TextTimeseries) ID() int32 {
	return ts.id
}

func (ts *TextTimeseries) Obstime(i int) time.Time {
	return ts.obses[i].Obstime
}

func (ts *TextTimeseries) Text(i int) string {
	return ts.obses[i].Original
}

func (config *ImportConfig) ImportText(pool *pgxpool.Pool, path string) error {
	dir, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	var totalRowsInserted int64
	for _, file := range dir {
		label, err := LabelFromFilename(file.Name())
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		if !label.ShouldBeImported(config) {
			continue
		}

		// TODO: should use lard.Label directly?
		tsid, err := lard.GetTimeseriesID(lard.Label(label), *config.FromTime, pool)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		if !contains(config.Ts, tsid) {
			continue
		}

		data, err := readCSVfile[TextObs](file.Name())
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		// TODO: I probably need the interface don't I?
		ts := &TextTimeseries{tsid, data}
		count, err := lard.InsertNonscalarData(ts, pool, "")
		if err != nil {
			slog.Error("Failed bulk insertion: " + err.Error())
			continue
		}

		totalRowsInserted += count
	}

	return nil
}

func readDataFiles() []TSLabel {
	// TODO:
	return nil
}
