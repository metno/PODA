package dump

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

func readLabelCSV(filename string) (labels []*db.KvLabel, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// TODO: maybe I should preallocate slice size if I can?
	err = gocsv.UnmarshalFile(file, &labels)
	return labels, err
}

func writeLabelCSV(path string, labels []*db.KvLabel) error {
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

// TODO: add number of rows as header row
func writeSeriesCSV[S db.DataSeries | db.TextSeries](series S, path string, label *db.KvLabel) error {
	filename := filepath.Join(path, label.ToFilename())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	if err = gocsv.MarshalFile(series, file); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func getLabels[T db.DataSeries | db.TextSeries](table db.Table[T], pool *pgxpool.Pool, config *Config) (labels []*db.KvLabel, err error) {
	labelFile := table.Path + "_labels.csv"

	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		labels, err = table.DumpLabels(config.TimeSpan(), pool)
		if err != nil {
			return nil, err
		}

		err = writeLabelCSV(labelFile, labels)
		return labels, err
	}

	return readLabelCSV(labelFile)
}

func dumpTable[S db.DataSeries | db.TextSeries](table db.Table[S], pool *pgxpool.Pool, config *Config) {
	var labels []*db.KvLabel

	labels, err := getLabels(table, pool, config)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	timespan := config.TimeSpan()
	utils.SetLogFile(table.Path, "dump")

	// TODO: this bar is a bit deceiving if you don't dump all the labels
	// Maybe should only cache the ones requested from cli?
	bar := utils.NewBar(len(labels), table.Path)

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	var stationPath string
	for _, label := range labels {
		bar.Add(1)

		if !config.ShouldProcessLabel(label) {
			continue
		}

		thisPath := filepath.Join(table.Path, fmt.Sprint(label.StationID))
		if thisPath != stationPath {
			stationPath = thisPath
			if err := os.MkdirAll(stationPath, os.ModePerm); err != nil {
				slog.Error(err.Error())
				continue
			}
		}

		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer func() {
				wg.Done()
				// Release semaphore
				<-semaphore
			}()

			series, err := table.DumpSeries(label, timespan, pool)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			if err := writeSeriesCSV(series, stationPath, label); err != nil {
				slog.Error(err.Error())
				return
			}

			slog.Info(label.ToString() + ": dumped successfully")
		}()
	}
	wg.Wait()

	log.SetOutput(os.Stdout)
}

func dumpDB(database db.DB, config *Config) {
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

	if config.ChosenTable(db.DATA_TABLE_NAME) {
		dumpTable(DataTable(path), pool, config)
	}

	if config.ChosenTable(db.TEXT_TABLE_NAME) {
		dumpTable(TextTable(path), pool, config)
	}
}
