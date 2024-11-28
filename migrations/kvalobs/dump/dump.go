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

func writeLabels(path string, labels []*db.KvLabel) error {
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

func writeSeries[S db.DataSeries | db.TextSeries](series S, path string, label *db.KvLabel) error {
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

func dumpTable[S db.DataSeries | db.TextSeries](path string, table Table[S], pool *pgxpool.Pool, config *Config) {
	var labels []*db.KvLabel

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

	path = filepath.Join(path, table.Name)
	utils.SetLogFile(path, "dump")

	// TODO: this bar is a bit deceiving if you don't dump all the labels
	// Maybe should only cache the ones requested from cli?
	bar := utils.NewBar(len(labels), path)

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	var stationPath string
	for _, label := range labels {
		bar.Add(1)

		if !config.ShouldProcessLabel(label) {
			continue
		}

		thisPath := filepath.Join(path, fmt.Sprint(label.StationID))
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

			series, err := table.ObsFn(label, timespan, pool)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			if err := writeSeries(series, stationPath, label); err != nil {
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

	dataTable := Table[db.DataSeries]{
		Name:    db.DATA_TABLE_NAME,
		LabelFn: getDataLabels,
		ObsFn:   getDataSeries,
	}

	textTable := Table[db.TextSeries]{
		Name:    db.TEXT_TABLE_NAME,
		LabelFn: getTextLabels,
		ObsFn:   getTextSeries,
	}

	if config.ChosenTable(dataTable.Name) {
		dumpTable(path, dataTable, pool, config)
	}

	if config.ChosenTable(textTable.Name) {
		dumpTable(path, textTable, pool, config)
	}
}
