package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

func writeSeriesCSV[S db.DataSeries | db.TextSeries](series S, path string, label *db.Label) error {
	filename := filepath.Join(path, label.ToFilename())
	file, err := os.Create(filename)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	// Write number of lines on first line, keep headers on 2nd line
	file.Write([]byte(fmt.Sprintf("%v\n", len(series))))
	if err = gocsv.Marshal(series, file); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func getLabels[S db.DataSeries | db.TextSeries](table db.Table[S], pool *pgxpool.Pool, config *Config) (labels []*db.Label, err error) {
	labelFile := table.Path + "_labels.csv"

	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		labels, err = table.DumpLabels(config.TimeSpan(), pool)
		if err != nil {
			return nil, err
		}
		return labels, db.WriteLabelCSV(labelFile, labels)
	}
	return db.ReadLabelCSV(labelFile)
}

func getStationLabelMap(labels []*db.Label) map[int32][]*db.Label {
	labelmap := make(map[int32][]*db.Label)

	for _, label := range labels {
		labelmap[label.StationID] = append(labelmap[label.StationID], label)
	}

	return labelmap
}

func dumpTable[S db.DataSeries | db.TextSeries](table db.Table[S], pool *pgxpool.Pool, config *Config) {
	if !config.LabelsOnly {
		utils.SetLogFile(table.Path, "dump")
	}
	fmt.Printf("Dumping to %q...\n", table.Path)
	defer fmt.Println(strings.Repeat("- ", 40))

	labels, err := getLabels(table, pool, config)
	if err != nil || config.LabelsOnly {
		return
	}

	stationMap := getStationLabelMap(labels)
	timespan := config.TimeSpan()

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	for station, labels := range stationMap {
		stationPath := filepath.Join(table.Path, fmt.Sprint(station))

		if !utils.IsEmptyOrContains(config.Stations, station) {
			continue
		}

		if err := os.MkdirAll(stationPath, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		// TODO: this bar is a bit deceiving if you don't dump all the labels
		// Maybe should only cache the ones requested from cli?
		bar := utils.NewBar(len(labels), fmt.Sprintf("%10d", station))
		bar.RenderBlank()

		for _, label := range labels {
			wg.Add(1)
			semaphore <- struct{}{}

			go func() {
				defer func() {
					bar.Add(1)
					wg.Done()
					// Release semaphore
					<-semaphore
				}()

				if !config.ShouldProcessLabel(label) {
					return
				}

				series, err := table.DumpSeries(label, timespan, pool)
				if err != nil {
					return
				}

				if err := writeSeriesCSV(series, stationPath, label); err != nil {
					return
				}

				slog.Info(label.LogStr() + "dumped successfully")
			}()
		}
		wg.Wait()
	}
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

	if utils.IsEmptyOrEqual(config.Table, db.DATA_TABLE_NAME) {
		table := DataTable(path)
		dumpTable(table, pool, config)
	}

	if utils.IsEmptyOrEqual(config.Table, db.TEXT_TABLE_NAME) {
		table := TextTable(path)
		dumpTable(table, pool, config)
	}
}
