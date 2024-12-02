package dump

import (
	"context"
	"fmt"
	"log"
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

func readLabelCSV(filename string) (labels []*db.Label, err error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	defer file.Close()

	slog.Info("Reading previously dumped labels...")
	err = gocsv.Unmarshal(file, &labels)
	if err != nil {
		slog.Error(err.Error())
	}
	return labels, err
}

func writeLabelCSV(path string, labels []*db.Label) error {
	file, err := os.Create(path)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	slog.Info("Writing timeseries labels to " + path)
	err = gocsv.Marshal(labels, file)
	if err != nil {
		slog.Error(err.Error())
	}
	return err
}

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
		return labels, writeLabelCSV(labelFile, labels)
	}
	return readLabelCSV(labelFile)
}

func getStationLabelMap(labels []*db.Label) map[int32][]*db.Label {
	labelmap := make(map[int32][]*db.Label)

	var station int32
	for _, label := range labels {
		if station != label.StationID {
			station = label.StationID
		}
		labelmap[station] = append(labelmap[station], label)
	}

	return labelmap
}

func dumpTable[S db.DataSeries | db.TextSeries](table db.Table[S], pool *pgxpool.Pool, config *Config) {
	utils.SetLogFile(table.Path, "dump")
	fmt.Printf("Dumping to %q...\n", table.Path)
	defer func() {
		fmt.Println(strings.Repeat("- ", 50))
		log.SetOutput(os.Stdout)
	}()

	labels, err := getLabels(table, pool, config)
	if err != nil {
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
