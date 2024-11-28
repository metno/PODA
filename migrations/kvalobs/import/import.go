package port

import (
	"fmt"
	"log"
	"log/slog"
	"migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ImportTable[S db.DataSeries | db.TextSeries](table db.Table[S], permits *lard.PermitMaps, pool *pgxpool.Pool, config *Config) (int64, error) {
	stations, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}
	fmt.Println(stations)

	var rowsInserted int64
	for _, station := range stations {
		stationDir := filepath.Join(table.Path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
			continue
		}

		var wg sync.WaitGroup

		bar := utils.NewBar(len(labels), station.Name())
		for _, file := range labels {
			bar.Add(1)

			label, err := db.LabelFromFilename(file.Name())
			if err != nil {
				slog.Error(err.Error())
				continue
			}

			if !config.ShouldProcessLabel(label) {
				continue
			}

			labelStr := label.ToString()

			// Check if data for this station/element is restricted
			if !permits.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
				// TODO: eventually use this to choose which table to use on insert
				slog.Warn(labelStr + "Timeseries data is restricted")
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				// FIXME: FromTime can be nil and anyway config.FromTime is wrong here!
				lardLabel := lard.Label(*label)
				// tsid, err := lard.GetTimeseriesID(&lardLabel, config.FromTime.Inner(), pool)
				tsid, err := lard.GetTimeseriesID(&lardLabel, time.Now(), pool)
				if err != nil {
					slog.Error(err.Error())
					return
				}

				ts, flags, err := table.ReadCSV(tsid, filepath.Join(stationDir, file.Name()))
				if err != nil {
					slog.Error(err.Error())
					return
				}

				count, err := table.Import(ts, pool, labelStr)
				if err != nil {
					slog.Error("Failed bulk insertion: " + err.Error())
					return
				}

				if err := lard.InsertFlags(flags, pool, labelStr); err != nil {
					slog.Error(labelStr + "failed flag bulk insertion - " + err.Error())
				}

				rowsInserted += count
			}()
		}
		wg.Wait()
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", table.Path, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)

	log.SetOutput(os.Stdout)
	return rowsInserted, nil
}

// TODO: here we trust that kvalobs and stinfosys have the same
// non scalar parameters, which might not be the case
func ImportDB(database db.DB, permits *lard.PermitMaps, pool *pgxpool.Pool, config *Config) {
	path := filepath.Join(config.Path, database.Name)

	if config.ChosenTable(db.DATA_TABLE_NAME) {
		table := DataTable(path)
		utils.SetLogFile(table.Path, "import")

		ImportTable(table, permits, pool, config)
	}

	if config.ChosenTable(db.TEXT_TABLE_NAME) {
		table := TextTable(path)
		utils.SetLogFile(table.Path, "import")

		ImportTable(table, permits, pool, config)
	}
}
