package port

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

func ImportTable[S db.DataSeries | db.TextSeries](table db.Table[S], permits *lard.PermitMaps, pool *pgxpool.Pool, config *Config) (int64, error) {
	stations, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	var rowsInserted int64
	for _, station := range stations {
		stnr, err := strconv.ParseInt(station.Name(), 10, 32)
		if err != nil || !utils.IsEmptyOrContains(config.Stations, int32(stnr)) {
			continue
		}

		stationDir := filepath.Join(table.Path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
			continue
		}

		bar := utils.NewBar(len(labels), stationDir)
		var wg sync.WaitGroup
		for _, file := range labels {
			// TODO: only add if label was processed?
			bar.Add(1)

			label, err := db.LabelFromFilename(file.Name())
			if err != nil {
				slog.Error(err.Error())
				continue
			}

			if !config.ShouldProcessLabel(label) {
				continue
			}

			labelStr := label.LogStr()

			// Check if data for this station/element is restricted
			if !permits.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
				// TODO: eventually use this to choose which table to use on insert
				slog.Warn(labelStr + "timeseries data is restricted, skipping")
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				lardLabel := lard.Label(*label)
				// TODO: figure out if we should convert (0, 0) to (NULL, NULL) for sensor, level
				// TODO: figure out where to get fromtime, kvalobs directly? Stinfosys?
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
