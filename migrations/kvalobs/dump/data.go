package dump

import (
	"context"
	"log/slog"
	"migrate/kvalobs/db"
	"migrate/lard"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DATA_LABEL_CSV string = "data_labels.csv"

func (config *Config) DumpData(outpath string, pool *pgxpool.Pool) {
	var labels []*lard.Label

	dataPath := filepath.Join(outpath, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	labelFile := filepath.Join(outpath, DATA_LABEL_CSV)
	if _, err := os.Stat(outpath); err != nil {
		if labels, err = dumpLabels(pool, labelFile, getDataLabels, config); err != nil {
			return
		}
	} else {
		if labels, err = db.ReadLabelCSV(labelFile); err != nil {
			return
		}
	}

	for _, ts := range labels {
		if !config.ShouldDumpLabel(ts) {
			continue
		}

		data, err := readData(ts, pool, config)
		if err != nil {
			continue
		}

		filename := filepath.Join(dataPath, db.LabelToFilename(ts))
		file, err := os.Create(filename)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		slog.Info("Writing data to " + filename)
		if err = gocsv.MarshalFile(data, file); err != nil {
			slog.Error(err.Error())
			continue
		}
	}
}

func getDataLabels(pool *pgxpool.Pool, config *Config) ([]*lard.Label, error) {
	// TODO: not sure about the sensor/level conditions,
	// they should never be NULL since they have default values different from NULL?
	// TODO: We probably don't even need the join,
	// because `name` (`param_code`) is not present in our `labels.met`?
	// query := `SELECT DISTINCT stationid, typeid, paramid, sensor::int, level, name FROM data
	//              LEFT JOIN param USING (paramid)
	//              WHERE name IS NOT NUL
	//                AND sensor IS NOT NULL
	//                AND level IS NOT NULL
	//                AND ($1::timestamp IS NULL OR obstime >= $1)
	//                AND ($2::timestamp IS NULL OR obstime < $2)`
	query := `SELECT DISTINCT stationid, typeid, paramid, sensor::int, level FROM data
              WHERE ($1::timestamp IS NULL OR obstime >= $1) AND ($2::timestamp IS NULL OR obstime < $2)`

	rows, err := pool.Query(context.TODO(), query, config.FromTime, config.ToTime)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	tsList, err := pgx.CollectRows(rows, pgx.RowToStructByName[*lard.Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return tsList, nil
}

func readData(label *lard.Label, pool *pgxpool.Pool, config *Config) (db.Data, error) {
	// TODO: is the case useful here, we can just check for cfailed = '' in here
	// query := `SELECT
	// 			obstime,
	// 			original,
	// 			tbtime,
	// 			CASE
	// 				WHEN original = corrected AND cfailed = '' THEN NULL
	// 				ELSE corrected
	// 			END,
	// 			controlinfo,
	// 			useinfo,
	// 			cfailed
	// 		FROM
	// 			data
	// 		WHERE
	// 			stationid = $1
	// 			AND typeid = $2
	// 			AND paramid = $3
	// 			AND sensor = $4
	// 			AND level = $5
	// 			AND obstime >= $6
	// TODO: should we keep these? Maybe obstime is actually useful
	// 		ORDER BY
	// 			stationid,
	// 			obstime`
	query := `SELECT obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
                FROM data
                WHERE stationid = $1
                  AND typeid = $2
                  AND paramid = $3
                  AND sensor = $4
                  AND level = $5
                AND ($6::timestamp IS NULL OR obstime >= $6)
                AND ($7::timestamp IS NULL OR obstime < $7)
                ORDER BY 
                    stationid, obstime`

	rows, err := pool.Query(
		context.TODO(),
		query,
		label.StationID,
		label.TypeID,
		label.ParamID,
		label.Sensor,
		label.Level,
		config.FromTime,
		config.ToTime,
	)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToStructByName[*db.DataObs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}
