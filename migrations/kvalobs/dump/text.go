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

func DumpText(path string, pool *pgxpool.Pool, config *Config) {
	var labels []*lard.Label

	textPath := filepath.Join(path, "text")
	if err := os.MkdirAll(textPath, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	labelFile := filepath.Join(path, "labels.csv")
	if _, err := os.Stat(labelFile); err != nil {
		if labels, err = dumpLabels(pool, labelFile, getTextLabels, config); err != nil {
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

		// TODO: Dump per station? Not strictly necessary? But makes it more organized?
		stationDir := filepath.Join(textPath, string(ts.StationID))
		if err := os.MkdirAll(stationDir, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		data, err := readTextData(ts, pool, config)
		if err != nil {
			continue
		}

		filename := filepath.Join(textPath, string(ts.StationID), db.LabelToFilename(ts))
		file, err := os.Create(filename)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		slog.Info("Writing text to " + filename)
		if err = gocsv.MarshalFile(data, file); err != nil {
			slog.Error(err.Error())
			continue
		}
	}
}

func (config *Config) dumpTextTS(pool *pgxpool.Pool) {
	timeseries, err := getTextLabels(pool, config)
	if err != nil {
		// Error logged inside getTextTS
		return
	}

	if err := os.MkdirAll(config.BaseDir, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	path := filepath.Join(config.BaseDir, "text_timeseries.csv")
	file, err := os.Create(path)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	slog.Info("Writing timeseries labels to CSV")
	if err = gocsv.Marshal(timeseries, file); err != nil {
		slog.Error(err.Error())
		return
	}
}

func getTextLabels(pool *pgxpool.Pool, config *Config) ([]*lard.Label, error) {
	// OGquery := `SELECT DISTINCT
	//            stationid,
	//            typeid,
	//            paramid,
	//            0 AS sensor,
	//            0 AS level,
	//            name AS code
	//        FROM
	//            text_data
	//        LEFT JOIN
	//            param USING (paramid)
	//        WHERE
	//            obstime >= $1
	// TODO: probably don't need this?
	//            AND obstime <= $2
	//            AND name IS NOT NULL
	// TODO: do we need this order by? As far as I can see,
	// it's used to compare text_data and scalar_data timeseries
	//        ORDER BY
	//            stationid,
	//            typeid,
	//            paramid,
	//            level,
	//            sensor`

	// NOTE: `param` table is empty in histkvalobs
	// TODO: We probably don't even need the join,
	// because `name` (`param_code`) is not present in our `labels.met`?
	// query := `SELECT DISTINCT stationid, typeid, paramid, name FROM text_data
	//              LEFT JOIN param USING (paramid)
	//              WHERE name IS NOT NULL
	//                AND ($1::timestamp IS NULL OR obstime >= $1)
	//                AND ($2::timestamp IS NULL OR obstime < $2)`
	//
	// TODO: should sensor/level be NULL or 0
	query := `SELECT DISTINCT stationid, typeid, paramid, NULL AS sensor, NULL AS level FROM text_data
              WHERE ($1::timestamp IS NULL OR obstime >= $1) AND ($2::timestamp IS NULL OR obstime < $2)`

	slog.Info("Querying distinct timeseries labels")
	rows, err := pool.Query(context.TODO(), query, config.FromTime, config.ToTime)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	slog.Info("Collecting rows to slice")
	tsList, err := pgx.CollectRows(rows, pgx.RowToStructByName[*lard.Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return tsList, nil
}

func readTextData(label *lard.Label, pool *pgxpool.Pool, config *Config) (db.Text, error) {
	// query := `
	//        SELECT
	//            obstime,
	//            original AS originaltext,
	//            tbtime
	//        FROM
	//            text_data
	//        WHERE
	//            stationid = $1
	//            AND typeid = $2
	//            AND paramid = $3
	//            AND obstime >= $4
	//            AND obstime <= $5
	// TODO: should we keep these? Maybe obstime is actually useful
	//        ORDER BY
	//            stationid,
	//            obstime`
	query := `SELECT obstime, original, tbtime FROM text_data
                WHERE stationid = $1
                  AND typeid = $2
                  AND paramid = $3
                  AND ($4::timestamp IS NULL OR obstime >= $4)
                  AND ($5::timestamp IS NULL OR obstime < $5)
                ORDER BY 
                    stationid, obstime`

	rows, err := pool.Query(
		context.TODO(),
		query,
		label.StationID,
		label.TypeID,
		label.ParamID,
		config.FromTime,
		config.ToTime,
	)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToStructByName[*db.TextObs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}
