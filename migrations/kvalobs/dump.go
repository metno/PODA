package kvalobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	// "migrate/lard"
	"os"
	"path/filepath"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Same timeseries could be in both 'data' and 'text_data' tables
// First of all, why?
// Second, do we care?
// func readDataAndText(label *TSLabel, pool *pgxpool.Pool, config *DumpConfig) Data {
//     // Supposed to join text anf number data to single slice
//     return nil
// }

type DumpConfig struct {
	BaseConfig
}

func (config *DumpConfig) Execute(_ []string) error {
	config.setup()

	// dump kvalobs
	config.Dump("KVALOBS_CONN_STRING", filepath.Join(config.BaseDir, "kvalobs"))

	// dump histkvalobs
	config.Dump("HISTKVALOBS_CONN_STRING", filepath.Join(config.BaseDir, "histkvalobs"))

	return nil
}

func (config *DumpConfig) Dump(envvar, path string) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(envvar))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
		return
	}
	defer pool.Close()

	config.DumpText(pool, path)
	config.DumpData(pool, path)
}

func (config *DumpConfig) DumpText(pool *pgxpool.Pool, path string) {
	var labels []TSLabel

	textPath := filepath.Join(path, "text")
	if err := os.MkdirAll(textPath, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	labelFile := filepath.Join(path, "text_labels.csv")
	if _, err := os.Stat(labelFile); err != nil {
		if labels, err = config.dumpLabels(pool, labelFile, getTextLabels); err != nil {
			return
		}
	} else {
		if labels, err = readCSVfile[TSLabel](labelFile); err != nil {
			return
		}
	}

	for _, ts := range labels {
		if !ts.ShouldBeDumped(config) {
			continue
		}

		data, err := readTextData(&ts, pool, config)
		if err != nil {
			continue
		}

		filename := filepath.Join(textPath, ts.toFilename())
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

func (config *DumpConfig) DumpData(pool *pgxpool.Pool, path string) {
	var labels []TSLabel

	dataPath := filepath.Join(path, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	labelFile := filepath.Join(path, "data_labels.csv")
	if _, err := os.Stat(path); err != nil {
		if labels, err = config.dumpLabels(pool, labelFile, getDataLabels); err != nil {
			return
		}
	} else {
		if labels, err = readCSVfile[TSLabel](labelFile); err != nil {
			return
		}
	}

	for _, ts := range labels {
		if !ts.ShouldBeDumped(config) {
			continue
		}

		data, err := readData(&ts, pool, config)
		if err != nil {
			continue
		}

		filename := filepath.Join(dataPath, ts.toFilename())
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

type LabelDumpFunc = func(pool *pgxpool.Pool, config *DumpConfig) ([]TSLabel, error)

func (config *DumpConfig) dumpLabels(pool *pgxpool.Pool, path string, fn LabelDumpFunc) ([]TSLabel, error) {
	labels, err := fn(pool, config)
	if err != nil {
		// Error logged inside fn
		return nil, err
	}

	file, err := os.Create(path)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	slog.Info("Writing timeseries labels to " + path)
	if err = gocsv.Marshal(labels, file); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}

func (config *DumpConfig) dumpTextTS(pool *pgxpool.Pool) {
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

// This is basically the same as lard.Label (except for ParamCode)
type TSLabel struct {
	StationID int32  `db:"stationid"`
	TypeID    int32  `db:"typeid"`
	ParamID   int32  `db:"paramid"`
	Sensor    *int32 `db:"sensor"`
	Level     *int32 `db:"level"`
	// ParamCode string `db:"name,omitempty"`
}

// Serialize Label to CSV file name
func (ts *TSLabel) toFilename() string {
	var sensor, level string
	if ts.Sensor != nil {
		sensor = fmt.Sprint(ts.Sensor)
	}
	if ts.Level != nil {
		level = fmt.Sprint(ts.Level)
	}
	return fmt.Sprintf("%v_%v_%v_%v_%v.csv", ts.StationID, ts.TypeID, ts.ParamID, sensor, level)
}

func parseFilename(s *string) (*int32, error) {
	// TODO: probably there is a better way to do this without defining a gazillion functions
	if *s == "" {
		return nil, nil
	}
	res, err := strconv.ParseInt(*s, 10, 32)
	if err != nil {
		return nil, err
	}
	out := int32(res)
	return &out, nil
}

func pf(s *string) *int32 {
	// TODO: probably there is a better way to do this without defining a gazillion functions
	if *s == "" {
		return nil
	}
	res := toInt32(*s)
	return &res
}

// Deserialize filename to TSLabel struct
func (ts *TSLabel) fromFilename(filename string) error {
	name := strings.TrimSuffix(filename, ".csv")
	fields := strings.Split(name, "_")
	if len(fields) < 5 {
		return errors.New("Too few fields in file name: " + filename)
	}

	ptrs := make([]*string, len(fields))
	for i := range ptrs {
		ptrs[i] = &fields[i]
	}

	converted, err := TryMap(ptrs, parseFilename)
	if err != nil {
		return err
	}

	ts.StationID = *converted[0]
	ts.TypeID = *converted[1]
	ts.ParamID = *converted[2]
	ts.Sensor = converted[3]
	ts.Level = converted[4]

	return nil
}

func LabelFromFilename(filename string) (TSLabel, error) {
	name := strings.TrimSuffix(filename, ".csv")
	fields := strings.Split(name, "_")
	if len(fields) < 5 {
		return TSLabel{}, errors.New("Too few fields in file name: " + filename)
	}

	ptrs := make([]*string, len(fields))
	for i := range ptrs {
		ptrs[i] = &fields[i]
	}

	converted, err := TryMap(ptrs, parseFilename)
	if err != nil {
		return TSLabel{}, err
	}

	return TSLabel{
		StationID: *converted[0],
		TypeID:    *converted[1],
		ParamID:   *converted[2],
		Sensor:    converted[3],
		Level:     converted[4],
	}, nil
}

func getTextLabels(pool *pgxpool.Pool, config *DumpConfig) ([]TSLabel, error) {
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
	tsList, err := pgx.CollectRows(rows, pgx.RowToStructByName[TSLabel])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return tsList, nil
}

func getDataLabels(pool *pgxpool.Pool, config *DumpConfig) ([]TSLabel, error) {
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

	tsList, err := pgx.CollectRows(rows, pgx.RowToStructByName[TSLabel])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return tsList, nil
}

// TODO: not sure what to do with this one
// func joinTS(first, second []TSLabel)

// Kvalobs observation row
type Obs struct {
	Obstime     time.Time `db:"obstime"`
	Original    float64   `db:"original"`
	Tbtime      time.Time `db:"tbtime"`
	Corrected   float64   `db:"corrected"`
	Controlinfo *string   `db:"controlinfo"`
	Useinfo     *string   `db:"useinfo"`
	Cfailed     *string   `db:"cfailed"`
}

type TextObs struct {
	Obstime  time.Time `db:"obstime"`
	Original string    `db:"original"`
	Tbtime   time.Time `db:"tbtime"`
}

type Data = []Obs
type Text = []TextObs

func readTextData(label *TSLabel, pool *pgxpool.Pool, config *DumpConfig) (Text, error) {
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
                ORDER BY obstime`

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

	data, err := pgx.CollectRows(rows, pgx.RowToStructByName[TextObs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}

func readData(label *TSLabel, pool *pgxpool.Pool, config *DumpConfig) (Data, error) {
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
                ORDER BY obstime`

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

	data, err := pgx.CollectRows(rows, pgx.RowToStructByName[Obs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}
