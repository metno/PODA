package dump

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

// Returns a DataTable for dump
func DataTable(path string) db.DataTable {
	return db.DataTable{
		Path:       filepath.Join(path, db.DATA_TABLE_NAME),
		DumpLabels: dumpDataLabels,
		DumpSeries: dumpDataSeries,
	}
}

func dumpDataLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*db.Label, error) {
	query := `SELECT DISTINCT stationid, typeid, paramid, sensor::int, level 
                FROM data
                WHERE ($1::timestamp IS NULL OR obstime >= $1) 
                  AND ($2::timestamp IS NULL OR obstime < $2)
                ORDER BY stationid`

	slog.Info("Querying data labels...")
	rows, err := pool.Query(context.TODO(), query, timespan.From, timespan.To)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	slog.Info("Collecting data labels...")
	labels := make([]*db.Label, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByName[db.Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}

func dumpDataSeries(label *db.Label, timespan *utils.TimeSpan, pool *pgxpool.Pool) (db.DataSeries, error) {
	// NOTE: sensor and level could be NULL, but in reality they have default values
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

	// Convert to string because `sensor` in Kvalobs is a BPCHAR(1)
	var sensor *string
	if label.Sensor != nil {
		sensorval := fmt.Sprint(*label.Sensor)
		sensor = &sensorval
	}

	rows, err := pool.Query(
		context.TODO(),
		query,
		label.StationID,
		label.TypeID,
		label.ParamID,
		sensor,
		label.Level,
		timespan.From,
		timespan.To,
	)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[db.DataObs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}
