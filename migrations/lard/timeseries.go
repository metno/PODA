package lard

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Struct that mimics `labels.met` table structure
type Label struct {
	StationID int32
	TypeID    int32
	ParamID   int32
	Sensor    *int32
	Level     *int32
}

func GetTimeseriesID(label Label, fromtime time.Time, pool *pgxpool.Pool) (tsid int32, err error) {
	// Query LARD labels table
	err = pool.QueryRow(
		context.TODO(),
		`SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID
	if err == nil {
		return tsid, nil
	}

	// Otherwise insert new timeseries
	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return tsid, err
	}

	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id`,
		fromtime,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		context.TODO(),
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor)
	if err != nil {
		return tsid, err
	}

	err = transaction.Commit(context.TODO())
	return tsid, err
}
