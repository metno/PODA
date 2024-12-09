package db

import (
	"context"
	"log/slog"
	"migrate/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Here are implemented the `LabelDumpFunc` stored inside the Table struct

func dumpDataLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*Label, error) {
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
	labels := make([]*Label, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByName[Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}

func dumpTextLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*Label, error) {
	// NOTE: `param` table is empty in histkvalobs
	query := `SELECT DISTINCT stationid, typeid, paramid, NULL AS sensor, NULL AS level
                FROM text_data
                WHERE ($1::timestamp IS NULL OR obstime >= $1) 
                  AND ($2::timestamp IS NULL OR obstime < $2)
                ORDER BY stationid`

	slog.Info("Querying text labels...")
	rows, err := pool.Query(context.TODO(), query, timespan.From, timespan.To)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	slog.Info("Collecting text labels...")
	labels := make([]*Label, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByName[Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}
