package dump

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

// Returns a TextTable for dump
func TextTable(path string) db.TextTable {
	return db.TextTable{
		Path:       filepath.Join(path, db.TEXT_TABLE_NAME),
		DumpLabels: dumpTextLabels,
		DumpSeries: dumpTextSeries,
	}
}

func dumpTextLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*db.Label, error) {
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
	labels := make([]*db.Label, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByPos[db.Label])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}

func dumpTextSeries(label *db.Label, timespan *utils.TimeSpan, pool *pgxpool.Pool) (db.TextSeries, error) {
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
		timespan.From,
		timespan.To,
	)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[db.TextObs])
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return data, nil
}
