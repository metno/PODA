package dump

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
)

func getDataLabels(timespan *TimeSpan, pool *pgxpool.Pool) ([]*db.KvLabel, error) {
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
              WHERE ($1::timestamp IS NULL OR obstime >= $1) AND ($2::timestamp IS NULL OR obstime < $2)
              ORDER BY stationid`

	slog.Info("Querying data labels...")
	rows, err := pool.Query(context.TODO(), query, timespan.From, timespan.To)
	if err != nil {
		return nil, err
	}

	slog.Info("Collecting data labels...")
	labels := make([]*db.KvLabel, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByPos[db.KvLabel])
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func getDataSeries(label *db.KvLabel, timespan *TimeSpan, pool *pgxpool.Pool) (db.DataSeries, error) {
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
		timespan.From,
		timespan.To,
	)
	if err != nil {
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[db.DataObs])
	if err != nil {
		return nil, err
	}

	return data, nil
}
