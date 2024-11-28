package dump

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/utils"
)

func getTextLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*db.KvLabel, error) {
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
              WHERE ($1::timestamp IS NULL OR obstime >= $1) AND ($2::timestamp IS NULL OR obstime < $2)
              ORDER BY stationid`

	slog.Info("Querying text labels...")
	rows, err := pool.Query(context.TODO(), query, timespan.From, timespan.To)
	if err != nil {
		return nil, err
	}

	slog.Info("Collecting text labels...")
	labels := make([]*db.KvLabel, 0, rows.CommandTag().RowsAffected())
	labels, err = pgx.AppendRows(labels, rows, pgx.RowToAddrOfStructByPos[db.KvLabel])
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func getTextSeries(label *db.KvLabel, timespan *utils.TimeSpan, pool *pgxpool.Pool) (db.TextSeries, error) {
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
		timespan.From,
		timespan.To,
	)
	if err != nil {
		return nil, err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[db.TextObs])
	if err != nil {
		return nil, err
	}

	return data, nil
}
