package cache

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
)

type StationId = int32
type PermitId = int32

type ParamPermitMap map[StationId][]ParamPermit
type StationPermitMap map[StationId]PermitId

type ParamPermit struct {
	TypeId   int
	ParamdId int
	PermitId int
}

func cacheParamPermits(conn *pgx.Conn) ParamPermitMap {
	cache := make(ParamPermitMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT stationid, message_formatid, paramid, permitid FROM v_station_param_policy",
	)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for rows.Next() {
		var stnr StationId
		var permit ParamPermit

		if err := rows.Scan(&stnr, &permit.TypeId, &permit.ParamdId, &permit.PermitId); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[stnr] = append(cache[stnr], permit)
	}

	if rows.Err() != nil {
		slog.Error(rows.Err().Error())
		os.Exit(1)
	}

	return cache
}

func cacheStationPermits(conn *pgx.Conn) StationPermitMap {
	cache := make(StationPermitMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT stationid, permitid FROM station_policy",
	)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for rows.Next() {
		var stnr StationId
		var permit PermitId

		if err := rows.Scan(&stnr, &permit); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[stnr] = permit
	}

	if rows.Err() != nil {
		slog.Error(rows.Err().Error())
		os.Exit(1)
	}

	return cache
}
