package lard

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const STINFO_ENV_VAR string = "STINFO_STRING"

type StationId = int32
type PermitId = int32

type ParamPermitMap map[StationId][]ParamPermit
type StationPermitMap map[StationId]PermitId

type ParamPermit struct {
	TypeId   int32
	ParamdId int32
	PermitId int32
}

type PermitMaps struct {
	ParamPermits   ParamPermitMap
	StationPermits StationPermitMap
}

func NewPermitTables() *PermitMaps {
	slog.Info("Connecting to Stinfosys to cache permits")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(STINFO_ENV_VAR))
	if err != nil {
		slog.Error("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(ctx)

	return &PermitMaps{
		ParamPermits:   cacheParamPermits(conn),
		StationPermits: cacheStationPermits(conn),
	}
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

func (c *PermitMaps) TimeseriesIsOpen(stnr, typeid, paramid int32) bool {
	// First check param permit table
	if permits, ok := c.ParamPermits[stnr]; ok {
		for _, permit := range permits {
			if (permit.TypeId == 0 || permit.TypeId == typeid) &&
				(permit.ParamdId == 0 || permit.ParamdId == paramid) {
				return permit.PermitId == 1
			}
		}
	}

	// Otherwise check station permit table
	if permit, ok := c.StationPermits[stnr]; ok {
		return permit == 1
	}

	return false
}
