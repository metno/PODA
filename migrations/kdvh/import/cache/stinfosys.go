package cache

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"migrate/kdvh/db"
	"migrate/lard"
)

// Map of metadata used to query timeseries ID in LARD
type StinfoMap = map[StinfoKey]StinfoParam

// StinfoKey is used for lookup of parameter offsets and metadata from Stinfosys
type StinfoKey struct {
	ElemCode  string
	TableName string
}

// Subset of elem_map_cfnames_param query with only param info
type StinfoParam struct {
	TypeID   int32
	ParamID  int32
	Hlevel   *int32
	Sensor   int32
	Fromtime time.Time
	IsScalar bool
}

// Save metadata for later use by quering Stinfosys
func cacheStinfoMeta(tables, elements []string, kdvh *db.KDVH) StinfoMap {
	cache := make(StinfoMap)

	slog.Info("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(lard.STINFO_ENV_VAR))
	if err != nil {
		slog.Error("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(ctx)

	for _, table := range kdvh.Tables {
		if len(tables) > 0 && !slices.Contains(tables, table.TableName) {
			continue
		}

		// select paramid, elem_code, scalar from elem_map_cfnames_param join param using(paramid) where scalar = false
		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime, scalar
                    FROM elem_map_cfnames_param
                    JOIN param USING(paramid)
                    WHERE table_name = $1
                    AND ($2::text[] = '{}' OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, table.TableName, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for rows.Next() {
			var key StinfoKey
			var param StinfoParam
			err := rows.Scan(
				&key.ElemCode,
				&key.TableName,
				&param.TypeID,
				&param.ParamID,
				&param.Hlevel,
				&param.Sensor,
				&param.Fromtime,
				&param.IsScalar,
			)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}

			cache[key] = param
		}

		if rows.Err() != nil {
			slog.Error(rows.Err().Error())
			os.Exit(1)
		}
	}

	return cache
}
