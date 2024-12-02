package cache

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"migrate/kdvh/db"
	"migrate/utils"
)

// Map of `from_time` and `to_time` for each (table, station, element) triplet. Not present for all parameters
type KDVHMap = map[KDVHKey]utils.TimeSpan

// Used for lookup of fromtime and totime from KDVH
type KDVHKey struct {
	Inner   StinfoKey
	Station int32
}

func newKDVHKey(elem, table string, stnr int32) KDVHKey {
	return KDVHKey{StinfoKey{ElemCode: elem, TableName: table}, stnr}
}

// Cache timeseries timespan from KDVH
func cacheKDVH(tables, stations, elements []string, kdvh *db.KDVH) KDVHMap {
	cache := make(KDVHMap)

	slog.Info("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(db.KDVH_ENV_VAR))
	if err != nil {
		slog.Error("Could not connect to KDVH proxy. Make sure to be connected to the VPN: " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, t := range kdvh.Tables {
		if len(tables) > 0 && !slices.Contains(tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] = '{}' OR stnr = ANY($1))
                AND ($2::text[] = '{}' OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, stations, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for rows.Next() {
			var key KDVHKey
			var span utils.TimeSpan

			err := rows.Scan(
				&key.Inner.TableName,
				&key.Station,
				&key.Inner.ElemCode,
				&span.From,
				&span.To,
			)

			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}

			cache[key] = span
		}

		if rows.Err() != nil {
			slog.Error(rows.Err().Error())
			os.Exit(1)
		}

	}

	return cache
}
