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
)

// Map of `from_time` and `to_time` for each (table, station, element) triplet. Not present for all parameters
type KDVHMap = map[KDVHKey]Timespan

// Used for lookup of fromtime and totime from KDVH
type KDVHKey struct {
	Inner   StinfoKey
	Station int32
}

func newKDVHKey(elem, table string, stnr int32) KDVHKey {
	return KDVHKey{StinfoKey{ElemCode: elem, TableName: table}, stnr}
}

// Timespan stored in KDVH for a given (table, station, element) triplet
type Timespan struct {
	FromTime *time.Time `db:"fdato"`
	ToTime   *time.Time `db:"tdato"`
}

// Struct used to deserialize KDVH query in cacheKDVH
type MetaKDVH struct {
	ElemCode  string     `db:"elem_code"`
	TableName string     `db:"table_name"`
	Station   int32      `db:"stnr"`
	FromTime  *time.Time `db:"fdato"`
	ToTime    *time.Time `db:"tdato"`
}

func (m *MetaKDVH) toTimespan() Timespan {
	return Timespan{m.FromTime, m.ToTime}
}

func (m *MetaKDVH) toKey() KDVHKey {
	return KDVHKey{StinfoKey{ElemCode: m.ElemCode, TableName: m.TableName}, m.Station}
}

func cacheKDVH(tables, stations, elements []string) KDVHMap {
	cache := make(KDVHMap)

	fmt.Println("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error("Could not connect to KDVH proxy. Make sure to be connected to the VPN: " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, t := range db.KDVH {
		if tables != nil && !slices.Contains(tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] IS NULL OR stnr = ANY($1))
                AND ($2::text[] IS NULL OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, stations, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByName[MetaKDVH])
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for _, meta := range metas {
			cache[meta.toKey()] = meta.toTimespan()
		}
	}

	return cache
}
