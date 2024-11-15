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

// Map of metadata used to query timeseries ID in LARD
type StinfoMap = map[StinfoKey]StinfoParam

// StinfoKey is used for lookup of parameter offsets and metadata from Stinfosys
type StinfoKey struct {
	ElemCode  string
	TableName string
}

// Subset of StinfoQuery with only param info
type StinfoParam struct {
	TypeID   int32
	ParamID  int32
	Hlevel   *int32
	Sensor   int32
	Fromtime time.Time
	IsScalar bool
}

// Struct holding query from Stinfosys elem_map_cfnames_param
type StinfoQuery struct {
	ElemCode  string    `db:"elem_code"`
	TableName string    `db:"table_name"`
	TypeID    int32     `db:"typeid"`
	ParamID   int32     `db:"paramid"`
	Hlevel    *int32    `db:"hlevel"`
	Sensor    int32     `db:"sensor"`
	Fromtime  time.Time `db:"fromtime"`
	IsScalar  bool      `db:"scalar"`
}

func (q *StinfoQuery) toParam() StinfoParam {
	return StinfoParam{
		TypeID:   q.TypeID,
		ParamID:  q.ParamID,
		Hlevel:   q.Hlevel,
		Sensor:   q.Sensor,
		Fromtime: q.Fromtime,
		IsScalar: q.IsScalar,
	}
}
func (q *StinfoQuery) toKey() StinfoKey {
	return StinfoKey{q.ElemCode, q.TableName}
}

// Save metadata for later use by quering Stinfosys
func cacheStinfo(tables, elements []string) StinfoMap {
	cache := make(StinfoMap)

	fmt.Println("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		slog.Error("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, table := range db.KDVH {
		if tables != nil && !slices.Contains(tables, table.TableName) {
			continue
		}
		// select paramid, elem_code, scalar from elem_map_cfnames_param join param using(paramid) where scalar = false
		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime, scalar
                    FROM elem_map_cfnames_param
                    JOIN param USING(paramid)
                    WHERE table_name = $1
                    AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, table.TableName, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByName[StinfoQuery])
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for _, meta := range metas {
			cache[meta.toKey()] = meta.toParam()
		}
	}

	return cache
}
