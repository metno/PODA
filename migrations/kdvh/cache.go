package kdvh

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/rickb777/period"
)

// Caches all the metadata needed for import.
// If any error occurs inside here the program will exit.
func (config *ImportConfig) CacheMetadata() {
	config.cacheStinfo()
	config.cacheKDVH()
	config.cacheParamOffsets()
}

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
func (config *ImportConfig) cacheStinfo() {
	cache := make(map[StinfoKey]StinfoParam)

	fmt.Println("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		slog.Error("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, table := range KDVH {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		// select paramid, elem_code, scalar from elem_map_cfnames_param join param using(paramid) where scalar = false
		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime, scalar
                    FROM elem_map_cfnames_param
                    JOIN param USING(paramid)
                    WHERE table_name = $1
                    AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, table.TableName, config.Elements)
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

	config.StinfoMap = cache
}

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

func (config *ImportConfig) cacheKDVH() {
	cache := make(map[KDVHKey]Timespan)

	fmt.Println("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error("Could not connect to KDVH proxy. Make sure to be connected to the VPN: " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, t := range KDVH {
		if config.Tables != nil && !slices.Contains(config.Tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] IS NULL OR stnr = ANY($1))
                AND ($2::text[] IS NULL OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, config.Stations, config.Elements)
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

	config.KDVHMap = cache
}

// Caches how to modify the obstime (in KDVH) for certain paramids
func (config *ImportConfig) cacheParamOffsets() {
	cache := make(map[StinfoKey]period.Period)

	type CSVRow struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int32  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}

	csvfile, err := os.Open("kdvh/product_offsets.csv")
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[StinfoKey{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	config.OffsetMap = cache
}
