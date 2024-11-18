package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	"migrate/kdvh/db"
	"migrate/lard"
)

type Cache struct {
	Offsets        OffsetMap
	Stinfo         StinfoMap
	KDVH           KDVHMap
	ParamPermits   ParamPermitMap
	StationPermits StationPermitMap
}

// Caches all the metadata needed for import of KDVH tables.
// If any error occurs inside here the program will exit.
func CacheMetadata(tables, stations, elements []string, kdvh *db.KDVH) *Cache {
	fmt.Println("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		slog.Error("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}

	stinfoMeta := cacheStinfoMeta(tables, elements, kdvh, conn)
	stationPermits := cacheStationPermits(conn)
	paramPermits := cacheParamPermits(conn)

	conn.Close(context.TODO())

	return &Cache{
		Stinfo:         stinfoMeta,
		StationPermits: stationPermits,
		ParamPermits:   paramPermits,
		Offsets:        cacheParamOffsets(),
		KDVH:           cacheKDVH(tables, stations, elements, kdvh),
	}
}

// Convenience struct that holds information for a specific timeseries
type TsInfo struct {
	Id      int32
	Station int32
	Element string
	Offset  period.Period
	Param   StinfoParam
	Span    Timespan
	Logstr  string
	IsOpen  bool
}

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*TsInfo, error) {
	logstr := fmt.Sprintf("%v - %v - %v: ", table, station, element)
	key := newKDVHKey(element, table, station)

	param, ok := cache.Stinfo[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("")
	}

	// Check if data for this station/element is restricted
	isOpen := cache.timeseriesIsOpen(station, param.TypeID, param.ParamID)

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	span := cache.KDVH[key]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		Level:     param.Hlevel,
	}

	tsid, err := lard.GetTimeseriesID(label, param.Fromtime, pool)
	if err != nil {
		slog.Error(logstr + "could not obtain timeseries - " + err.Error())
		return nil, err
	}

	// TODO: check if station is restricted

	return &TsInfo{
		Id:      tsid,
		Station: station,
		Element: element,
		Offset:  offset,
		Param:   param,
		Span:    span,
		Logstr:  logstr,
		IsOpen:  isOpen,
	}, nil
}
