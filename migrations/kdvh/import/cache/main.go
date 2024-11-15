package cache

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	"migrate/lard"
)

type Cache struct {
	OffsetMap OffsetMap
	StinfoMap StinfoMap
	KDVHMap   KDVHMap
}

// TODO: cache permissions

// Caches all the metadata needed for import.
// If any error occurs inside here the program will exit.
func CacheMetadata(tables, stations, elements []string) *Cache {
	return &Cache{
		OffsetMap: cacheParamOffsets(),
		StinfoMap: cacheStinfo(tables, elements),
		KDVHMap:   cacheKDVH(tables, stations, elements),
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
}

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*TsInfo, error) {
	logstr := fmt.Sprintf("%v - %v - %v: ", table, station, element)
	key := newKDVHKey(element, table, station)

	param, ok := cache.StinfoMap[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.OffsetMap[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	span := cache.KDVHMap[key]

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

	return &TsInfo{
		Id:      tsid,
		Station: station,
		Element: element,
		Offset:  offset,
		Param:   param,
		Span:    span,
		Logstr:  logstr,
	}, nil
}
