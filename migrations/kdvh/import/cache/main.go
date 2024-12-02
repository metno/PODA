package cache

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	"migrate/kdvh/db"
	"migrate/lard"
	"migrate/utils"
)

type Cache struct {
	Offsets OffsetMap
	Stinfo  StinfoMap
	KDVH    KDVHMap
	Permits lard.PermitMaps
}

// Caches all the metadata needed for import of KDVH tables.
// If any error occurs inside here the program will exit.
func CacheMetadata(tables, stations, elements []string, kdvh *db.KDVH) *Cache {
	return &Cache{
		Stinfo:  cacheStinfoMeta(tables, elements, kdvh),
		Permits: lard.NewPermitTables(),
		Offsets: cacheParamOffsets(),
		KDVH:    cacheKDVH(tables, stations, elements, kdvh),
	}
}

// Convenience struct that holds information for a specific timeseries
type TsInfo struct {
	Id      int32
	Station int32
	Element string
	Offset  period.Period
	Param   StinfoParam
	Span    utils.TimeSpan
	Logstr  string
	IsOpen  bool
}

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*TsInfo, error) {
	logstr := fmt.Sprintf("[%v - %v - %v]: ", table, station, element)
	key := newKDVHKey(element, table, station)

	param, ok := cache.Stinfo[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("No metadata")
	}

	// Check if data for this station/element is restricted
	// TODO: eventually use this to choose which table to use on insert
	isOpen := cache.Permits.TimeseriesIsOpen(station, param.TypeID, param.ParamID)
	if !isOpen {
		slog.Warn(logstr + "Timeseries data is restricted")
		return nil, errors.New("Restricted data")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	span, ok := cache.KDVH[key]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		Level:     param.Hlevel,
	}

	// TODO: are Param.Fromtime and Span.From different?
	timespan := utils.TimeSpan{From: &param.Fromtime, To: span.To}
	tsid, err := lard.GetTimeseriesID(&label, timespan, pool)
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
		IsOpen:  isOpen,
	}, nil
}
