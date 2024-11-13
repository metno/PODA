package kdvh

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rickb777/period"
)

// In KDVH for each table name we usually have three separate tables:
// 1. A DATA table containing observation values;
// 2. A FLAG table containing quality control (QC) flags;
// 3. A ELEM table containing metadata about the validity of the timeseries.
//
// DATA and FLAG tables have the same schema:
// | dato | stnr | ... |
// where 'dato' is the timestamp of the observation, 'stnr' is the station
// where the observation was measured, and '...' is a varying number of columns
// each with different observations, where the column name is the 'elem_code'
// (e.g. for air temperature, 'ta').
//
// The ELEM tables have the following schema:
// | stnr | elem_code | fdato | tdato | table_name | flag_table_name | audit_dato

// Table contains metadata on how to treat different tables in KDVH
type Table struct {
	TableName     string          // Name of the DATA table
	FlagTableName string          // Name of the FLAG table
	ElemTableName string          // Name of the ELEM table
	Path          string          // Directory name of where the dumped table is stored
	dumpFunc      DumpFunction    // Function used to dump the KDVH table (found in `dump_functions.go`)
	convFunc      ConvertFunction // Function that converts KDVH obs to Lardobs (found in `import_functions.go`)
	importUntil   int             // Import data only until the year specified by this field
}

type DumpFunction func(path string, meta DumpMeta, conn *sql.DB) error
type DumpMeta struct {
	element   string
	station   string
	dataTable string
	flagTable string
	overwrite bool
	logStr    string
}

type ConvertFunction func(KdvhObs) (LardObs, error)
type KdvhObs struct {
	*TimeseriesInfo
	Obstime time.Time
	Data    string
	Flags   string
}

// Convenience struct that holds information for a specific timeseries
type TimeseriesInfo struct {
	station int32
	element string
	offset  period.Period
	param   StinfoParam
	span    Timespan
	logstr  string
}

func (config *ImportConfig) NewTimeseriesInfo(table, element string, station int32) (*TimeseriesInfo, error) {
	logstr := fmt.Sprintf("%v - %v - %v: ", table, station, element)
	key := newKDVHKey(element, table, station)

	meta, ok := config.StinfoMap[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := config.OffsetMap[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	span := config.KDVHMap[key]

	return &TimeseriesInfo{
		station: station,
		element: element,
		offset:  offset,
		param:   meta,
		span:    span,
		logstr:  logstr,
	}, nil
}

// Creates default Table
func NewTable(data, flag, elem string) *Table {
	return &Table{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		Path:          data + "_combined", // NOTE: '_combined' kept for backward compatibility with original scripts
		dumpFunc:      dumpDataAndFlags,
		convFunc:      makeDataPage,
	}
}

// Sets the `ImportUntil` field if the year is greater than 0
func (t *Table) SetImport(year int) *Table {
	if year > 0 {
		t.importUntil = year
	}
	return t
}

// Sets the function used to dump the Table
func (t *Table) SetDumpFunc(fn DumpFunction) *Table {
	t.dumpFunc = fn
	return t
}

// Sets the function used to convert observations from the table to Lardobservations
func (t *Table) SetConvFunc(fn ConvertFunction) *Table {
	t.convFunc = fn
	return t
}
