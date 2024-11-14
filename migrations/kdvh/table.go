package kdvh

import (
	"errors"
	"fmt"
	"log/slog"
	"migrate/lard"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	importUntil   int             // Import data only until the year specified by this field. If this field is not explicitly set, table import is skipped.
}

// Implementation of these functions can be found in `dump_functions.go`
type DumpFunction func(path string, meta DumpMeta, pool *pgxpool.Pool) error
type DumpMeta struct {
	element   string
	station   string
	dataTable string
	flagTable string
	overwrite bool
	logStr    string
}

// Implementation of these functions can be found in `import_functions.go`
// It returns three structs for each of the lard tables we are inserting into
type ConvertFunction func(KdvhObs) (lard.DataObs, lard.TextObs, lard.Flag, error)
type KdvhObs struct {
	*TimeseriesInfo
	id      int32
	obstime time.Time
	data    string
	flags   string
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
		convFunc:      Convert,
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
