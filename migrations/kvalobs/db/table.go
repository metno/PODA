package db

import (
	"migrate/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Maps to `data` and `text_data` tables in Kvalobs
type Table[S DataSeries | TextSeries] struct {
	// Name       string         // Name of the table
	Path       string         // Path of the dumped table
	DumpLabels LabelDumpFunc  // Function that dumps labels from the table
	DumpSeries ObsDumpFunc[S] // Function that dumps observations from the table
	Import     ImportFunc     // Function that ingests observations into LARD
	ReadCSV    ReadCSVFunc    // Function that reads dumped CSV files
}

type DataTable = Table[DataSeries]
type TextTable = Table[TextSeries]

// Function used to query labels from kvalobs given an optional timespan
type LabelDumpFunc func(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*Label, error)

// Function used to query timeseries from kvalobs for a specific label
type ObsDumpFunc[S DataSeries | TextSeries] func(label *Label, timespan *utils.TimeSpan, pool *pgxpool.Pool) (S, error)

// Lard Import function
type ImportFunc func(ts [][]any, pool *pgxpool.Pool, logStr string) (int64, error)

// How to read dumped CSV, returns one array for observations and one for flags
type ReadCSVFunc func(tsid int32, filename string) ([][]any, [][]any, error)
