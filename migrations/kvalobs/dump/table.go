package dump

import (
	"migrate/kvalobs/db"
	"migrate/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Maps to `data` and `text_data` tables in Kvalobs
type Table[S db.DataSeries | db.TextSeries] struct {
	Name     string         // Name of the table
	LabelFn  LabelDumpFunc  // Function that dumps labels from the table
	ObsFn    ObsDumpFunc[S] // Function that dumps observations from the table
	ImportFn func()
}

// Function used to query labels from kvalobs given an optional timespan
type LabelDumpFunc func(timespan *utils.TimeSpan, pool *pgxpool.Pool) ([]*db.KvLabel, error)

// Function used to query timeseries from kvalobs for a specific label
type ObsDumpFunc[S db.DataSeries | db.TextSeries] func(label *db.KvLabel, timespan *utils.TimeSpan, pool *pgxpool.Pool) (S, error)
