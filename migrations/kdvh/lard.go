package kdvh

import (
	"time"

	"github.com/jackc/pgx/v5"
)

// LardTimeseries in LARD have and ID and associated observations
type LardTimeseries struct {
	id   int32
	data []LardObs
}

func NewTimeseries(id int32, data []LardObs) *LardTimeseries {
	return &LardTimeseries{id, data}
}

func (ts *LardTimeseries) Len() int {
	return len(ts.data)
}

func (ts *LardTimeseries) InsertData(i int) ([]any, error) {
	return []any{
		ts.id,
		ts.data[i].Obstime,
		ts.data[i].Data,
	}, nil
}

func (ts *LardTimeseries) InsertText(i int) ([]any, error) {
	return []any{
		ts.id,
		ts.data[i].Obstime,
		ts.data[i].Text,
	}, nil
}

var FLAGS_TABLE pgx.Identifier = pgx.Identifier{"flags", "kdvh"}
var FLAGS_COLS []string = []string{"timeseries", "obstime", "controlinfo", "useinfo"}

func (ts *LardTimeseries) InsertFlags(i int) ([]any, error) {
	return []any{
		ts.id,
		ts.data[i].Obstime,
		ts.data[i].Controlinfo,
		ts.data[i].Useinfo,
	}, nil
}

// Struct containg all the fields we want to save in LARD from KDVH
type LardObs struct {
	// Time of observation
	Obstime time.Time
	// Observation data formatted as a single precision floating point number
	Data *float32
	// Observation data that cannot be represented as a float, therefore stored as a string
	Text *string
	// Flag encoding quality control status
	Controlinfo string
	// Flag encoding quality control status
	Useinfo string
}
