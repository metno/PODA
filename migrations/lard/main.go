package lard

import "time"

// Timeseries in LARD have and ID and associated observations
type Timeseries struct {
	id   int32
	data []Obs
}

func NewTimeseries(id int32, data []Obs) *Timeseries {
	return &Timeseries{id, data}
}

func (ts *Timeseries) Len() int {
	return len(ts.data)
}

func (ts *Timeseries) ID() int32 {
	return ts.id
}

func (ts *Timeseries) Obstime(i int) time.Time {
	return ts.data[i].Obstime
}

func (ts *Timeseries) Text(i int) string {
	return *ts.data[i].Text
}

func (ts *Timeseries) Data(i int) float32 {
	return *ts.data[i].Data
}

func (ts *Timeseries) Controlinfo(i int) string {
	return ts.data[i].Controlinfo
}

func (ts *Timeseries) Useinfo(i int) string {
	return ts.data[i].Useinfo
}

// Struct containg all the fields we want to save in LARD
type Obs struct {
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

// TODO: I'm not sure I like the interface solution
type DataInserter interface {
	Obstime(i int) time.Time
	Data(i int) float32
	ID() int32
	Len() int
}

type TextInserter interface {
	Obstime(i int) time.Time
	Text(i int) string
	ID() int32
	Len() int
}

// TODO: This maybe needs different implementation for each system
// i.e. insert to different tables and different columns
type FlagInserter interface {
	ID() int32
	Obstime(i int) time.Time
	Controlinfo(i int) string
	Useinfo(i int) string
	Len() int
}
