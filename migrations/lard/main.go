package lard

import "time"

// Struct mimicking the `public.data` table
type DataObs struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Observation data formatted as a single precision floating point number
	Data *float32
}

func (o *DataObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Data}
}

// Struct mimicking the `public.nonscalar_data` table
type TextObs struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Observation data that cannot be represented as a float, therefore stored as a string
	Text *string
}

func (o *TextObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Text}
}

// Struct mimicking the `flags.old_databases` table
type Flag struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Corrected value after QC tests
	Corrected *float32
	// Flag encoding quality control status
	Controlinfo *string
	// Flag encoding quality control status
	Useinfo *string
	// Number of tests that failed?
	Cfailed *int32
}

func (o *Flag) ToRow() []any {
	// "timeseries", "obstime", "corrected","controlinfo", "useinfo", "cfailed"
	return []any{o.Id, o.Obstime, o.Corrected, o.Controlinfo, o.Useinfo, o.Cfailed}
}
