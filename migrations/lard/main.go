package lard

import "time"

const LARD_ENV_VAR string = "LARD_CONN_STRING"

// Struct mimicking the `public.data` table
type DataObs struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Observation data formatted as a single precision floating point number
	Data *float32
	// Indicator of QC status (true = no failures) always true for us as we don't QC in the
	// migrations. Will be updated when it's QCed ata later date
	QcUsable bool
}

func (o *DataObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Data, o.QcUsable}
}

// Struct mimicking the `public.nonscalar_data` table
type TextObs struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Observation data that cannot be represented as a float, therefore stored as a string
	Text *string
	// Indicator of QC status (true = no failures) always true for us as we don't QC in the
	// migrations. Will be updated when it's QCed ata later date
	QcUsable bool
}

func (o *TextObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Text, o.QcUsable}
}

// Struct mimicking the `flags.kvdata` table
type Flag struct {
	// Timeseries ID
	Id int32
	// Time of observation
	Obstime time.Time
	// Original value before QC tests
	Original *float32
	// Corrected value after QC tests
	Corrected *float32
	// Flag encoding quality control status
	Controlinfo *string
	// Flag encoding quality control status
	Useinfo *string
	// Number of tests that failed?
	Cfailed *string
}

func (o *Flag) ToRow() []any {
	// "timeseries", "obstime", "corrected","controlinfo", "useinfo", "cfailed"
	return []any{o.Id, o.Obstime, o.Original, o.Corrected, o.Controlinfo, o.Useinfo, o.Cfailed}
}
