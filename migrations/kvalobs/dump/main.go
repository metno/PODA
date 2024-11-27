package dump

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
)

// Same timeseries could be in both 'data' and 'text_data' tables
// First of all, why?
// Second, do we care?
// func readDataAndText(label *lard.Label, pool *pgxpool.Pool, config *DumpConfig) Data {
//     // Supposed to join text anf number data to single slice
//     return nil
// }
//
// TODO: not sure what to do with this one
// func joinTS(first, second []lard.Label)

type Table[T int32 | string, S db.DataSeries | db.TextSeries] struct {
	Name    string
	LabelFn LabelFunc[T]
	ObsFn   ObsFunc[T, S]
}

// Function used to query labels from kvalobs given an optional timespan
type LabelFunc[T int32 | string] func(timespan *TimeSpan, pool *pgxpool.Pool) ([]*db.Label[T], error)

// Function used to query timeseries from kvalobs for a specific label
type ObsFunc[T int32 | string, S db.DataSeries | db.TextSeries] func(label *db.Label[T], timespan *TimeSpan, pool *pgxpool.Pool) (S, error)

type DB struct {
	Name       string
	ConnEnvVar string
}

type Config struct {
	db.BaseConfig[string]
	UpdateLabels bool   `help:"Overwrites the label CSV files"`
	Database     string `arg:"--db" help:"Which database to dump from. Choices: ['kvalobs', 'histkvalobs']"`
	Table        string `help:"Which table to dump. Choices: ['data', 'text']"`
}

type TimeSpan struct {
	From *time.Time
	To   *time.Time
}

func (config *Config) TimeSpan() *TimeSpan {
	return &TimeSpan{From: config.FromTime.Inner(), To: config.ToTime.Inner()}
}

func (config *Config) ChosenDB(name string) bool {
	return config.Database == "" || config.Database == name
}

func (config *Config) ChosenTable(name string) bool {
	return config.Table == "" || config.Table == name
}

func (config *Config) Execute() {
	kvalobs := DB{Name: "kvalobs", ConnEnvVar: "KVALOBS_CONN_STRING"}
	histkvalobs := DB{Name: "histkvalobs", ConnEnvVar: "HISTKVALOBS_CONN_STRING"}

	dataTable := Table[string, db.DataSeries]{
		Name:    "data",
		LabelFn: getDataLabels,
		ObsFn:   getDataSeries,
	}

	textTable := Table[string, db.TextSeries]{
		Name:    "text",
		LabelFn: getTextLabels,
		ObsFn:   getTextSeries,
	}

	if config.ChosenDB(kvalobs.Name) {
		dumpDB(kvalobs, dataTable, textTable, config)
	}

	if config.ChosenDB(histkvalobs.Name) {
		dumpDB(histkvalobs, dataTable, textTable, config)
	}
}
