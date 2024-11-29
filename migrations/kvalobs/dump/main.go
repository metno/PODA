package dump

import (
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

type Config struct {
	db.BaseConfig
	UpdateLabels bool `help:"Overwrites the label CSV files"`
	MaxConn      int  `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to Kvalobs"`
}

func (config *Config) Execute() {
	kvalobs, histkvalobs := db.InitDBs()

	if config.ChosenDB(kvalobs.Name) {
		dumpDB(kvalobs, config)
	}

	if config.ChosenDB(histkvalobs.Name) {
		dumpDB(histkvalobs, config)
	}
}
