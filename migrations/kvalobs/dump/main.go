package dump

import (
	"migrate/kvalobs/db"
	"migrate/utils"
)

// TODO: there were some comments in the original script about
// the fact that the same timeseries could be in both
// 'data' and 'text_data'

type Config struct {
	db.BaseConfig
	LabelsOnly   bool `arg:"--labels-only" help:"Only dump labels"`
	UpdateLabels bool `arg:"--labels-update" help:"Overwrites the label CSV files"`
	MaxConn      int  `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to Kvalobs"`
}

func (config *Config) Execute() {
	kvalobs, histkvalobs := db.InitDBs()
	// tables := []*db.Table{}

	if utils.IsEmptyOrEqual(config.Database, kvalobs.Name) {
		dumpDB(kvalobs, config)
	}

	if utils.IsEmptyOrEqual(config.Database, histkvalobs.Name) {
		dumpDB(histkvalobs, config)
	}
}
