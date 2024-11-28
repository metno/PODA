package db

import (
	"time"

	"migrate/utils"
)

// TODO: should we use this one as default or process all times
// TODO: it looks like histkvalobs has data only starting from 2023-06-01?
var FROMTIME time.Time = time.Date(2006, 01, 01, 00, 00, 00, 00, time.UTC)

type BaseConfig[T int32 | string] struct {
	Path     string           `arg:"-p" default:"./dumps" help:"Location the dumped data will be stored in"`
	FromTime *utils.Timestamp `arg:"--from" help:"Fetch data only starting from this date-only timestamp"`
	ToTime   *utils.Timestamp `arg:"--to" help:"Fetch data only until this date-only timestamp"`
	Database string           `arg:"--db" help:"Which database to process, all by default. Choices: ['kvalobs', 'histkvalobs']"`
	Table    string           `help:"Which table to process, all by default. Choices: ['data', 'text']"`
	Stations []int32          `help:"Optional space separated list of station numbers"`
	TypeIds  []int32          `help:"Optional space separated list of type IDs"`
	ParamIds []int32          `help:"Optional space separated list of param IDs"`
	Sensors  []T              `help:"Optional space separated list of sensors"`
	Levels   []int32          `help:"Optional space separated list of levels"`
}

func (config *BaseConfig[T]) ShouldProcessLabel(label *Label[T]) bool {
	// (config.Ts == nil || slices.Contains(config.Ts, ts.ID)) ||
	return utils.IsEmptyOrContains(config.Stations, label.StationID) &&
		utils.IsEmptyOrContains(config.TypeIds, label.TypeID) &&
		utils.IsEmptyOrContains(config.ParamIds, label.ParamID) &&
		// TODO: these two should never be null anyway?
		utils.IsEmptyOrContainsPtr(config.Sensors, label.Sensor) &&
		utils.IsEmptyOrContainsPtr(config.Levels, label.Level)
}

func (config *BaseConfig[T]) TimeSpan() *utils.TimeSpan {
	return &utils.TimeSpan{From: config.FromTime.Inner(), To: config.ToTime.Inner()}
}

// Check if the `--db` flag was passed in
func (config *BaseConfig[T]) ChosenDB(name string) bool {
	return config.Database == "" || config.Database == name
}

// Check if the `--table` flag was passed in
func (config *BaseConfig[T]) ChosenTable(name string) bool {
	return config.Table == "" || config.Table == name
}
