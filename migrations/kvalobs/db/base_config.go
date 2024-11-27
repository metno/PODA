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
	Stations []int32          `help:"Optional space separated list of station numbers"`
	TypeIds  []int32          `help:"Optional space separated list of type IDs"`
	ParamIds []int32          `help:"Optional space separated list of param IDs"`
	Sensors  []T              `help:"Optional space separated list of sensors"`
	Levels   []int32          `help:"Optional space separated list of levels"`
}

func (config *BaseConfig[T]) ShouldProcessLabel(label *Label[T]) bool {
	// (config.Ts == nil || slices.Contains(config.Ts, ts.ID)) ||
	return utils.Contains(config.Stations, label.StationID) &&
		utils.Contains(config.TypeIds, label.TypeID) &&
		utils.Contains(config.ParamIds, label.ParamID) &&
		// TODO: these two should never be null anyway
		utils.NullableContains(config.Sensors, label.Sensor) &&
		utils.NullableContains(config.Levels, label.Level)
}
