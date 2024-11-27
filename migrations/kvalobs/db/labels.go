package db

import (
	"errors"
	"fmt"
	"migrate/utils"
	"strconv"
	"strings"
)

// Kvalobs specific label
type Label[T int32 | string] struct {
	StationID int32
	TypeID    int32
	ParamID   int32
	// These two are not present in the `text_data` tabl
	Sensor *T // bpchar(1) in `data` table
	Level  *int32
}

func (l *Label[T]) sensorLevelString() (string, string) {
	var sensor, level string
	if l.Sensor != nil {
		sensor = fmt.Sprint(*l.Sensor)
	}
	if l.Level != nil {
		level = fmt.Sprint(*l.Level)
	}
	return sensor, level
}

func (l *Label[T]) ToFilename() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf("%v_%v_%v_%v_%v.csv", l.StationID, l.TypeID, l.ParamID, sensor, level)
}

func (l *Label[T]) ToString() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf(
		"%v - %v - %v - %v - %v",
		l.StationID,
		l.ParamID,
		l.TypeID,
		sensor,
		level,
	)
}

func parseFilenameFields(s *string) (*int32, error) {
	// TODO: probably there is a better way to do this without defining a gazillion functions
	if *s == "" {
		return nil, nil
	}
	res, err := strconv.ParseInt(*s, 10, 32)
	if err != nil {
		return nil, err
	}
	out := int32(res)
	return &out, nil
}

// Deserialize filename to lard.Label
func LabelFromFilename(filename string) (*Label[int32], error) {
	name := strings.TrimSuffix(filename, ".csv")

	fields := strings.Split(name, "_")
	if len(fields) < 5 {
		return nil, errors.New("Too few fields in file name: " + filename)
	}

	ptrs := make([]*string, len(fields))
	for i := range ptrs {
		ptrs[i] = &fields[i]
	}

	converted, err := utils.TryMap(ptrs, parseFilenameFields)
	if err != nil {
		return nil, err
	}

	return &Label[int32]{
		StationID: *converted[0],
		TypeID:    *converted[1],
		ParamID:   *converted[2],
		Sensor:    converted[3],
		Level:     converted[4],
	}, nil
}
