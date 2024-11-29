package db

import (
	"errors"
	"fmt"
	"migrate/utils"
	"strconv"
	"strings"
)

// Kvalobs specific label
type Label struct {
	StationID int32
	ParamID   int32
	TypeID    int32
	// These two are not present in the `text_data` tabl
	Sensor *int32 // bpchar(1) in `data` table
	Level  *int32
}

func (l *Label) sensorLevelString() (string, string) {
	var sensor, level string
	if l.Sensor != nil {
		sensor = fmt.Sprint(*l.Sensor)
	}
	if l.Level != nil {
		level = fmt.Sprint(*l.Level)
	}
	return sensor, level
}

func (l *Label) ToFilename() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf("%v_%v_%v_%v_%v.csv", l.StationID, l.ParamID, l.ParamID, sensor, level)
}

func (l *Label) LogStr() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf(
		"[%v - %v - %v - %v - %v]: ",
		l.StationID, l.ParamID, l.TypeID, sensor, level,
	)
}

func parseFilenameFields(s *string) (*int32, error) {
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

// Deserialize filename to LardLabel
func LabelFromFilename(filename string) (*Label, error) {
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

	return &Label{
		StationID: *converted[0],
		ParamID:   *converted[1],
		TypeID:    *converted[2],
		Sensor:    converted[3],
		Level:     converted[4],
	}, nil
}
