package db

import (
	"errors"
	"fmt"
	"migrate/lard"
	"migrate/utils"
	"strconv"
	"strings"
)

// Serialize lard.Label to CSV file name
func LabelToFilename(ts *lard.Label) string {
	var sensor, level string
	if ts.Sensor != nil {
		sensor = fmt.Sprint(ts.Sensor)
	}
	if ts.Level != nil {
		level = fmt.Sprint(ts.Level)
	}
	return fmt.Sprintf("%v_%v_%v_%v_%v.csv", ts.StationID, ts.TypeID, ts.ParamID, sensor, level)
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
func LabelFromFilename(filename string) (*lard.Label, error) {
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

	return &lard.Label{
		StationID: *converted[0],
		TypeID:    *converted[1],
		ParamID:   *converted[2],
		Sensor:    converted[3],
		Level:     converted[4],
	}, nil
}
