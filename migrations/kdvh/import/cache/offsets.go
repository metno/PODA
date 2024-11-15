package cache

import (
	"log/slog"
	"os"

	"github.com/gocarina/gocsv"
	"github.com/rickb777/period"
)

// Map of offsets used to correct KDVH times for specific parameters
type OffsetMap = map[StinfoKey]period.Period

// Caches how to modify the obstime (in KDVH) for certain paramids
func cacheParamOffsets() OffsetMap {
	cache := make(OffsetMap)

	type CSVRow struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int32  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}

	csvfile, err := os.Open("kdvh/product_offsets.csv")
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[StinfoKey{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	return cache
}
