package kdvh

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

// Fetch min and max year from table, needed for tables that are dumped by year
func fetchYearRange(tableName, station string, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string
	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = $1", tableName)

	if err := conn.QueryRow(query, station).Scan(&beginStr, &endStr); err != nil {
		slog.Error(fmt.Sprint("Could not query row: ", err))
		return 0, 0, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not parse year '%s': %s", beginStr, err))
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not parse year '%s': %s", endStr, err))
		return 0, 0, err
	}

	return begin, end, nil
}

func dumpByYearDataOnly(path string, meta DumpMeta, conn *sql.DB) error {
	begin, end, err := fetchYearRange(meta.dataTable, meta.station, conn)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s 
        WHERE %[1]s IS NOT NULL
        AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2`,
		meta.element,
		meta.dataTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, meta.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			return err
		}

		path := filepath.Join(path, fmt.Sprint(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := dumpToFile(path, meta.element, rows); err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return nil
}

func dumpByYear(path string, meta DumpMeta, conn *sql.DB) error {
	dataBegin, dataEnd, err := fetchYearRange(meta.dataTable, meta.station, conn)
	if err != nil {
		return err
	}

	flagBegin, flagEnd, err := fetchYearRange(meta.flagTable, meta.station, conn)
	if err != nil {
		return err
	}

	begin := min(dataBegin, flagBegin)
	end := max(dataEnd, flagEnd)

	query := fmt.Sprintf(
		`SELECT
            dato AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, stnr, %[1]s FROM %[2]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) d
        FULL OUTER JOIN
            (SELECT dato, stnr, %[1]s FROM %[3]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) f
        USING (dato)`,
		meta.element,
		meta.dataTable,
		meta.flagTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, meta.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			return err
		}

		yearPath := filepath.Join(path, fmt.Sprint(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := dumpToFile(yearPath, meta.element, rows); err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return nil
}

func dumpHomogenMonth(path string, meta DumpMeta, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		// NOTE: adding a dummy argument is the only way to suppress this stupid warning
		meta.element, "",
	)

	rows, err := conn.Query(query, meta.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(path, meta.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataOnly(path string, meta DumpMeta, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s 
        WHERE %[1]s IS NOT NULL AND stnr = $1`,
		meta.element,
		meta.dataTable,
	)

	rows, err := conn.Query(query, meta.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(path, meta.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataAndFlags(path string, meta DumpMeta, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT
            dato AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
        FULL OUTER JOIN
            (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
        USING (dato)`,
		meta.element,
		meta.dataTable,
		meta.flagTable,
	)

	rows, err := conn.Query(query, meta.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(path, meta.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpToFile(path, element string, rows *sql.Rows) error {
	filename := filepath.Join(path, element+".csv")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	lines, err := sortRows(rows)
	if err != nil {
		return err
	}

	err = writeElementFile(lines, file)
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// Struct representing a single record in the output CSV file
type Record struct {
	time time.Time
	data sql.NullString
	flag sql.NullString
}

// Scans the rows and collects them in a slice of chronologically sorted lines
func sortRows(rows *sql.Rows) ([]Record, error) {
	defer rows.Close()

	// TODO: if we use pgx we might be able to preallocate the right size
	var records []Record
	var record Record

	for rows.Next() {
		if err := rows.Scan(&record.time, &record.data, &record.flag); err != nil {
			return nil, errors.New("Could not scan rows: " + err.Error())
		}
		records = append(records, record)
	}

	slices.SortFunc(records, func(a, b Record) int {
		return a.time.Compare(b.time)
	})

	return records, rows.Err()
}

// Format string for date field in CSV files
const TIMEFORMAT string = "2006-01-02_15:04:05"

// Writes queried (time | data | flag) columns to CSV
func writeElementFile(lines []Record, file io.Writer) error {
	// Write number of lines as header
	file.Write([]byte(fmt.Sprintf("%v\n", len(lines))))

	writer := csv.NewWriter(file)

	record := make([]string, 3)
	for _, l := range lines {
		record[0] = l.time.Format(TIMEFORMAT)
		record[1] = l.data.String
		record[2] = l.flag.String

		if err := writer.Write(record); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	writer.Flush()
	return writer.Error()
}
