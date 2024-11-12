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

func fileExists(filename string, overwrite bool) error {
	if _, err := os.Stat(filename); err == nil && !overwrite {
		return errors.New(
			fmt.Sprintf(
				"Skipping dump of '%s' because dumped file already exists and the --overwrite flag was not provided",
				filename,
			))
	}
	return nil
}

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
		yearPath := filepath.Join(path, fmt.Sprint(year))
		if err := os.MkdirAll(yearPath, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		filename := filepath.Join(yearPath, meta.element+".csv")
		if err := fileExists(filename, meta.overwrite); err != nil {
			slog.Warn(err.Error())
			continue
		}

		rows, err := conn.Query(query, meta.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			continue
		}

		if err := dumpToFile(filename, rows); err != nil {
			slog.Error(err.Error())
			continue
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
		yearPath := filepath.Join(path, fmt.Sprint(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		filename := filepath.Join(yearPath, meta.element+".csv")
		if err := fileExists(filename, meta.overwrite); err != nil {
			slog.Warn(err.Error())
			continue
		}

		rows, err := conn.Query(query, meta.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			continue
		}

		if err := dumpToFile(filename, rows); err != nil {
			slog.Error(err.Error())
			continue
		}
	}

	return nil
}

// T_HOMOGEN_MONTH contains seasonal and annual data, plus other derivative
// data combining both of these. We decided to dump only the monthly data (season BETWEEN 1 AND 12) for
//   - TAM (mean hourly temperature), and
//   - RR (hourly precipitations, note that in Stinfosys this parameter is 'RR_1')
//
// We calculate the other data on the fly (outside this program) if needed.
func dumpHomogenMonth(path string, meta DumpMeta, conn *sql.DB) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(err.Error())
		return err
	}

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

	if err := dumpToFile(filename, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataOnly(path string, meta DumpMeta, conn *sql.DB) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(err.Error())
		return err
	}

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

	if err := dumpToFile(filename, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataAndFlags(path string, meta DumpMeta, conn *sql.DB) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(err.Error())
		return err
	}

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

	if err := dumpToFile(path, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpToFile(filename string, rows *sql.Rows) error {
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
