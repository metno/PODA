package dump

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
)

// Function used to dump the KDVH table, see below
type DumpFunction func(path string, meta DumpArgs, pool *pgxpool.Pool) error
type DumpArgs struct {
	element   string
	station   string
	dataTable string
	flagTable string
	overwrite bool
	logStr    string
}

func getDumpFunc(table *db.Table) DumpFunction {
	switch table.TableName {
	case "T_METARDATA", "T_HOMOGEN_DIURNAL":
		return dumpDataOnly
	case "T_SECOND_DATA", "T_MINUTE_DATA", "T_10MINUTE_DATA":
		return dumpByYear
	case "T_HOMOGEN_MONTH":
		return dumpHomogenMonth
	}
	return dumpDataAndFlags
}

func fileExists(filename string, overwrite bool) error {
	if _, err := os.Stat(filename); err == nil && !overwrite {
		return errors.New(
			fmt.Sprintf(
				"Skipping dump of %q because dumped file already exists and the --overwrite flag was not provided",
				filename,
			))
	}
	return nil
}

// Helper function for dumpByYear functinos Fetch min and max year from table, needed for tables that are dumped by year
func fetchYearRange(tableName, station string, pool *pgxpool.Pool) (int64, int64, error) {
	var beginStr, endStr string
	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = $1", tableName)

	if err := pool.QueryRow(context.TODO(), query, station).Scan(&beginStr, &endStr); err != nil {
		return 0, 0, fmt.Errorf("Could not query row: %v", err)
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("Could not parse year %q: %s", beginStr, err)
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("Could not parse year %q: %s", endStr, err)
	}

	return begin, end, nil
}

// This function is used when the table contains large amount of data
// (T_SECOND, T_MINUTE, T_10MINUTE)
func dumpByYear(path string, meta DumpArgs, pool *pgxpool.Pool) error {
	dataBegin, dataEnd, err := fetchYearRange(meta.dataTable, meta.station, pool)
	if err != nil {
		return err
	}

	flagBegin, flagEnd, err := fetchYearRange(meta.flagTable, meta.station, pool)
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
			slog.Error(meta.logStr + err.Error())
			continue
		}

		filename := filepath.Join(yearPath, meta.element+".csv")
		if err := fileExists(filename, meta.overwrite); err != nil {
			slog.Warn(meta.logStr + err.Error())
			continue
		}

		rows, err := pool.Query(context.TODO(), query, meta.station, year)
		if err != nil {
			slog.Error(meta.logStr + fmt.Sprint("Could not query KDVH: ", err))
			continue
		}

		if err := writeToCsv(filename, rows); err != nil {
			slog.Error(meta.logStr + err.Error())
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
func dumpHomogenMonth(path string, meta DumpArgs, pool *pgxpool.Pool) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(meta.logStr + err.Error())
		return err
	}

	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		// NOTE: adding a dummy argument is the only way to suppress this stupid warning
		meta.element, "",
	)

	rows, err := pool.Query(context.TODO(), query, meta.station)
	if err != nil {
		slog.Error(meta.logStr + err.Error())
		return err
	}

	if err := writeToCsv(filename, rows); err != nil {
		slog.Error(meta.logStr + err.Error())
		return err
	}

	return nil
}

// This function is used to dump tables that don't have a FLAG table,
// (T_METARDATA, T_HOMOGEN_DIURNAL)
func dumpDataOnly(path string, meta DumpArgs, pool *pgxpool.Pool) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(meta.logStr + err.Error())
		return err
	}

	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s 
        WHERE %[1]s IS NOT NULL AND stnr = $1`,
		meta.element,
		meta.dataTable,
	)

	rows, err := pool.Query(context.TODO(), query, meta.station)
	if err != nil {
		slog.Error(meta.logStr + err.Error())
		return err
	}

	if err := writeToCsv(filename, rows); err != nil {
		slog.Error(meta.logStr + err.Error())
		return err
	}

	return nil
}

// This is the default dump function.
// It selects both data and flag tables for a specific (station, element) pair,
// and then performs a full outer join on the two subqueries
func dumpDataAndFlags(path string, meta DumpArgs, pool *pgxpool.Pool) error {
	filename := filepath.Join(path, meta.element+".csv")
	if err := fileExists(filename, meta.overwrite); err != nil {
		slog.Warn(meta.logStr + err.Error())
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

	rows, err := pool.Query(context.TODO(), query, meta.station)
	if err != nil {
		slog.Error(meta.logStr + err.Error())
		return err
	}

	if err := writeToCsv(filename, rows); err != nil {
		if !errors.Is(err, EMPTY_QUERY_ERR) {
			slog.Error(meta.logStr + err.Error())
		}
		return err
	}

	return nil
}
