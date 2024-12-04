package port

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Returns a DataTable for import
func DataTable(path string) kvalobs.Table {
	return kvalobs.Table{
		Path:   filepath.Join(path, kvalobs.DATA_TABLE_NAME),
		Import: importData,
	}
}

func importData(tsid int32, label *kvalobs.Label, filename, logStr string, pool *pgxpool.Pool) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	if label.IsSpecialCloudType() {
		text, err := parseSpecialCloudType(tsid, rowCount, scanner)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		count, err := lard.InsertTextData(text, pool, logStr)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		return count, nil
	}

	data, flags, err := parseDataCSV(tsid, rowCount, scanner)
	count, err := lard.InsertData(data, pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	if err := lard.InsertFlags(flags, pool, logStr); err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	return count, nil
}

func parseDataCSV(tsid int32, rowCount int, scanner *bufio.Scanner) ([][]any, [][]any, error) {
	data := make([][]any, 0, rowCount)
	flags := make([][]any, 0, rowCount)
	var originalPtr, correctedPtr *float32
	for scanner.Scan() {
		// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
		// We don't parse tbtime
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, nil, err
		}

		obsvalue64, err := strconv.ParseFloat(fields[1], 32)
		if err != nil {
			return nil, nil, err
		}

		corrected64, err := strconv.ParseFloat(fields[1], 32)
		if err != nil {
			return nil, nil, err
		}

		original := float32(obsvalue64)
		corrected := float32(corrected64)

		// Filter out special values that in Kvalobs stand for null observations
		if !slices.Contains(kvalobs.NULL_VALUES, original) {
			originalPtr = &original
		}
		if !slices.Contains(kvalobs.NULL_VALUES, corrected) {
			correctedPtr = &corrected
		}

		// Original value is inserted in main data table
		lardObs := lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    originalPtr,
		}

		var cfailed *string
		if fields[6] != "" {
			cfailed = &fields[6]
		}

		flag := lard.Flag{
			Id:          tsid,
			Obstime:     obstime,
			Original:    originalPtr,
			Corrected:   correctedPtr,
			Controlinfo: &fields[4], // Never null, has default values in KValobs
			Useinfo:     &fields[5], // Never null, has default values in KValobs
			Cfailed:     cfailed,
		}

		data = append(data, lardObs.ToRow())
		flags = append(flags, flag.ToRow())
	}

	return data, flags, nil
}

// Function for paramids 305, 306, 307, 308 that were stored as scalar data
// but should be treated as text
func parseSpecialCloudType(tsid int32, rowCount int, scanner *bufio.Scanner) ([][]any, error) {
	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
		// TODO: should parse everything and return the flags?
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, err
		}

		lardObs := lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		}

		data = append(data, lardObs.ToRow())
	}

	return data, nil
}
