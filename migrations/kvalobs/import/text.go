package port

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Returns a TextTable for import
func TextTable(path string) kvalobs.Table {
	return kvalobs.Table{
		Path:   filepath.Join(path, kvalobs.TEXT_TABLE_NAME),
		Import: importText,
	}
}

func importText(tsid int32, label *kvalobs.Label, filename, logStr string, pool *pgxpool.Pool) (int64, error) {
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

	if label.IsMetarCloudType() {
		data, err := parseMetarCloudType(tsid, rowCount, scanner)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}
		count, err := lard.InsertData(data, pool, logStr)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		return count, nil
	}

	text, err := parseTextCSV(tsid, rowCount, scanner)
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

// Text obs are not flagged
func parseTextCSV(tsid int32, rowCount int, scanner *bufio.Scanner) ([][]any, error) {
	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime
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

// Function for paramids 2751, 2752, 2753, 2754 that were stored as text data
// but should instead be treated as scalars
// TODO: I'm not sure these params should be scalars given that the other cloud types are not.
// Should all cloud types be integers?
func parseMetarCloudType(tsid int32, rowCount int, scanner *bufio.Scanner) ([][]any, error) {
	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, err
		}

		val, err := strconv.ParseFloat(fields[1], 32)
		if err != nil {
			return nil, err
		}

		original := float32(val)
		lardObs := lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    &original,
		}

		data = append(data, lardObs.ToRow())
	}

	// TODO: Original text obs were not flagged, so we don't return a flags?
	// Or should we return default values?
	return data, nil
}
