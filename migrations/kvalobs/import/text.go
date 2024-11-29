package port

import (
	"bufio"
	"log/slog"
	"migrate/kvalobs/db"
	"migrate/lard"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Returns a TextTable for import
func TextTable(path string) db.TextTable {
	return db.TextTable{
		Path:    filepath.Join(path, db.TEXT_TABLE_NAME),
		Import:  lard.InsertTextData,
		ReadCSV: ReadTextCSV,
	}
}

func ReadTextCSV(tsid int32, filename string) ([][]any, [][]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, nil, err
	}
	defer file.Close()

	reader := bufio.NewScanner(file)

	// Parse number of rows
	reader.Scan()
	rowCount, _ := strconv.Atoi(reader.Text())

	// Skip header
	reader.Scan()

	// Parse observations
	data := make([][]any, 0, rowCount)
	for reader.Scan() {
		// obstime, original, tbtime
		fields := strings.Split(reader.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, nil, err
		}

		lardObs := lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		}

		data = append(data, lardObs.ToRow())
	}

	// Text obs are not flagged
	return data, nil, nil
}
