package port

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"migrate/kvalobs/db"
	"migrate/lard"
)

// Returns a DataTable for import
func DataTable(path string) db.DataTable {
	return db.DataTable{
		Path:    filepath.Join(path, db.DATA_TABLE_NAME),
		Import:  lard.InsertData,
		ReadCSV: ReadDataCSV,
	}
}

func ReadDataCSV(tsid int32, filename string) ([][]any, [][]any, error) {
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
	flags := make([][]any, 0, rowCount)
	for reader.Scan() {
		// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
		// We don't parse tbtime
		fields := strings.Split(reader.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, nil, err
		}

		// TODO: probably should insert corrected to data table
		// and keep original in flags table?
		obsvalue64, err := strconv.ParseFloat(fields[1], 32)
		if err != nil {
			return nil, nil, err
		}

		corrected64, err := strconv.ParseFloat(fields[1], 32)
		if err != nil {
			return nil, nil, err
		}

		obsvalue := float32(obsvalue64)
		corrected := float32(corrected64)

		lardObs := lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    &obsvalue,
		}

		var cfailed *string = nil
		if fields[6] != "" {
			cfailed = &fields[6]
		}

		flag := lard.Flag{
			Id:          tsid,
			Obstime:     obstime,
			Corrected:   &corrected,
			Controlinfo: &fields[4],
			Useinfo:     &fields[5],
			Cfailed:     cfailed,
		}

		data = append(data, lardObs.ToRow())
		flags = append(flags, flag.ToRow())
	}

	return data, flags, nil
}
