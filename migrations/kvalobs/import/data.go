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

	"github.com/gocarina/gocsv"
)

func readDataFiles() []lard.Label {
	// TODO:
	return nil
}

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

	// TODO: maybe I should preallocate slice size if I can?
	// Parse header
	// reader.Scan()
	// rowCount, _ = strconv.Atoi(scanner.Text())
	// data := make([][]any, 0, rowCount)
	// flags := make([][]any, 0, rowCount)
	var data [][]any
	var flags [][]any

	for reader.Scan() {
		// obstime, original, tbtime, corrected, Controlinfo, Useinfo, Cfailed
		fields := strings.Split(reader.Text(), ",")

		obstime, err := time.Parse(time.RFC3339Nano, fields[0])
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
