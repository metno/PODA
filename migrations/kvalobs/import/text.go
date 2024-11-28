package port

import (
	"bufio"
	"log/slog"
	"migrate/kvalobs/db"
	"migrate/lard"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"
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

	// TODO: maybe I should preallocate slice size if I can?
	var data [][]any
	for reader.Scan() {
		var kvObs db.TextObs

		err = gocsv.UnmarshalString(reader.Text(), &kvObs)
		if err != nil {
			return nil, nil, err
		}

		lardObs := lard.TextObs{
			Id:      tsid,
			Obstime: kvObs.Obstime,
			Text:    &kvObs.Original,
		}

		data = append(data, lardObs.ToRow())
	}

	// Text obs are not flagged
	return data, nil, nil
}
