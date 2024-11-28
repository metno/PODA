package db

import (
	"bufio"
	"log/slog"
	"migrate/lard"
	"os"

	"github.com/gocarina/gocsv"
)

func ReadLabelCSV(filename string) (labels []*KvLabel, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// TODO: maybe I should preallocate slice size if I can?
	err = gocsv.UnmarshalFile(file, &labels)
	return labels, err
}

func ReadDataCSV(tsid int32, filename string) ([][]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewScanner(file)

	// TODO: maybe I should preallocate slice size if I can?
	var data [][]any
	for reader.Scan() {
		var obs lard.DataObs

		err = gocsv.UnmarshalString(reader.Text(), &obs)
		if err != nil {
			return nil, err
		}

		// Kvalobs does not have IDs so we have to add it here
		obs.Id = tsid

		row := obs.ToRow()
		data = append(data, row)
	}

	return data, nil
}

func ReadTextCSV(tsid int32, filename string) ([][]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewScanner(file)

	// TODO: maybe I should preallocate slice size if I can?
	var data [][]any
	for reader.Scan() {
		var obs lard.TextObs

		err = gocsv.UnmarshalString(reader.Text(), &obs)
		if err != nil {
			return nil, err
		}

		// Kvalobs does not have IDs so we have to add it here
		obs.Id = tsid

		row := obs.ToRow()
		data = append(data, row)
	}

	return data, nil
}

type Rower interface {
	ToRow() []any
}

func ReadSeriesCSV[T Rower](tsid int32, filename string) ([][]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewScanner(file)

	// TODO: maybe I should preallocate slice size if I can?
	var data [][]any
	for reader.Scan() {
		var obs T

		err = gocsv.UnmarshalString(reader.Text(), &obs)
		if err != nil {
			return nil, err
		}

		// Kvalobs does not have IDs so we have to add it here
		// obs.Id = tsid

		row := obs.ToRow()
		data = append(data, row)
	}

	return data, nil
}
