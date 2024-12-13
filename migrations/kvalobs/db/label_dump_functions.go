package db

import (
	"context"
	"log/slog"
	"migrate/utils"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Here are implemented the `LabelDumpFunc` stored inside the Table struct

const OBSDATA_QUERY string = `SELECT DISTINCT paramid, sensor::int, level FROM obsdata
JOIN observations USING(observationid)
WHERE stationid = $1
  AND typeid = $2
  AND ($3::timestamp IS NULL OR obstime >= $3)
  AND ($4::timestamp IS NULL OR obstime < $4)`

const OBSTEXTDATA_QUERY string = `SELECT DISTINCT paramid FROM obstextdata
JOIN observations USING(observationid)
WHERE stationid = $1
  AND typeid = $2
  AND ($3::timestamp IS NULL OR obstime >= $3)
  AND ($4::timestamp IS NULL OR obstime < $4)`

type StationType struct {
	stationid int32
	typeid    int32
}

// Lazily initialized slice of distinct stationids and typeids from the `observations` table
var UNIQUE_STATIONS_TYPES []*StationType = nil

func initUniqueStationsAndTypeIds(timespan *utils.TimeSpan, pool *pgxpool.Pool) error {
	if UNIQUE_STATIONS_TYPES != nil {
		return nil
	}

	rows, err := pool.Query(context.TODO(),
		`SELECT DISTINCT stationid, typeid FROM observations
            WHERE ($1::timestamp IS NULL OR obstime >= $1)
              AND ($2::timestamp IS NULL OR obstime < $2)
            ORDER BY stationid`,
		timespan.From, timespan.To)
	if err != nil {
		return err
	}

	UNIQUE_STATIONS_TYPES = make([]*StationType, 0, rows.CommandTag().RowsAffected())
	UNIQUE_STATIONS_TYPES, err = pgx.AppendRows(UNIQUE_STATIONS_TYPES, rows, func(row pgx.CollectableRow) (*StationType, error) {
		var label StationType
		err := row.Scan(&label.stationid, &label.typeid)
		return &label, err
	})

	if err != nil {
		return err
	}
	return nil
}

func dumpDataLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool, maxConn int) ([]*Label, error) {
	// First query stationid and typeid from observations
	// Then query paramid, sensor, level from obsdata
	// This is faster than querying all of them together from data
	slog.Info("Querying data labels...")
	if err := initUniqueStationsAndTypeIds(timespan, pool); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	bar := utils.NewBar(len(UNIQUE_STATIONS_TYPES), "Stations")
	var labels []*Label
	var wg sync.WaitGroup

	semaphore := make(chan struct{}, maxConn)
	for _, s := range UNIQUE_STATIONS_TYPES {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer func() {
				bar.Add(1)
				wg.Done()
				<-semaphore
			}()

			rows, err := pool.Query(context.TODO(), OBSDATA_QUERY, s.stationid, s.typeid, timespan.From, timespan.To)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			innerLabels := make([]*Label, 0, rows.CommandTag().RowsAffected())
			innerLabels, err = pgx.AppendRows(innerLabels, rows, func(row pgx.CollectableRow) (*Label, error) {
				label := Label{StationID: s.stationid, TypeID: s.typeid}
				err := row.Scan(&label.ParamID, &label.Sensor, &label.Level)
				return &label, err
			})

			if err != nil {
				slog.Error(err.Error())
				return
			}

			labels = slices.Concat(labels, innerLabels)
		}()
	}

	wg.Wait()

	return labels, nil
}

func dumpTextLabels(timespan *utils.TimeSpan, pool *pgxpool.Pool, maxConn int) ([]*Label, error) {
	// First query stationid and typeid from observations
	// Then query paramid from obstextdata
	// This is faster than querying all of them together from data
	slog.Info("Querying text labels...")
	if err := initUniqueStationsAndTypeIds(timespan, pool); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	bar := utils.NewBar(len(UNIQUE_STATIONS_TYPES), "Stations")
	var labels []*Label
	var wg sync.WaitGroup

	semaphore := make(chan struct{}, maxConn)
	for _, s := range UNIQUE_STATIONS_TYPES {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer func() {
				bar.Add(1)
				wg.Done()
				<-semaphore
			}()

			rows, err := pool.Query(context.TODO(), OBSTEXTDATA_QUERY, s.stationid, s.typeid, timespan.From, timespan.To)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			innerLabels := make([]*Label, 0, rows.CommandTag().RowsAffected())
			innerLabels, err = pgx.AppendRows(innerLabels, rows, func(row pgx.CollectableRow) (*Label, error) {
				label := Label{StationID: s.stationid, TypeID: s.typeid}
				err := row.Scan(&label.ParamID)
				return &label, err
			})

			if err != nil {
				slog.Error(err.Error())
				return
			}
			labels = slices.Concat(labels, innerLabels)
		}()
	}
	wg.Wait()
	return labels, nil
}
