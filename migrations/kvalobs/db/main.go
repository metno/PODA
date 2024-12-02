package db

import (
	"time"
)

// Kvalobs is composed of two databases
// 1) `kvalobs` for fresh data?
// 2) `histkvalobs` for data older than <not sure how long, 2-3 months?>
//
// Both contain the same tables:
// - `algorithms`: stores procedure code (!!!) for QC checks
// - `checks`: stores tags and signatures of QC tests
// - `data`: stores numerical observations, associated metadata, and QC info
//
//       Column    |            Type             | Collation | Nullable |          Default
//    -------------+-----------------------------+-----------+----------+----------------------------
//     stationid   | integer                     |           | not null |
//     obstime     | timestamp without time zone |           | not null |
//     original    | double precision            |           | not null |
//     paramid     | integer                     |           | not null |
//     tbtime      | timestamp without time zone |           | not null |
//     typeid      | integer                     |           | not null |
//     sensor      | character(1)                |           |          | '0'::bpchar
//     level       | integer                     |           |          | 0
//     corrected   | double precision            |           | not null |
//     controlinfo | character(16)               |           |          | '0000000000000000'::bpchar
//     useinfo     | character(16)               |           |          | '0000000000000000'::bpchar
//     cfailed     | text                        |           |          |
//
// - `default_missing`:
// - `default_missing_values`: default values for some paramids (-32767)
// - `model`: stores model names
// - `model_data`: stores model data for different stations, paramids, etc.
// - `param`: part of stinfosys `param` table
//      Column    |  Type   | Collation | Nullable | Default
//   -------------+---------+-----------+----------+---------
//    paramid     | integer |           | not null |
//    name        | text    |           | not null |
//    description | text    |           |          |
//    unit        | text    |           |          |
//    level_scale | integer |           |          | 0
//    comment     | text    |           |          |
//    scalar      | boolean |           |          | true
//
//   TODO: should we dump this one as well?
// - `pdata`: same structure as data?
// - `station`: station metadata such as (lat, lon, height, name, wmonr, etc)
// - `station_metadata`: Stores fromtime and totime for `stationid` and optionally `paramid`.
//                       `typeid`, `sensor`, and `level` are always NULL.
//
// - `text_data`: Similar to `data`, but without QC info nor sensor/level
//
//      Column   |            Type             | Collation | Nullable | Default
//    -----------+-----------------------------+-----------+----------+---------
//     stationid | integer                     |           | not null |
//     obstime   | timestamp without time zone |           | not null |
//     original  | text                        |           | not null |
//     paramid   | integer                     |           | not null |
//     tbtime    | timestamp without time zone |           | not null |
//     typeid    | integer                     |           | not null |
//
// NOTE: In `histkvalobs` only `data` and `text_data` are non-empty.
//
// IMPORTANT: considerations for migrations to LARD
//            - LARD stores Timeseries labels (stationid, paramid, typeid, sensor, level) in a separate table
//            - (sensor, level) can be NULL, while in Kvalobs they have default values (0,0)
//                  => POSSIBLE INCONSISTENCY when importing to LARD
//            - Timestamps are UTC
//            - Kvalobs doesn't have the concept of timeseries ID,
//              instead there is a sequential ID associated with each observation row

const DATA_TABLE_NAME string = "data"
const TEXT_TABLE_NAME string = "text" // text_data

// Special values that are treated as NULL in Kvalobs
// TODO: are there more values we should be looking for?
var NULL_VALUES []float32 = []float32{-32767, -32766}

type DataSeries = []*DataObs

// Kvalobs data table observation row
type DataObs struct {
	Obstime     time.Time `db:"obstime"`
	Original    float64   `db:"original"`
	Tbtime      time.Time `db:"tbtime"`
	Corrected   float64   `db:"corrected"`
	Controlinfo *string   `db:"controlinfo"`
	Useinfo     *string   `db:"useinfo"`
	Cfailed     *string   `db:"cfailed"`
}

type TextSeries = []*TextObs

// Kvalobs text_data table observation row
type TextObs struct {
	Obstime  time.Time `db:"obstime"`
	Original string    `db:"original"`
	Tbtime   time.Time `db:"tbtime"`
}

type DB struct {
	Name       string
	ConnEnvVar string
}

func InitDBs() (DB, DB) {
	kvalobs := DB{Name: "kvalobs", ConnEnvVar: "KVALOBS_CONN_STRING"}
	histkvalobs := DB{Name: "histkvalobs", ConnEnvVar: "HISTKVALOBS_CONN_STRING"}
	return kvalobs, histkvalobs
}
