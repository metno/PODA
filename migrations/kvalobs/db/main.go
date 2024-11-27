package db

import (
	"time"
)

// Kvalobs is composed of two databases
// 1) `kvalobs` for fresh data
// 2) `histkvalobs` for data older than <not sure how long, 2-3 months?>
//
// Both contain the same tables:
// - `algorithms`: empty (???) - stores procedure info for QC checks
// - `checks`: empty (???)
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
// - `default_missing_values`:
//
// - `model`:
//     Column  |  Type   | Collation | Nullable | Default
//    ---------+---------+-----------+----------+---------
//     modelid | integer |           | not null |
//     name    | text    |           |          |
//     comment | text    |           |          |
//
// - `model_data`:
//      Column   |            Type             | Collation | Nullable | Default
//    -----------+-----------------------------+-----------+----------+---------
//     stationid | integer                     |           | not null |
//     obstime   | timestamp without time zone |           | not null |
//     paramid   | integer                     |           | not null |
//     level     | integer                     |           | not null |
//     modelid   | integer                     |           | not null |
//     original  | double precision            |           |          |
//
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
// - `pdata`: same as `data` without the `original` column and all `paramid` null???
// - `station`: station metadata such as (lat, lon, height, name, wmonr, etc)
// - `station_metadata`: this one seems to map well to our `labels.met`?
//                       Problem is `typeid`, `sensor`, and `level` are always NULL
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
// In `histkvalobs` only data tables seem to be non-empty
//
// IMPORTANT: considerations for migrations to LARD
//            - LARD stores Timeseries labels (stationid, paramid, typeid, sensor, level) in a separate table
//            - (sensor, level) can be NULL, while in Kvalobs they have default values (0,0)
//                  => POSSIBLE INCONSISTENCY when importing to LARD
//            - Timestamps are UTC
//            - Kvalobs doesn't have the concept of timeseries ID,
//              instead there is a sequential ID associated with each observation row

var NULL_VALUES []float64 = []float64{-34767, -34766}

type DataSeries = []*DataObs

// Kvalobs data observation row
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

// Kvalobs text observation row
type TextObs struct {
	Obstime  time.Time `db:"obstime"`
	Original string    `db:"original"`
	Tbtime   time.Time `db:"tbtime"`
}

type Kvalobs struct {
	Name       string
	ConnEnvVar string
}
