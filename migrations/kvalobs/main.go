package kvalobs

import (
	"migrate/kvalobs/dump"
	port "migrate/kvalobs/import"
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

// type timespan struct {
// 	fromtime time.Time
// 	totime   time.Time
// }
//
// type Kvalobs struct {
// 	Hosts     []string
// 	Ports     []string
// 	DBs       []string
// 	Usernames []string
// 	Passwords []string
// }

// TODO: should we use this one as default or process all times
var FROMTIME time.Time = time.Date(2006, 01, 01, 00, 00, 00, 00, time.UTC)

// type BaseConfig struct {
// 	BaseDir  string     `short:"p" long:"path" default:"./dumps" description:"Location the dumped data will be stored in"`
// 	FromTime *time.Time `long:"from" description:"Fetch data only starting from this timestamp"`
// 	ToTime   *time.Time `long:"to" description:"Fetch data only until this timestamp"`
// 	Ts       []int32    `long:"ts" description:"Optional comma separated list of timeseries. By default all available timeseries are processed"`
// 	Stations []int32    `long:"station" description:"Optional comma separated list of station numbers. By default all available station numbers are processed"`
// 	TypeIds  []int32    `long:"typeid" description:"Optional comma separated list of type IDs. By default all available type IDs are processed"`
// 	ParamIds []int32    `long:"paramid" description:"Optional comma separated list of param IDs. By default all available param IDs are processed"`
// 	Sensors  []int32    `long:"sensor" description:"Optional comma separated list of sensors. By default all available sensors are processed"`
// 	Levels   []int32    `long:"level" description:"Optional comma separated list of levels. By default all available levels are processed"`
// }

type Cmd struct {
	Dump   dump.Config `command:"dump" description:"Dump tables from Kvalobs to CSV"`
	Import port.Config `command:"import" description:"Import CSV file dumped from Kvalobs"`
}
