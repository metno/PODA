package db

import (
	"time"
)

// This is basically the same as lard.Label (except for ParamCode)
// type TSLabel struct {
// 	StationID int32  `db:"stationid"`
// 	TypeID    int32  `db:"typeid"`
// 	ParamID   int32  `db:"paramid"`
// 	Sensor    *int32 `db:"sensor"`
// 	Level     *int32 `db:"level"`
// // ParamCode string `db:"name,omitempty"`
// }

// Kvalobs observation row
type DataObs struct {
	Obstime     time.Time `db:"obstime"`
	Original    float64   `db:"original"`
	Tbtime      time.Time `db:"tbtime"`
	Corrected   float64   `db:"corrected"`
	Controlinfo *string   `db:"controlinfo"`
	Useinfo     *string   `db:"useinfo"`
	Cfailed     *string   `db:"cfailed"`
}

type TextObs struct {
	Obstime  time.Time `db:"obstime"`
	Original string    `db:"original"`
	Tbtime   time.Time `db:"tbtime"`
}

type Data = []*DataObs
type Text = []*TextObs
