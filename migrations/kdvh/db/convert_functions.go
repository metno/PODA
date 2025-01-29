package db

import (
	"errors"
	"strconv"

	"github.com/rickb777/period"

	"migrate/lard"
)

// Work around to return reference to consts
func addr[T any](t T) *T {
	return &t
}

func flagsAreValid(obs *KdvhObs) bool {
	if len(obs.Flags) != 5 {
		return false
	}
	_, err := strconv.ParseInt(obs.Flags, 10, 32)
	return err == nil
}

func useinfo(obs *KdvhObs) *string {
	if !flagsAreValid(obs) {
		return addr(INVALID_FLAGS)
	}
	return addr(obs.Flags + DELAY_DEFAULT)
}

// Default ConvertFunction
// NOTE: this should be the only function that can return `lard.TextObs` with non-null text data.
func convert(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	var valPtr *float32

	controlinfo := VALUE_PASSED_QC
	if obs.Data == "" {
		controlinfo = VALUE_MISSING
	}

	val, err := strconv.ParseFloat(obs.Data, 32)
	if err == nil {
		valPtr = addr(float32(val))
	}

	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}, nil
}

// This function modifies obstimes to always use totime
// This is needed because KDVH used incorrect and incosistent timestamps
func convertProduct(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	data, text, flag, err := convert(obs, ts)
	if !ts.Offset.IsZero() {
		if temp, ok := ts.Offset.AddTo(data.Obstime); ok {
			data.Obstime = temp
			text.Obstime = temp
			flag.Obstime = temp
		}
	}
	return data, text, flag, err
}

func convertEdata(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	var controlinfo string
	var valPtr *float32

	if val, err := strconv.ParseFloat(obs.Data, 32); err != nil {
		switch obs.Flags {
		case "70381", "70389", "90989":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// Includes "70000", "70101", "99999"
			controlinfo = VALUE_MISSING
		}
	} else {
		controlinfo = VALUE_PASSED_QC
		valPtr = addr(float32(val))
	}

	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}, nil
}

func convertPdata(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	var controlinfo string
	var valPtr *float32

	if val, err := strconv.ParseFloat(obs.Data, 32); err != nil {
		switch obs.Flags {
		case "20389", "30389", "40389", "50383", "70381", "71381":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// "00000", "10000", "10319", "30000", "30319",
			// "40000", "40929", "48929", "48999", "50000",
			// "50205", "60000", "70000", "70103", "70203",
			// "71000", "71203", "90909", "99999"
			controlinfo = VALUE_MISSING
		}
	} else {
		valPtr = addr(float32(val))

		switch obs.Flags {
		case "10319", "10329", "30319", "40319", "48929", "48999":
			controlinfo = VALUE_MANUALLY_INTERPOLATED
		case "20389", "30389", "40389", "50383", "70381", "71381", "99319":
			controlinfo = VALUE_CORRECTED_AUTOMATICALLY
		case "40929":
			controlinfo = INTERPOLATION_ADDED_MANUALLY
		default:
			// "71000", "71203", "90909", "99999"
			controlinfo = VALUE_PASSED_QC
		}
	}

	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}, nil
}

func convertNdata(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	var controlinfo string
	var valPtr *float32

	if val, err := strconv.ParseFloat(obs.Data, 32); err != nil {
		switch obs.Flags {
		case "70389":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// "30319", "38929", "40000", "40100", "40315"
			// "40319", "43325", "48325", "49225", "49915"
			// "70000", "70204", "71000", "73309", "78937"
			// "90909", "93399", "98999", "99999"
			controlinfo = VALUE_MISSING
		}
	} else {
		valPtr = addr(float32(val))

		switch obs.Flags {
		case "43325", "48325":
			controlinfo = VALUE_MANUALLY_ASSIGNED
		case "30319", "38929", "40315", "40319":
			controlinfo = VALUE_MANUALLY_INTERPOLATED
		case "49225", "49915":
			controlinfo = INTERPOLATION_ADDED_MANUALLY
		case "70389", "73309", "78937", "93399", "98999":
			controlinfo = VALUE_CORRECTED_AUTOMATICALLY
		default:
			// "40000", "40100", "70000", "70204", "71000", "90909", "99999"
			controlinfo = VALUE_PASSED_QC
		}
	}

	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}, nil
}

func convertVdata(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	var useinfo, controlinfo string
	var valPtr *float32

	// set useinfo based on time
	if h := obs.Obstime.Hour(); h == 0 || h == 6 || h == 12 || h == 18 {
		useinfo = COMPLETED_HQC
	} else {
		useinfo = INVALID_FLAGS
	}

	// set data and controlinfo
	if val, err := strconv.ParseFloat(obs.Data, 32); err != nil {
		controlinfo = VALUE_MISSING
	} else {
		// super special treatment clause of T_VDATA.OT_24, so it will be the same as in kvalobs
		// add custom offset, because OT_24 in KDVH has been treated differently than OT_24 in kvalobs
		if ts.Element == "OT_24" {
			offset, err := period.Parse("PT18H") // fromtime_offset -PT6H, timespan P1D
			if err != nil {
				return lard.DataObs{}, lard.TextObs{}, lard.Flag{}, errors.New("could not parse period")
			}
			temp, ok := offset.AddTo(obs.Obstime)
			if !ok {
				return lard.DataObs{}, lard.TextObs{}, lard.Flag{}, errors.New("could not add period")
			}

			obs.Obstime = temp
			// convert from hours to minutes
			val *= 60.0
		}

		valPtr = addr(float32(val))
		controlinfo = VALUE_PASSED_QC
	}

	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Useinfo:     &useinfo,
			Controlinfo: &controlinfo,
		}, nil
}

func convertDiurnalInterpolated(obs *KdvhObs, ts *TsInfo) (lard.DataObs, lard.TextObs, lard.Flag, error) {
	val, err := strconv.ParseFloat(obs.Data, 32)
	if err != nil {
		return lard.DataObs{}, lard.TextObs{}, lard.Flag{}, err
	}
	valPtr := addr(float32(val))
	return lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
			QcUsable: true,
		},
		lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
			QcUsable: true,
		},
		lard.Flag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Original:    valPtr,
			Corrected:   valPtr,
			Useinfo:     addr(DIURNAL_INTERPOLATED_USEINFO),
			Controlinfo: addr(VALUE_MANUALLY_INTERPOLATED),
		}, nil
}
