package kdvh

import (
	"errors"
	"migrate/lard"
	"strconv"

	"github.com/rickb777/period"
)

// In kvalobs a flag is a 16 char string containg QC information about the observation:
// Note: Missing numbers in the following lists are marked as reserved (not in use I guess?)
//
// CONTROLINFO FLAG:
//
//  0 - CONTROL LEVEL (not used)
//  1 - RANGE CHECK
//   0. Not checked
//   1. Check passed
//   2. Higher than HIGH
//   3. Lower than LOW
//   4. Higher than HIGHER
//   5. Lower that LOWER
//   6. Check failed, above HIGHEST or below LOWEST
//
//  2 - FORMAL CONSISTENCY CHECK
//    0. Not checked
//    1. Check passe
//    2. Inconsistency found, but not an error with the relevant parameter, no correction
//    3. Inconsistency found at the observation time, but not possible to determine which parameter, no correction
//    4. Inconsistency found at earliar/later observation times, but not possible to determine which parameter, no correction
//    6. Inconsistency found at the observation time, probably error with the relevant parameter, no correction
//    7. Inconsistency found at earliar/later observation times, probably error with relevant parameter, no correction
//    8. Inconsistency found, a parameter is missing, no correction
//    A. Inconsistency found at the observation time, corrected automatically
//    B. Inconsistency found at earliar/later observation times, corrected automatically
//    D. Check failed
//
//  3 - JUMP CHECK (STEP, DIP, FREEZE, DRIFT)
//    0. Not checked
//    1. Check passed
//    2. Change higher than test value, no correction
//    3. No change in measured value (freeze check did not pass?), no correction
//    4. Suspected error in freeze check, no error in dip check (??), no correction
//    5. Suspected error in dip check, no error in freeze check (??), no correction
//    7. Observed drift, no correction
//    9. Change higher than test value, corrected automatically
//    A. Freeze check did not pass, corrected automatically
//
//  4 - PROGNOSTIC CHECK
//    0. Not checked
//    1. Check passed
//    2. Deviation from model higher than HIGH
//    3. Deviation from model lower than LOW
//    4. Deviation from model higher than HIGHER
//    5. Deviation from model lower that LOWER
//    6. Check failed, deviation from model above HIGHEST or below LOWEST
//
//  5 - VALUE CHECK (FOR MOVING STATIONS)
//    0. Not checked
//    1. Check passed
//    3. Suspicious value, no correction
//    4. Suspicious value, corrected automatically
//    6. Check failed
//
//  6 - MISSING OBSERVATIONS
//    0. Original and corrected values exist
//    1. Original value missing, but corrected value exists
//    2. Corrected value missing, orginal value discarded
//    3. Original and corrected values missing
//
//  7 - TIMESERIES FITTING
//    0. Not checked
//    1. Interpolated with good fitness
//    2. Interpolated with unsure fitness
//    3. Intepolation not suitable
//
//  8 - WEATHER ANALYSIS
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//    3. Suspicious value, corrected automatically
//
//  9 - STATISTICAL CHECK
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//
// 10 - CLIMATOLOGICAL CONSISTENCY CHECK
//    0. Not checked
//    1. Check passed
//    2. Climatologically questionable, but not an error with the relevant parameter, no correction
//    3. Climatologically questionable at the observation time, but not possible to determine which parameter, no correction
//    4. Climatologically questionable at earliar/later observation times, but not possible to determine which parameter, no correction
//    6. Climatologically questionable at the observation time, probably error with the relevant parameter, no correction
//    7. Climatologically questionable at earliar/later observation times, probably error with relevant parameter, no correction
//    A. Inconsistency found at the observation time, corrected automatically
//    B. Inconsistency found at earliar/later observation times, corrected automatically
//    D. Check failed
//
// 11 - CLIMATOLOGICAL CHECK
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//    3. Suspicious value, corrected automatically
//
// 12 - DISTRIBUTION CHECK OF ACCUMULATED PARAMETERS (ESPECIALLY FOR PRECIPITATION)
//    0. Not checked
//    1. Not an accumulated value
//    2. Observation outside accumulated parameter range
//    3. Abnormal observation (??)
//    6. Accumulation calculated from numerical model
//    7. Accumulation calculated from weather analysis
//    A. Accumulation calculated with 'steady rainfall' method
//    B. Accumulation calculated with 'uneven rainfall' method
//
// 13 - PREQUALIFICATION (CERTAIN PAIRS OF 'STATIONID' AND 'PARAMID' CAN BE DISCARDED)
//    0. Not checked
//    5. Value is missing
//    6. Check failed, invalid original value
//    7. Check failed, original value is noisy
//
// 14 - COMBINATION CHECK
//    0. Not checked
//    1. Check passed
//    2. Outside test limit value, but no jumps detected and inside numerical model tolerance
//    9. Check failed. Outside test limit value, no jumps detected but outside numerical model tolerance
//    A. Check failed. Outside test limit value, jumps detected but inside numerical model tolerance
//    B. Check failed. Outside test limit value, jumps detected and outside numerical model tolerance
//
// 15 - MANUAL QUALITY CONTROL
//    0. Not checked
//    1. Check passed
//    2. Probably OK
//    5. Value manually interpolated
//    6. Value manually assigned
//    7. Value manually corrected
//    A. Manually rejected

const (
	VALUE_PASSED_QC = "00000" + "00000000000"

	// Corrected value is present, and original was remove by QC
	VALUE_CORRECTED_AUTOMATICALLY = "00000" + "01000000000"
	VALUE_MANUALLY_INTERPOLATED   = "00000" + "01000000005"
	VALUE_MANUALLY_ASSIGNED       = "00000" + "01000000006"

	VALUE_REMOVED_BY_QC = "00000" + "02000000000" // Corrected value is missing, and original was remove by QC
	VALUE_MISSING       = "00000" + "03000000000" // Both original and corrected are missing
	VALUE_PASSED_HQC    = "00000" + "00000000001" // Value was sent to HQC for inspection, but it was OK

	// original value still exists, not exactly sure what the difference with VALUE_MANUALLY_INTERPOLATED is
	INTERPOLATION_ADDED_MANUALLY = "00000" + "00000000005"
)

// USEINFO FLAG:
//
//  0 - CONTROL LEVELS PASSED
//    1. Completed QC1, QC2 and HQC
//    2. Completed QC2 and HQC
//    3. Completed QC1 and HQC
//    4. Completed HQC
//    5. Completed QC1 and QC2
//    6. Completed QC2
//    7. Completed QC1
//    9. Missing information
//
//  1 - DEVIATION FROM NORM (MEAN?)
//    0. Observation time and period are okay
//    1. Observation time deviates from norm
//    2. Observation period is shorter than norm
//    3. Observation perios is longer than norm
//    4. Observation time deviates from norm, and period is shorter than norm
//    5. Observation time deviates from norm, and period is longer than norm
//    8. Missing value
//    9. Missing status information
//
//  2 - QUALITY LEVEL OF ORIGNAL VALUE
//    0. Value is okay
//    1. Value is suspicious (probably correct)
//    2. Value is suspicious (probably wrong)
//    3. Value is wrong
//    9. Missing quality information
//
//  3 - TREATMENT OF ORIGINAL VALUE
//    0. Unchanged
//    1. Manually corrected
//    2. Manually interpolated
//    3. Automatically corrected
//    4. Automatically interpolated
//    5. Manually derived from accumulated value
//    6. Automatically derived from accumulated value
//    8. Rejected
//    9. Missing information
//
//  4 - MOST IMPORT CHECK RESULT (?)
//    0. Original value is okay
//    1. Range check
//    2. Consistency check
//    3. Jump check
//    4. Consistency check in relation with earlier/later observations
//    5. Prognostic check based on observation data
//    6. Prognostic check based on Timeseries
//    7. Prognostic check based on model data
//    8. Prognostic check based on statistics
//    9. Missing information
//
//  7 - DELAY INFORMATION
//    0. Observation carried out and reported at the right time
//    1. Observation carried out early and reported at the right time
//    2. Observation carried out late and reported at the right time
//    3. Observation reported early
//    4. Observation reported late
//    5. Observation carried out early and reported late
//    6. Observation carried out late and reported late
//    9. Missing information
//
//  8 - FIRST DIGIT OF HEXADECIMAL VALUE OF THE OBSERVATION CONFIDENCE LEVEL
//  9 - SECOND DIGIT OF HEXADECIMAL VALUE OF THE OBSERVATION CONFIDENCE LEVEL
// 13 - FIRST HQC OPERATOR DIGIT
// 14 - SECOND HQC OPERATOR DIGIT
// 15 - HEXADECIMAL DIGIT WITH NUMBER OF TESTS THAT DID NOT PASS (RETURNED A RESULT?)

const (
	// Remaing 11 digits of `useinfo` that follow the 5 digits contained in `obs.Flags`.
	// TODO: From the docs it looks like the '9' should be changed by kvalobs when
	// the observation is inserted into the database but that's not the case?
	DELAY_DEFAULT = "00900000000"

	INVALID_FLAGS                = "99999" + DELAY_DEFAULT // Only returned when the flags are invalid
	COMPLETED_HQC                = "40000" + DELAY_DEFAULT // Specific to T_VDATA
	DIURNAL_INTERPOLATED_USEINFO = "48925" + DELAY_DEFAULT // Specific to T_DIURNAL_INTERPOLATED
)

func (obs *Obs) flagsAreValid() bool {
	if len(obs.Flags) != 5 {
		return false
	}
	_, err := strconv.ParseInt(obs.Flags, 10, 32)
	return err == nil
}

func (obs *Obs) Useinfo() string {
	if !obs.flagsAreValid() {
		return INVALID_FLAGS
	}
	return obs.Flags + DELAY_DEFAULT
}

// The following functions try to recover the original pair of `controlinfo`
// and `useinfo` generated by Kvalobs for the observation, based on `Obs.Flags` and `Obs.Data`
// Different KDVH tables need different ways to perform this conversion.

func makeDataPage(obs Obs) (lard.Obs, error) {
	var valPtr *float32

	controlinfo := VALUE_PASSED_QC
	if obs.Data == "" {
		controlinfo = VALUE_MISSING
	}

	// NOTE: this is the only function that can return `lard.Obs`
	// with non-null text data
	if !obs.param.IsScalar {
		return lard.Obs{
			Obstime:     obs.Obstime,
			Data:        valPtr,
			Text:        &obs.Data,
			Useinfo:     obs.Useinfo(),
			Controlinfo: controlinfo,
		}, nil
	}

	val, err := strconv.ParseFloat(obs.Data, 32)
	if err == nil {
		f32 := float32(val)
		valPtr = &f32
	}

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        valPtr,
		Useinfo:     obs.Useinfo(),
		Controlinfo: controlinfo,
	}, nil
}

// modify obstimes to always use totime
func makeDataPageProduct(obs Obs) (lard.Obs, error) {
	obsLard, err := makeDataPage(obs)
	if !obs.offset.IsZero() {
		if temp, ok := obs.offset.AddTo(obsLard.Obstime); ok {
			obsLard.Obstime = temp
		}
	}
	return obsLard, err
}

func makeDataPageEdata(obs Obs) (lard.Obs, error) {
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
		f32 := float32(val)
		valPtr = &f32
	}

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        valPtr,
		Useinfo:     obs.Useinfo(),
		Controlinfo: controlinfo,
	}, nil
}

func makeDataPagePdata(obs Obs) (lard.Obs, error) {
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
		f32 := float32(val)
		valPtr = &f32

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

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        valPtr,
		Useinfo:     obs.Useinfo(),
		Controlinfo: controlinfo,
	}, nil
}

func makeDataPageNdata(obs Obs) (lard.Obs, error) {
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
		f32 := float32(val)
		valPtr = &f32
	}

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        valPtr,
		Useinfo:     obs.Useinfo(),
		Controlinfo: controlinfo,
	}, nil
}

func makeDataPageVdata(obs Obs) (lard.Obs, error) {
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
		f32 := float32(val)

		if obs.element == "OT_24" {
			// add custom offset, because OT_24 in KDVH has been treated differently than OT_24 in kvalobs
			offset, err := period.Parse("PT18H") // fromtime_offset -PT6H, timespan P1D
			if err != nil {
				return lard.Obs{}, errors.New("could not parse period")
			}
			temp, ok := offset.AddTo(obs.Obstime)
			if !ok {
				return lard.Obs{}, errors.New("could not add period")
			}

			obs.Obstime = temp
			// convert from hours to minutes
			f32 *= 60.0
		}
		valPtr = &f32
		controlinfo = VALUE_PASSED_QC
	}

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        valPtr,
		Useinfo:     useinfo,
		Controlinfo: controlinfo,
	}, nil
}

func makeDataPageDiurnalInterpolated(obs Obs) (lard.Obs, error) {
	val, err := strconv.ParseFloat(obs.Data, 32)
	if err != nil {
		return lard.Obs{}, err
	}
	f32 := float32(val)

	return lard.Obs{
		Obstime:     obs.Obstime,
		Data:        &f32,
		Useinfo:     DIURNAL_INTERPOLATED_USEINFO,
		Controlinfo: VALUE_MANUALLY_INTERPOLATED,
	}, nil
}
