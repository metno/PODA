package db

// In KDVH for each table name we usually have three separate tables:
// 1. A DATA table containing observation values;
// 2. A FLAG table containing quality control (QC) flags;
// 3. A ELEM table containing metadata about the validity of the timeseries.
//
// DATA and FLAG tables have the same schema:
// | dato | stnr | ... |
// where 'dato' is the timestamp of the observation, 'stnr' is the station
// where the observation was measured, and '...' is a varying number of columns
// each with different observations, where the column name is the 'elem_code'
// (e.g. for air temperature, 'ta').
//
// The ELEM tables have the following schema:
// | stnr | elem_code | fdato | tdato | table_name | flag_table_name | audit_dato

// This struct contains basic metadata for a KDVH table
type Table struct {
	TableName     string // Name of the DATA table
	FlagTableName string // Name of the FLAG table
	ElemTableName string // Name of the ELEM table
	Path          string // Directory name of where the dumped table is stored
	importUntil   int    // Import data only until the year specified by this field. Table import will be skipped, if `SetImportYear` is not called.
}

// Creates default Table
func NewTable(data, flag, elem string) *Table {
	return &Table{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		// NOTE: '_combined' kept for backward compatibility with original scripts
		Path: data + "_combined",
	}
}

// Specify the year until data should be imported
func (t *Table) SetImportYear(year int) *Table {
	if year > 0 {
		t.importUntil = year
	}
	return t
}

// Checks if the table is set for import
func (t *Table) ShouldImport() bool {
	return t.importUntil > 0
}

// Checks if the table max import year was reached
func (t *Table) MaxImportYearReached(year int) bool {
	return t.importUntil < 0 || year >= t.importUntil
}
