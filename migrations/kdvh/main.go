package kdvh

// Command line arguments for KDVH migrations
type Cmd struct {
	Dump   DumpConfig   `command:"dump" description:"Dump tables from KDVH to CSV"`
	Import ImportConfig `command:"import" description:"Import CSV file dumped from KDVH"`
	List   ListConfig   `command:"list" description:"List available KDVH tables"`
}

// The KDVH database simply contains a map of "table name" to `Table`
var KDVH map[string]*Table = map[string]*Table{
	// Section 1: tables that need to be migrated entirely
	// TODO: figure out if we need to use the elem_code_paramid_level_sensor_t_edata table?
	"T_EDATA":     NewTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA").SetConvFunc(makeDataPageEdata).SetImport(3000),
	"T_METARDATA": NewTable("T_METARDATA", "", "T_ELEM_METARDATA").SetDumpFunc(dumpDataOnly).SetImport(3000),

	// Section 2: tables with some data in kvalobs, import only up to 2005-12-31
	"T_ADATA":      NewTable("T_ADATA", "T_AFLAG", "T_ELEM_OBS").SetImport(2006),
	"T_MDATA":      NewTable("T_MDATA", "T_MFLAG", "T_ELEM_OBS").SetImport(2006),
	"T_TJ_DATA":    NewTable("T_TJ_DATA", "T_TJ_FLAG", "T_ELEM_OBS").SetImport(2006),
	"T_PDATA":      NewTable("T_PDATA", "T_PFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPagePdata).SetImport(2006),
	"T_NDATA":      NewTable("T_NDATA", "T_NFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPageNdata).SetImport(2006),
	"T_VDATA":      NewTable("T_VDATA", "T_VFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPageVdata).SetImport(2006),
	"T_UTLANDDATA": NewTable("T_UTLANDDATA", "T_UTLANDFLAG", "T_ELEM_OBS").SetImport(2006),

	// Section 3: tables that should only be dumped
	"T_10MINUTE_DATA": NewTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear),
	"T_ADATA_LEVEL":   NewTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL", "T_ELEM_OBS"),
	"T_MINUTE_DATA":   NewTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear),
	"T_SECOND_DATA":   NewTable("T_SECOND_DATA", "T_SECOND_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear),
	"T_CDCV_DATA":     NewTable("T_CDCV_DATA", "T_CDCV_FLAG", "T_ELEM_EDATA"),
	"T_MERMAID":       NewTable("T_MERMAID", "T_MERMAID_FLAG", "T_ELEM_EDATA"),
	"T_SVVDATA":       NewTable("T_SVVDATA", "T_SVVFLAG", "T_ELEM_OBS"),

	// Section 4: special cases, namely digitized historical data
	"T_MONTH":           NewTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH").SetConvFunc(makeDataPageProduct).SetImport(1957),
	"T_DIURNAL":         NewTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL").SetConvFunc(makeDataPageProduct).SetImport(2006),
	"T_HOMOGEN_DIURNAL": NewTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpDataOnly).SetConvFunc(makeDataPageProduct),
	"T_HOMOGEN_MONTH":   NewTable("T_HOMOGEN_MONTH", "T_ELEM_HOMOGEN_MONTH", "").SetDumpFunc(dumpHomogenMonth).SetConvFunc(makeDataPageProduct),

	// Tables missing in the KDVH proxy:
	//   1. these exist in a separate database
	//        - "T_AVINOR"
	//        - "T_PROJDATA"
	//   2. these are not in active use and don't need to be imported in LARD
	//        - "T_DIURNAL_INTERPOLATED"
	//        - "T_MONTH_INTERPOLATED"
}
