package kdvh

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"migrate/utils"
)

type DumpConfig struct {
	BaseDir     string   `short:"p" long:"path" default:"./dumps/kdvh" description:"Location the dumped data will be stored in"`
	TablesCmd   string   `short:"t" long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `short:"s" long:"stnr" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `short:"e" long:"elem" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Overwrite   bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`

	Tables   []string
	Stations []string
	Elements []string
}

func (config *DumpConfig) setup() {
	if config.TablesCmd != "" {
		config.Tables = strings.Split(config.TablesCmd, ",")
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		config.Elements = strings.Split(config.ElementsCmd, ",")
	}
}

func (config *DumpConfig) Execute([]string) error {
	config.setup()

	conn, err := sql.Open("pgx", os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error(err.Error())
		return nil
	}

	for _, table := range KDVH {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		table.Dump(conn, config)
	}

	return nil
}

func (table *Table) Dump(conn *sql.DB, config *DumpConfig) {
	defer utils.SendEmailOnPanic(fmt.Sprintf("%s dump", table.TableName), config.Email)

	table.Path = filepath.Join(config.BaseDir, table.Path)

	utils.SetLogFile(table.TableName, "dump")

	elements, err := table.getElements(conn, config)
	if err != nil {
		return
	}

	bar := utils.NewBar(len(elements), table.TableName)

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	bar.RenderBlank()
	for _, element := range elements {
		table.dumpElement(element, conn, config)
		bar.Add(1)
	}
}

// TODO: maybe we don't do this? Or can we use pgdump/copy?
// The problem is that there are no indices on the tables, that's why the queries are super slow
// Dumping the whole table might be a lot faster (for T_MDATA it's ~10 times faster!),
// but it might be more difficult to recover if something goes wrong?
// =>
// copyQuery := fmt.SPrintf("\\copy (select * from t_mdata) TO '%s/%s.csv' WITH CSV HEADER", config.BaseDir, table.TableName)
// cmd := exec.Command("psql", CONN_STRING, "-c", copyQuery)
// cmd.Stderr = &bytes.Buffer{}
// err = cmd.Run()
func (table *Table) dumpElement(element string, conn *sql.DB, config *DumpConfig) {
	stations, err := table.getStationsWithElement(element, conn, config)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch stations for table %s: %v", table.TableName, err))
		return
	}

	for _, station := range stations {
		path := filepath.Join(table.Path, string(station))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		err := table.dumpFunc(
			path,
			DumpMeta{
				element:   element,
				station:   station,
				dataTable: table.TableName,
				flagTable: table.FlagTableName,
				overwrite: config.Overwrite,
			},
			conn,
		)

		// NOTE: Non-nil errors are logged inside each DumpFunc
		if err == nil {
			slog.Info(fmt.Sprintf("%s - %s - %s: dumped successfully", table.TableName, station, element))
		}
	}
}

// Fetches elements and filters them based on user input
func (table *Table) getElements(conn *sql.DB, config *DumpConfig) ([]string, error) {
	elements, err := table.fetchElements(conn)
	if err != nil {
		return nil, err
	}

	elements = utils.FilterSlice(config.Elements, elements, "")
	return elements, nil
}

// List of columns that we do not need to select when extracting the element codes from a KDVH table
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season", "xxx"}

// Fetch column names for a given table
// We skip the columns defined in INVALID_COLUMNS and all columns that contain the 'kopi' string
// TODO: should we dump these invalid/kopi elements even if we are not importing them?
func (table *Table) fetchElements(conn *sql.DB) (elements []string, err error) {
	slog.Info(fmt.Sprintf("Fetching elements for %s...", table.TableName))

	// TODO: not sure why we only dump these two for this table
	if table.TableName == "T_HOMOGEN_MONTH" {
		return []string{"rr", "tam"}, nil
	}

	rows, err := conn.Query(
		`SELECT column_name FROM information_schema.columns
            WHERE table_name = $1
            AND NOT column_name = ANY($2::text[])
            AND column_name NOT LIKE '%kopi%'`,
		// NOTE: needs to be lowercase with PG
		strings.ToLower(table.TableName),
		INVALID_COLUMNS,
	)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
			return nil, err
		}
		elements = append(elements, name)
	}
	return elements, rows.Err()
}

// Fetches station numbers and filters them based on user input
func (table *Table) getStationsWithElement(element string, conn *sql.DB, config *DumpConfig) ([]string, error) {
	stations, err := table.fetchStationsWithElement(element, conn)
	if err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Element '%s'", element) + "not available for station '%s'"
	stations = utils.FilterSlice(config.Stations, stations, msg)
	return stations, nil
}

// Fetches the unique station numbers in the table for a given element (and when that element is not null)
// NOTE: splitting by element does make it a bit better, because we avoid quering for stations that have no data or flag for that element?
func (table *Table) fetchStationsWithElement(element string, conn *sql.DB) (stations []string, err error) {
	slog.Info(fmt.Sprintf("Fetching station numbers for %s (this can take a while)...", element))

	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s WHERE %s IS NOT NULL`,
		table.TableName,
		element,
	)

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}

// Fetches all unique station numbers in the table
// FIXME: the DISTINCT query can be extremely slow
// NOTE: decided to use fetchStationsWithElement instead
func (table *Table) fetchStationNumbers(conn *sql.DB) (stations []string, err error) {
	slog.Info(fmt.Sprint("Fetching station numbers (this can take a while)..."))

	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s`,
		table.TableName,
	)

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}
