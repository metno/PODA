package kdvh

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"

	"migrate/utils"
)

// List of columns that we do not need to select when extracting the element codes from a KDVH table
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season", "xxx"}

type DumpConfig struct {
	BaseDir   string   `short:"p" long:"path" default:"./dumps/kdvh" description:"Location the dumped data will be stored in"`
	Tables    []string `short:"t" delimiter:"," long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations  []string `short:"s" delimiter:"," long:"stnr" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements  []string `short:"e" delimiter:"," long:"elem" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Overwrite bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Email     []string `long:"email" delimiter:"," description:"Optional comma separated list of email addresses used to notify if the program crashed"`
	MaxConn   int      `long:"conns" default:"10" description:"Max number of concurrent connections allowed"`
}

func (config *DumpConfig) Execute([]string) error {
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
	if err := os.MkdirAll(table.Path, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	utils.SetLogFile(table.TableName, "dump")

	elements, err := table.getElements(conn, config)
	if err != nil {
		return
	}

	stations, err := table.getStations(conn, config)
	if err != nil {
		return
	}

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)

	bar := utils.NewBar(len(stations), table.TableName)
	bar.RenderBlank()
	for _, station := range stations {
		path := filepath.Join(table.Path, string(station))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		var wg sync.WaitGroup
		for _, element := range elements {
			// This blocks if the channel is full
			semaphore <- struct{}{}

			wg.Add(1)
			go func() {
				defer wg.Done()

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

				// Release semaphore
				<-semaphore
			}()
		}
		wg.Wait()
		bar.Add(1)
	}
}

// Fetches elements and filters them based on user input
func (table *Table) getElements(conn *sql.DB, config *DumpConfig) ([]string, error) {
	elements, err := table.fetchElements(conn)
	if err != nil {
		return nil, err
	}

	filename := filepath.Join(table.Path, "elements.txt")
	if err := utils.SaveToFile(elements, filename); err != nil {
		slog.Warn("Could not save element list to " + filename)
	}

	elements = utils.FilterSlice(config.Elements, elements, "")
	return elements, nil
}

// Fetch column names for a given table
// We skip the columns defined in INVALID_COLUMNS and all columns that contain the 'kopi' string
// TODO: should we dump these invalid/kopi elements even if we are not importing them?
func (table *Table) fetchElements(conn *sql.DB) (elements []string, err error) {
	slog.Info(fmt.Sprintf("Fetching elements for %s...", table.TableName))

	// NOTE: T_HOMOGEN_MONTH is a special case, refer to `dumpHomogenMonth` in
	// `dump_functions.go` for more information
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
func (table *Table) getStations(conn *sql.DB, config *DumpConfig) ([]string, error) {
	stations, err := table.fetchStnrFromElemTable(conn)
	if err != nil {
		return nil, err
	}

	stations = utils.FilterSlice(config.Stations, stations, "")
	return stations, nil
}

// This function uses the ELEM table to fetch the station numbers
func (table *Table) fetchStnrFromElemTable(conn *sql.DB) (stations []string, err error) {
	slog.Info(fmt.Sprint("Fetching station numbers..."))

	var rows *sql.Rows
	if table.ElemTableName == "T_ELEM_OBS" {
		query := `SELECT DISTINCT stnr FROM t_elem_obs WHERE table_name = $1`
		rows, err = conn.Query(query, table.TableName)
	} else {
		query := fmt.Sprintf("SELECT DISTINCT stnr FROM %s", strings.ToLower(table.ElemTableName))
		rows, err = conn.Query(query)
	}

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
