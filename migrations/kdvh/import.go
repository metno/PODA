package kdvh

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	"migrate/lard"
	"migrate/utils"
)

type ImportConfig struct {
	Verbose     bool     `short:"v" description:"Increase verbosity level"`
	BaseDir     string   `short:"p" long:"path" default:"./dumps/kdvh" description:"Location the dumped data will be stored in"`
	TablesCmd   string   `short:"t" long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `short:"s" long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `short:"e" long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Sep         string   `long:"sep" default:","  description:"Separator character in the dumped files. Needs to be quoted"`
	HasHeader   bool     `long:"header" description:"Add this flag if the dumped files have a header row"`
	Skip        string   `long:"skip" choice:"data" choice:"flags" description:"Skip import of data or flags"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`

	Tables   []string
	Stations []string
	Elements []string

	OffsetMap map[StinfoKey]period.Period // Map of offsets used to correct (?) KDVH times for specific parameters
	StinfoMap map[StinfoKey]StinfoParam   // Map of metadata used to query timeseries ID in LARD
	KDVHMap   map[KDVHKey]Timespan        // Map of `from_time` and `to_time` for each (table, station, element) triplet. Not present for all parameters
}

func (config *ImportConfig) setup() {
	if len(config.Sep) > 1 {
		fmt.Printf("Error: '--sep' only accepts single-byte characters. Got %s", config.Sep)
		os.Exit(1)
	}
	if config.TablesCmd != "" {
		config.Tables = strings.Split(config.TablesCmd, ",")
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		config.Elements = strings.Split(config.ElementsCmd, ",")
	}
	config.CacheMetadata()
}

func (config *ImportConfig) Execute([]string) error {
	config.setup()

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return err
	}
	defer pool.Close()

	for _, table := range KDVH {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		table.Import(pool, config)
	}

	return nil
}

func (table *Table) Import(pool *pgxpool.Pool, config *ImportConfig) (rowsInserted int64) {
	defer utils.SendEmailOnPanic("importTable", config.Email)

	if table.importUntil == 0 {
		if config.Verbose {
			slog.Info("Skipping import of" + table.TableName + "  because this table is not set for import")
		}
		return 0
	}

	utils.SetLogFile(table.TableName, "import")

	table.Path = filepath.Join(config.BaseDir, table.Path)
	stations, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not read directory %s: %s", table.Path, err))
		return 0
	}

	bar := utils.NewBar(len(stations), table.TableName)
	bar.RenderBlank()
	for _, station := range stations {
		count, err := table.importStation(station, pool, config)
		if err == nil {
			rowsInserted += count
		}
		bar.Add(1)
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", table.TableName, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)
	return rowsInserted
}

// Loops over the element files present in the station directory and processes them concurrently
func (table *Table) importStation(station os.DirEntry, pool *pgxpool.Pool, config *ImportConfig) (totRows int64, err error) {
	stnr, err := getStationNumber(station, config.Stations)
	if err != nil {
		if config.Verbose {
			slog.Info(err.Error())
		}
		return 0, err
	}

	dir := filepath.Join(table.Path, station.Name())
	elements, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not read directory %s: %s", dir, err))
		return 0, err
	}

	var wg sync.WaitGroup
	for _, element := range elements {
		elemCode, err := getElementCode(element, config.Elements)
		if err != nil {
			if config.Verbose {
				slog.Info(err.Error())
			}
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			tsInfo, err := config.NewTimeseriesInfo(table.TableName, elemCode, stnr)
			if err != nil {
				return
			}

			tsid, err := getTimeseriesID(tsInfo, pool)
			if err != nil {
				slog.Error(tsInfo.logstr + "could not obtain timeseries - " + err.Error())
				return
			}

			filename := filepath.Join(dir, element.Name())
			data, err := table.parseElementFile(filename, tsInfo, config)
			if err != nil {
				return
			}

			ts := NewTimeseries(tsid, data)
			count, err := importData(ts, tsInfo, pool, config)
			if err != nil {
				return
			}
			totRows += count
		}()
	}
	wg.Wait()

	return totRows, nil
}

func (table *Table) parseElementFile(filename string, tsInfo *TimeseriesInfo, config *ImportConfig) ([]LardObs, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not open file '%s': %s", filename, err))
		return nil, err
	}
	defer file.Close()

	data, err := table.parseData(file, tsInfo, config)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not parse data from '%s': %s", filename, err))
		return nil, err
	}

	if len(data) == 0 {
		slog.Info(tsInfo.logstr + "no rows to insert (all obstimes > max import time)")
		return nil, err
	}

	return data, nil
}

func importData(ts *LardTimeseries, tsInfo *TimeseriesInfo, pool *pgxpool.Pool, config *ImportConfig) (count int64, err error) {
	if !(config.Skip == "data") {
		if tsInfo.param.IsScalar {
			count, err = lard.InsertData(ts, pool, tsInfo.logstr)
			if err != nil {
				slog.Error(tsInfo.logstr + "failed data bulk insertion - " + err.Error())
				return 0, err
			}
		} else {
			count, err = lard.InsertTextData(ts, pool, tsInfo.logstr)
			if err != nil {
				slog.Error(tsInfo.logstr + "failed non-scalar data bulk insertion - " + err.Error())
				return 0, err
			}
			// TODO: should we skip inserting flags here? In kvalobs there are no flags for text data
			// return count, nil
		}
	}

	if !(config.Skip == "flags") {
		if err := lard.InsertFlags(ts, FLAGS_TABLE, FLAGS_COLS, pool, tsInfo.logstr); err != nil {
			slog.Error(tsInfo.logstr + "failed flag bulk insertion - " + err.Error())
		}
	}

	return count, nil
}

func getStationNumber(station os.DirEntry, stationList []string) (int32, error) {
	if !station.IsDir() {
		return 0, errors.New(fmt.Sprintf("%s is not a directory, skipping", station.Name()))
	}

	if stationList != nil && !slices.Contains(stationList, station.Name()) {
		return 0, errors.New(fmt.Sprintf("Station %v not in the list, skipping", station.Name()))
	}

	stnr, err := strconv.ParseInt(station.Name(), 10, 32)
	if err != nil {
		return 0, errors.New("Error parsing station number:" + err.Error())
	}

	return int32(stnr), nil
}

func getElementCode(element os.DirEntry, elementList []string) (string, error) {
	elemCode := strings.ToUpper(strings.TrimSuffix(element.Name(), ".csv"))

	if elementList != nil && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not in the list, skipping", elemCode))
	}

	if elemcodeIsInvalid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not set for import, skipping", elemCode))
	}
	return elemCode, nil
}

func getTimeseriesID(tsInfo *TimeseriesInfo, pool *pgxpool.Pool) (int32, error) {
	label := lard.Label{
		StationID: tsInfo.station,
		TypeID:    tsInfo.param.TypeID,
		ParamID:   tsInfo.param.ParamID,
		Sensor:    &tsInfo.param.Sensor,
		Level:     tsInfo.param.Hlevel,
	}
	tsid, err := lard.GetTimeseriesID(label, tsInfo.param.Fromtime, pool)
	if err != nil {
		slog.Error(tsInfo.logstr + "could not obtain timeseries - " + err.Error())
		return 0, err

	}
	return tsid, nil
}

func (table *Table) parseData(handle *os.File, meta *TimeseriesInfo, config *ImportConfig) ([]LardObs, error) {
	scanner := bufio.NewScanner(handle)

	var rowCount int
	// Try to infer row count from header
	if config.HasHeader {
		scanner.Scan()
		// rowCount, _ = strconv.Atoi(scanner.Text())
		if temp, err := strconv.Atoi(scanner.Text()); err == nil {
			rowCount = temp
		}
	}

	data := make([]LardObs, 0, rowCount)
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			return nil, err
		}

		// Only import data between KDVH's defined fromtime and totime
		if meta.span.FromTime != nil && obsTime.Sub(*meta.span.FromTime) < 0 {
			continue
		} else if meta.span.ToTime != nil && obsTime.Sub(*meta.span.ToTime) > 0 {
			break
		}

		if obsTime.Year() >= table.importUntil {
			break
		}

		temp, err := table.convFunc(KdvhObs{meta, obsTime, cols[1], cols[2]})
		if err != nil {
			return nil, err
		}

		data = append(data, temp)
	}

	return data, nil
}

// TODO: add CALL_SIGN? It's not in stinfosys?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}
