package check

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"migrate/kvalobs/db"
	"migrate/stinfosys"
	"migrate/utils"
)

type Config struct {
	Path      string `arg:"-p" default:"./dumps" help:"Directory of the dumped data"`
	CheckName string `arg:"positional" required:"true" help:"Choices: ['overlap', 'non-scalars']"`
}

func (c *Config) Execute() {
	dbs := db.InitDBs()
	if utils.IsEmptyOrEqual(c.CheckName, "overlap") {
		fmt.Println("Checking if some param IDs are stored in both the `data` and `text_data` tables")
		for _, db := range dbs {
			c.checkDataAndTextParamsOverlap(&db)
		}
	}
	if utils.IsEmptyOrEqual(c.CheckName, "non-scalars") {
		fmt.Println("Checking if param IDs in `text_data` match non-scalar parameters in Stinfosys")
		stinfoParams := getStinfoNonScalars()
		for _, db := range dbs {
			c.checkNonScalars(&db, stinfoParams)
		}
	}
}

// Simply checks if some params are found both in the data and text_data
func (c *Config) checkDataAndTextParamsOverlap(database *db.DB) {
	defer fmt.Println(strings.Repeat("- ", 40))
	datapath := filepath.Join(c.Path, database.Name, db.DATA_TABLE_NAME+"_labels.csv")
	textpath := filepath.Join(c.Path, database.Name, db.TEXT_TABLE_NAME+"_labels.csv")

	dataParamids, derr := loadParamids(datapath)
	textParamids, terr := loadParamids(textpath)
	if derr != nil || terr != nil {
		return
	}

	ids := make([]int32, 0, len(textParamids))
	for id := range dataParamids {
		if _, ok := textParamids[id]; ok {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	for _, id := range ids {
		fmt.Printf("ParamID %5d exists in both data and text tables\n", id)
	}
}

func loadParamids(path string) (map[int32]int32, error) {
	labels, err := db.ReadLabelCSV(path)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	paramids := uniqueParamids(labels)
	return paramids, nil

}

// Creates hashset of paramids
func uniqueParamids(labels []*db.Label) map[int32]int32 {
	paramids := make(map[int32]int32)
	for _, label := range labels {
		paramids[label.ParamID] += 1
	}
	return paramids
}

type StinfoPair struct {
	ParamID  int32 `db:"paramid"`
	IsScalar bool  `db:"scalar"`
}

func getStinfoNonScalars() []int32 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(stinfosys.STINFO_ENV_VAR))
	if err != nil {
		log.Fatal("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(context.TODO(), "SELECT paramid FROM param WHERE scalar = false ORDER BY paramid")
	if err != nil {
		log.Fatal(err)
	}
	nonscalars, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		log.Fatal(err)
	}
	return nonscalars
}

// Checks that text params in Kvalobs are considered non-scalar in Stinfosys
func (c *Config) checkNonScalars(database *db.DB, nonscalars []int32) {
	defer fmt.Println(strings.Repeat("- ", 40))
	datapath := filepath.Join(c.Path, database.Name, db.DATA_TABLE_NAME+"_labels.csv")
	textpath := filepath.Join(c.Path, database.Name, db.TEXT_TABLE_NAME+"_labels.csv")

	dataParamids, derr := loadParamids(datapath)
	textParamids, terr := loadParamids(textpath)
	if derr != nil || terr != nil {
		return
	}

	for _, id := range nonscalars {
		if _, ok := textParamids[id]; ok {
			fmt.Printf("MATCH: ParamID %5d is text in both Stinfosys and Kvalobs\n", id)
			delete(textParamids, id)
		} else if _, ok := dataParamids[id]; ok {
			fmt.Printf(" FAIL: ParamID %5d is text in Stinfosys, but not in Kvalobs\n", id)
		} else {
			fmt.Printf(" WARN: ParamID %5d not found in Kvalobs\n", id)
		}
	}

	idsLeft := make([]int32, 0, len(textParamids))
	for id := range textParamids {
		idsLeft = append(idsLeft, id)
	}

	slices.Sort(idsLeft)
	for _, id := range idsLeft {
		fmt.Printf(" FAIL: ParamID %5d is text in Kvalobs, but not in Stinfosys\n", id)
	}

}
