package list

import (
	"fmt"
	"slices"

	"migrate/kdvh/db"
)

type Config struct{}

func (config *Config) Execute() {
	fmt.Println("Available tables in KDVH:")

	kdvh := db.Init()

	var tables []string
	for table := range kdvh.Tables {
		tables = append(tables, table)
	}

	slices.Sort(tables)
	for _, table := range tables {
		fmt.Println("    -", table)
	}
}
