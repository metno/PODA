package list

import (
	"fmt"
	"slices"

	"migrate/kdvh/db"
)

type Config struct{}

func (config *Config) Execute(_ []string) error {
	fmt.Println("Available tables in KDVH:")

	var tables []string
	for table := range db.KDVH {
		tables = append(tables, table)
	}

	slices.Sort(tables)
	for _, table := range tables {
		fmt.Println("    -", table)
	}

	return nil
}
