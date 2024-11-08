package kdvh

import (
	"fmt"
	"slices"
)

type ListConfig struct{}

func (config *ListConfig) Execute(_ []string) error {
	fmt.Println("Available tables in KDVH:")

	var tables []string
	for table := range KDVH {
		tables = append(tables, table)
	}

	slices.Sort(tables)
	for _, table := range tables {
		fmt.Println("    -", table)
	}

	return nil
}
