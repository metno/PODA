package kdvh

import (
	"migrate/kdvh/dump"
	port "migrate/kdvh/import"
	"migrate/kdvh/list"
)

// Command line arguments for KDVH migrations
type Cmd struct {
	Dump   dump.DumpConfig `command:"dump" description:"Dump tables from KDVH to CSV"`
	Import port.Config     `command:"import" description:"Import CSV file dumped from KDVH"`
	List   list.Config     `command:"list" description:"List available KDVH tables"`
}
