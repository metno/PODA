package kdvh

import (
	"fmt"
	"os"

	"github.com/alexflint/go-arg"

	"migrate/kdvh/dump"
	port "migrate/kdvh/import"
	"migrate/kdvh/list"
)

// Command line arguments for KDVH migrations
type Cmd struct {
	Dump   *dump.Config `arg:"subcommand" help:"Dump tables from KDVH to CSV"`
	Import *port.Config `arg:"subcommand" help:"Import CSV file dumped from KDVH"`
	List   *list.Config `arg:"subcommand" help:"List available KDVH tables"`
}

func (c *Cmd) Execute(parser *arg.Parser) {
	switch {
	case c.Dump != nil:
		c.Dump.Execute()
	case c.Import != nil:
		c.Import.Execute()
	case c.List != nil:
		c.List.Execute()
	default:
		fmt.Println("Error: passing a subcommand is required.")
		fmt.Println()
		parser.WriteHelpForSubcommand(os.Stdout, "kdvh")
	}
}
