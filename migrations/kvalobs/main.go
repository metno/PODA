package kvalobs

import (
	"fmt"
	"os"

	"github.com/alexflint/go-arg"

	"migrate/kvalobs/check"
	"migrate/kvalobs/dump"
	port "migrate/kvalobs/import"
)

type Cmd struct {
	Dump   *dump.Config  `arg:"subcommand" help:"Dump tables from Kvalobs to CSV"`
	Import *port.Config  `arg:"subcommand" help:"Import CSV file dumped from Kvalobs"`
	Check  *check.Config `arg:"subcommand" help:"Performs various checks on kvalobs timeseries"`
}

func (c *Cmd) Execute(parser *arg.Parser) {
	switch {
	case c.Dump != nil:
		c.Dump.Execute()
	case c.Import != nil:
		c.Import.Execute()
	case c.Check != nil:
		c.Check.Execute()
	default:
		fmt.Println("Error: passing a subcommand is required.")
		fmt.Println()
		parser.WriteHelpForSubcommand(os.Stdout, "kvalobs")
	}
}
