package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alexflint/go-arg"
	"github.com/joho/godotenv"

	"migrate/kdvh"
	"migrate/kvalobs"
)

type CmdArgs struct {
	KDVH    *kdvh.Cmd    `arg:"subcommand" help:"Perform KDVH migrations"`
	Kvalobs *kvalobs.Cmd `arg:"subcommand" help:"Perform Kvalobs migrations"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// The following env variables are needed:
	// 1. Dump
	//   - kdvh: "KDVH_PROXY_CONN"
	//
	// 2. Import
	//   - kdvh: "LARD_STRING", "STINFO_STRING", "KDVH_PROXY_CONN"
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	args := CmdArgs{}
	parser := arg.MustParse(&args)

	switch {
	case args.KDVH != nil:
		args.KDVH.Execute(parser)
	case args.Kvalobs != nil:
		args.Kvalobs.Execute(parser)
	default:
		fmt.Println("Error: passing a subcommand is required.")
		fmt.Println()
		parser.WriteHelp(os.Stdout)
	}
}
