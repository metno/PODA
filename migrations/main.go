package main

import (
	"fmt"
	"log"

	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"

	"migrate/kdvh"
)

type CmdArgs struct {
	KDVH kdvh.Cmd `command:"kdvh" description:"Perform KDVH migrations"`
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

	// NOTE: go-flags calls the Execute method on the parsed subcommand
	_, err = flags.Parse(&CmdArgs{})
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok {
			if flagsErr.Type == flags.ErrHelp {
				return
			}
		}
		fmt.Println("Type './migrate -h' for help")
		return
	}
}
