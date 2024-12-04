package stinfosys

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const STINFOSYS_ENV_VAR string = "STINFO_CONN_STRING"

func Connect() (*pgx.Conn, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(STINFOSYS_ENV_VAR))
	if err != nil {
		log.Fatal("Could not connect to Stinfosys. Make sure to be connected to the VPN. " + err.Error())
	}
	return conn, ctx
}
